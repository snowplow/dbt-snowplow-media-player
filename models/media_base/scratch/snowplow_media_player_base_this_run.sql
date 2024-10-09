{#
Copyright (c) 2022-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{{
  config(
    materialized='table',
    tags=["this_run"],
    partition_by = snowplow_utils.get_value_by_target_type(bigquery_val={
      "field": "start_tstamp",
      "data_type": "timestamp"
    }, databricks_val='start_tstamp_date'),
    cluster_by=snowplow_utils.get_value_by_target_type(bigquery_val=["media_identifier"]),
    sort = 'start_tstamp',
    dist = 'play_id',
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
  )
}}

with

events_this_run as (
    select
      *
      ,row_number()
        over (partition by media_session__media_session_id order by start_tstamp desc) as media_session_index_desc
      ,row_number()
        over (partition by play_id order by start_tstamp asc) as play_id_index_asc
    from {{ ref('snowplow_media_player_base_events_this_run') }}
)

, prep as (

  select
    i.play_id
    ,i.app_id
    ,i.media_identifier
    ,i.player_id
    ,i.media_label
    ,i.session_identifier
    ,i.user_identifier
    ,i.user_id
    ,i.platform
    ,i.media_type
    ,i.media_player_type
    ,i.page_referrer
    ,i.page_url
    ,i.geo_region_name
    ,i.br_name
    ,i.dvce_type
    ,i.os_name
    ,i.os_timezone

    {%- if var('snowplow__base_passthroughs', []) -%}
      {%- set passthrough_names = [] -%}
      {%- for identifier in var('snowplow__base_passthroughs', []) %}
      {# Check if it is a simple column or a sql+alias #}
      {%- if identifier is mapping -%}
        ,{{identifier['sql']}} as {{identifier['alias']}}
        {%- do passthrough_names.append(identifier['alias']) -%}
      {%- else -%}
        ,i.{{identifier}}
        {%- do passthrough_names.append(identifier) -%}
      {%- endif -%}
      {% endfor -%}
    {%- endif %}

    ,max(i.source_url) as source_url
    ,max(i.duration_secs) as duration_secs
    ,min(start_tstamp) as start_tstamp
    ,max(start_tstamp) as end_tstamp
    ,sum(case when i.event_type = 'play' then 1 else 0 end) as plays
    ,sum(case when i.event_type in ('seek', 'seeked', 'seekend') then 1 else 0 end) as seeks
    ,sum(i.play_time_secs) as play_time_secs
    ,sum(i.play_time_muted_secs) as play_time_muted_secs
    ,coalesce(
      sum(i.playback_rate * i.play_time_secs) / nullif(sum(i.play_time_secs), 0),
      max(i.playback_rate)
    ) as avg_playback_rate
    ,min(case when i.event_type in ('seek', 'seeked', 'seekstart', 'seekend') then start_tstamp end) as first_seek_time
    ,max(i.percent_progress) as max_percent_progress
    ,{{ snowplow_utils.get_string_agg('original_session_identifier', 'i', is_distinct=True) }} as domain_sessionid_array


  from events_this_run as i

  {{ dbt_utils.group_by(n=18+(var('snowplow__base_passthroughs', [])|length)) }}

)

, dedupe as (

  select
    p.*
    {% if target.type == 'postgres' %}
      ,row_number() over (partition by p.play_id order by p.start_tstamp) as duplicate_count
    {% endif %}

  from prep as p

  {% if target.type not in ['postgres'] %}
    qualify row_number() over (partition by p.play_id order by p.start_tstamp) = 1
  {% endif %}

)

, media_sessions as (

  select
    media_session__media_session_id as media_session_id,
    media_session__time_played as media_session_time_played,
    media_session__time_played_muted as media_session_time_played_muted,
    media_session__time_paused as media_session_time_paused,
    media_session__content_watched as media_session_content_watched,
    media_session__time_buffering as media_session_time_buffering,
    media_session__time_spent_ads as media_session_time_spent_ads,
    media_session__ads as media_session_ads,
    media_session__ads_clicked as media_session_ads_clicked,
    media_session__ads_skipped as media_session_ads_skipped,
    media_session__ad_breaks as media_session_ad_breaks,
    media_session__avg_playback_rate as media_session_avg_playback_rate

  from events_this_run
  where media_session_index_desc = 1

)

, first_page_views_by_play_id as (

  select
    ev.play_id, ev.page_view_id

  from events_this_run as ev
  where play_id_index_asc = 1

)

, page_view_id_aggregation as (

  select
    i.play_id
    ,{{ snowplow_utils.get_string_agg('page_view_id', 'i', is_distinct=True) }} as page_view_id_array

  from events_this_run as i
  group by 1

)

--- The following CTEs create a distinct list of percent_progress values for each play_id. We first need to select distinct percent_progress, because get_string_agg can't get distinct values with numeric ordering.
, distinct_percent_progress as (
  select distinct ev.play_id, ev.percent_progress
  from events_this_run as ev
  where ev.percent_progress is not null
)
, percent_progress_by_play_id as (
  select
    i.play_id,
    {{ snowplow_utils.get_string_agg('percent_progress', 'i', sort_numeric=true) }} as percent_progress_reached
  from distinct_percent_progress as i
  group by 1
)

, retention_rate as (

  select
    d.play_id,
    max(i.percent_progress) as retention_rate

  from dedupe as d

  inner join events_this_run as i
    on i.play_id = d.play_id

  where
    i.percent_progress is not null
    and (i.start_tstamp <= d.first_seek_time or d.first_seek_time is null)

  group by 1

)

-- for correcting NULLs in case of 'ready' events only where the metadata showing the duration_secs is usually missing as the event fires before it has time to load
, duration_fix as (

  select
    f.media_identifier,
    max(f.duration_secs) as duration_secs

  from events_this_run as f

  group by 1

)

{% set play_time_secs -%}
  coalesce(s.media_session_time_played, d.play_time_secs)
{%- endset %}

select
  d.play_id,
  pv.page_view_id,
  pva.page_view_id_array,
  d.media_identifier,
  d.player_id,
  d.media_label,
  d.session_identifier,
  d.domain_sessionid_array,
  d.user_identifier,
  d.user_id,
  d.page_referrer,
  d.page_url,
  d.source_url,
  d.geo_region_name,
  d.br_name,
  d.dvce_type,
  d.os_name,
  d.os_timezone,
  d.platform,

  -- media information
  f.duration_secs,
  d.media_type,
  d.media_player_type,

  -- playback information
  d.start_tstamp,
  d.end_tstamp,
  coalesce(
    s.media_session_avg_playback_rate,
    cast(d.avg_playback_rate as {{ type_float() }})
  ) as avg_playback_rate,

  -- time spent
  {{ play_time_secs }} as play_time_secs,
  coalesce(s.media_session_time_played_muted, d.play_time_muted_secs) as play_time_muted_secs,
  s.media_session_time_paused as paused_time_secs,
  s.media_session_time_buffering as buffering_time_secs,
  s.media_session_time_spent_ads as ads_time_secs,

  -- event counts
  d.seeks,
  s.media_session_ads as ads,
  s.media_session_ads_clicked as ads_clicked,
  s.media_session_ads_skipped as ads_skipped,
  s.media_session_ad_breaks as ad_breaks,

  -- playback progress
  d.plays > 0 as is_played,
  case
    when {{ play_time_secs }} > {{ var("snowplow__valid_play_sec") }} then true else
      false
  end as is_valid_play,
  case
    when
      coalesce(s.media_session_content_watched, d.play_time_secs) / nullif(f.duration_secs, 0)
      >= {{ var("snowplow__complete_play_rate") }}
      then true else
      false
  end as is_complete_play,
  cast(coalesce(case
    when r.retention_rate > d.max_percent_progress
      then d.max_percent_progress / cast(100 as {{ type_float() }})
    else r.retention_rate / cast(100 as {{ type_float() }})
  -- to correct incorrect result due to duplicate session_identifier (one removed)
  end, 0) as {{ type_float() }}) as retention_rate,
  p.percent_progress_reached,
  s.media_session_content_watched as content_watched_secs,
  case
    when d.duration_secs is not null and s.media_session_content_watched is not null and d.duration_secs > 0
    then least(
      s.media_session_content_watched / d.duration_secs,
      1.0
    )
  end as content_watched_percent

  {% if target.type in ['databricks', 'spark'] -%}
  , date(d.start_tstamp) as start_tstamp_date
  {%- endif %}

  -- passthrough fields
  {%- if var('snowplow__base_passthroughs', []) -%}
    {%- for col in passthrough_names %}
      , d.{{col}}
    {%- endfor -%}
  {%- endif %}

from dedupe as d

left join retention_rate as r
  on r.play_id = d.play_id

left join duration_fix as f
  on f.media_identifier = d.media_identifier

left join media_sessions as s
  on s.media_session_id = d.play_id

left join percent_progress_by_play_id as p
  on p.play_id = d.play_id

left join first_page_views_by_play_id as pv
  on pv.play_id = d.play_id

left join page_view_id_aggregation as pva
  on pva.play_id = d.play_id

{% if target.type == 'postgres' %}
  where d.duplicate_count = 1
{% endif %}
