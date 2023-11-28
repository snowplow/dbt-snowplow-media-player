{#
Copyright (c) 2022-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{{
  config(
    materialized= 'incremental',
    unique_key = 'media_identifier',
    sort = 'last_play',
    dist = 'media_identifier',
    tags=["derived"],
    partition_by = snowplow_utils.get_value_by_target_type(bigquery_val={
      "field": "first_play",
      "data_type": "timestamp"
    }, databricks_val='first_play_date'),
    cluster_by=snowplow_utils.get_value_by_target_type(bigquery_val=["media_identifier"]),
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt')),
    tblproperties={
      'delta.autoOptimize.optimizeWrite' : 'true',
      'delta.autoOptimize.autoCompact' : 'true'
    }
  )
}}

{% if is_incremental() %}

with new_data as (

  select
    p.media_identifier,
    p.player_id,
    p.media_label,
    max(p.duration_secs) as duration_secs,
    p.media_type,
    p.media_player_type,
    min(case when is_played then p.start_tstamp end) as first_play,
    max(case when is_played then p.start_tstamp end) as last_play,
    sum(p.play_time_secs) as play_time_secs,
    sum(case when is_played then 1 else 0 end) as plays,
    sum(case when is_valid_play then 1 else 0 end) as valid_plays,
    sum(case when p.is_complete_play then 1 else 0 end) as complete_plays,
    count(distinct p.page_view_id) as impressions,
    avg(case when is_played then coalesce(p.content_watched_secs, p.play_time_secs, 0) / nullif(p.duration_secs, 0) end) as avg_percent_played,
    avg(case when is_played then p.retention_rate end) as avg_retention_rate,
    avg(case when is_played then p.avg_playback_rate end) as avg_playback_rate,
    avg(case when is_played then p.content_watched_secs end) as avg_content_watched_sec,
    max(start_tstamp) as last_base_tstamp

from {{ ref("snowplow_media_player_base") }} p

where -- enough time has passed since the page_view's start_tstamp to be able to process it as a whole (please bear in mind the late arriving data)
cast({{ dateadd('hour', var("snowplow__max_media_pv_window", 10), 'p.end_tstamp ') }} as {{ type_timestamp() }}) < {{ snowplow_utils.current_timestamp_in_utc() }}
-- and it has not been processed yet
and (
  not exists(select 1 from {{ this }}) or -- no records in the table
  p.start_tstamp > ( select max(last_base_tstamp) from {{ this }} )
)

group by 1,2,3,5,6

)

, prep as (

  select
    n.media_identifier,
    n.player_id,
    n.media_label,
    greatest(n.duration_secs, coalesce(t.duration_secs, 0)) as duration_secs,
    n.media_type,
    n.media_player_type,
    n.last_base_tstamp,
    nullif(
      least(n.first_play, coalesce(t.first_play, cast('2999-01-01 00:00:00' as {{ type_timestamp() }}))),
      cast('2999-01-01 00:00:00' as {{ type_timestamp() }})
    ) as first_play,
    nullif(
      greatest(n.last_play, coalesce(t.last_play, cast('2000-01-01 00:00:00' as {{ type_timestamp() }}))),
      cast('2000-01-01 00:00:00' as {{ type_timestamp() }})
    ) as last_play,
    n.play_time_secs / cast(60 as {{ type_float() }}) + coalesce(t.play_time_mins, 0) as play_time_mins,
    (n.play_time_secs / cast(60 as {{ type_float() }}) + coalesce(t.play_time_mins, 0))  / nullif((n.plays + coalesce(t.plays, 0)), 0) as avg_play_time_mins,
    n.plays + coalesce(t.plays, 0) as plays,
    n.valid_plays + coalesce(t.valid_plays, 0) as valid_plays,
    n.complete_plays + coalesce(t.complete_plays, 0) as complete_plays,
    n.impressions + coalesce(t.impressions, 0)  as impressions,
    -- weighted average calculations
    (n.avg_percent_played * n.plays / nullif((n.plays + coalesce(t.plays, 0)),0)) + (coalesce(t.avg_percent_played, 0) * coalesce(t.plays, 0) / nullif((n.plays + coalesce(t.plays, 0)), 0)) as avg_percent_played,
    (n.avg_retention_rate * n.plays / nullif((n.plays + coalesce(t.plays, 0)), 0)) + (coalesce(t.avg_retention_rate, 0) * coalesce(t.plays, 0) / nullif((n.plays + coalesce(t.plays, 0)), 0)) as avg_retention_rate,
    (n.avg_playback_rate * n.plays / nullif((n.plays + coalesce(t.plays, 0)), 0)) + (coalesce(t.avg_playback_rate, 0) * coalesce(t.plays, 0) / nullif((n.plays + coalesce(t.plays, 0)), 0)) as avg_playback_rate,
    cast((coalesce(n.avg_content_watched_sec, 0.0) / cast(60 as {{ type_float() }}) * n.plays + coalesce(t.avg_content_watched_mins, 0.0) * coalesce(t.plays, 0.0)) / nullif((n.plays + coalesce(t.plays, 0.0)), 0.0) as {{ type_float() }}) as avg_content_watched_mins

  from new_data n

  left join {{ this }} t
  on n.media_identifier = t.media_identifier

)

, percent_progress_reached as (

    select
      p.media_identifier,
      {{ snowplow_utils.get_split_to_array('percent_progress_reached', 'p') }} as percent_progress_reached

    from {{ ref("snowplow_media_player_base") }} p

    where -- enough time has passed since the page_view`s start_tstamp to be able to process it a a whole (please bear in mind the late arriving data)

    cast({{ dateadd('hour', var("snowplow__max_media_pv_window", 10), 'p.end_tstamp ') }} as {{ type_timestamp() }}) < {{ snowplow_utils.current_timestamp_in_utc() }}

    -- and it has not been processed yet
    and p.start_tstamp > ( select max(last_base_tstamp) from {{ this }} )

)

, unnesting as (

  {{ snowplow_utils.unnest('media_identifier', 'percent_progress_reached', 'value_reached', 'percent_progress_reached') }}

)

, pivoting as (

  select
    u.media_identifier,
  {{ dbt_utils.pivot(
    column='u.value_reached',
    values=dbt_utils.get_column_values( table=ref('snowplow_media_player_pivot_base'), column='percent_progress', default=[]) | sort,
    alias=True,
    agg='sum',
    cmp='=',
    prefix='percent_reached_',
    quote_identifiers=FALSE
    ) }}

  from unnesting u

  group by 1

)

, addition as (

  select
    coalesce(p.media_identifier, t.media_identifier) as media_identifier,

  {% for element in get_percentage_boundaries(var("snowplow__percent_progress_boundaries")) %}

    {% set element_string = element | string() %}

    {% set alias  = 'percent_reached_' + element_string %}

    coalesce(p.percent_reached_{{ element_string }}, 0)
  + coalesce(t.percent_reached_{{ element_string }}, 0)
    as {{ alias }}

    {% if not loop.last %}

      ,

    {% endif %}

  {% endfor %}

  from pivoting p

  full outer join {{ this }} t
  on t.media_identifier = p.media_identifier

)

{% else %}

with prep as (

  select
    p.media_identifier,
    p.player_id,
    p.media_label,
    max(p.duration_secs) as duration_secs,
    p.media_type,
    p.media_player_type,
    max(start_tstamp) as last_base_tstamp,
    min(case when is_played then p.start_tstamp end) as first_play,
    max(case when is_played then p.start_tstamp end) as last_play,
    sum(p.play_time_secs) / cast(60 as {{ type_float() }}) as play_time_mins,
    avg(case when is_played then p.play_time_secs / cast(60 as {{ type_float() }}) end) as avg_play_time_mins,
    sum(case when is_played then 1 else 0 end) as plays,
    sum(case when is_valid_play then 1 else 0 end) as valid_plays,
    sum(case when p.is_complete_play then 1 else 0 end) as complete_plays,
    count(distinct p.page_view_id) as impressions,
    avg(case when is_played then coalesce(p.content_watched_secs, p.play_time_secs, 0) / nullif(p.duration_secs, 0) end) as avg_percent_played,
    avg(case when is_played then p.retention_rate end) as avg_retention_rate,
    avg(case when is_played then p.avg_playback_rate end) as avg_playback_rate,
    cast(avg(
      case
        when is_played and p.content_watched_secs is not null
        then p.content_watched_secs / cast(60 as {{ type_float() }}) end
    ) as {{ type_float() }}) as avg_content_watched_mins


from {{ ref("snowplow_media_player_base") }} p

group by 1,2,3,5,6

)

, percent_progress_reached as (

    select
      p.media_identifier,
      {{ snowplow_utils.get_split_to_array('percent_progress_reached', 'p') }} as percent_progress_reached

    from {{ ref("snowplow_media_player_base") }} p

)

, unnesting as (

  {{ snowplow_utils.unnest('media_identifier', 'percent_progress_reached', 'value_reached', 'percent_progress_reached') }}

)

{% endif %}


select
  p.media_identifier,
  p.player_id,
  p.media_label,
  p.duration_secs,
  p.media_type,
  p.media_player_type,
  p.play_time_mins,
  p.avg_play_time_mins,
  p.avg_content_watched_mins,
  p.first_play,
  p.last_play,
  p.plays,
  p.valid_plays,
  p.complete_plays,
  p.impressions,
  p.avg_playback_rate,
  p.plays / cast(nullif(p.impressions, 0) as {{ type_float() }}) as play_rate,
  p.complete_plays / cast(nullif(p.plays, 0) as {{ type_float() }}) as completion_rate_by_plays,
  p.avg_percent_played,
  p.avg_retention_rate,
  l.last_base_tstamp,

  {% if target.type in ['databricks', 'spark'] -%}
    date(p.first_play) as first_play_date,
  {%- endif %}

  {% if is_incremental() %}

    {% for element in get_percentage_boundaries(var("snowplow__percent_progress_boundaries")) %}
      coalesce(cast(a.percent_reached_{{ element }} as {{ type_int() }}), 0) as percent_reached_{{ element }}
      {% if not loop.last %}
        ,
      {% endif %}
    {% endfor %}

  {% else %}

    {{ dbt_utils.pivot(
    column='un.value_reached',
    values=dbt_utils.get_column_values( table=ref('snowplow_media_player_pivot_base'), column='percent_progress', default=[]) | sort,
    alias=True,
    agg='sum',
    cmp='=',
    prefix='percent_reached_',
    quote_identifiers=FALSE
    ) }}

  {% endif %}

  from prep p

  left join (select max(last_base_tstamp) as last_base_tstamp from prep ) l
  on 1 = 1

  {% if is_incremental() %}

  left join addition a
  on a.media_identifier = p.media_identifier

  {% else %}

  left join unnesting un
  on un.media_identifier = p.media_identifier

  {{ dbt_utils.group_by(n=21) }}

{% endif %}
