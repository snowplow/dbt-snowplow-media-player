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
    sort='last_event',
    dist='media_ad_id',
    partition_by = snowplow_utils.get_value_by_target_type(bigquery_val={
      "field": "viewed_at",
      "data_type": "timestamp"
    }, databricks_val='viewed_at_date'),
    cluster_by=snowplow_utils.get_value_by_target_type(bigquery_val=["media_ad_id"]),
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt')),
    enabled=var('snowplow__enable_media_ad', false)
  )
}}

with

events_this_run as (

    select * from {{ ref('snowplow_media_player_base_events_this_run') }}
    where ad_id is not null and media_identifier is not null

)

, prep as (

  select
    {{ dbt_utils.generate_surrogate_key(['ev.platform', 'ev.media_identifier', 'ev.ad_id']) }} as media_ad_id

    ,ev.platform
    ,ev.media_identifier
    ,ev.user_identifier
    ,ev.session_identifier
    ,ev.user_id
    ,ev.play_id
    ,{{ media_ad_break_field('ev.ad_break_id') }} as ad_break_id
    ,{{ media_ad_field('ev.ad_id') }} as ad_id

    {%- if var('snowplow__ad_views_passthroughs', []) -%}
      {%- set passthrough_names = [] -%}
      {%- for identifier in var('snowplow__ad_views_passthroughs', []) %}
      {# Check if it is a simple column or a sql+alias #}
      {%- if identifier is mapping -%}
        ,{{identifier['sql']}} as {{identifier['alias']}}
        {%- do passthrough_names.append(identifier['alias']) -%}
      {%- else -%}
        ,ev.{{identifier}}
        {%- do passthrough_names.append(identifier) -%}
      {%- endif -%}
      {% endfor -%}
    {%- endif %}

    ,max(ev.media_label) as media_label
    ,{{ media_ad_break_field('max(ev.ad_break_name)' ) }} as ad_break_name
    ,{{ media_ad_break_field('max(ev.ad_break_type)' ) }} as ad_break_type

    ,{{ media_ad_field('max(ev.ad_name)') }} as name
    ,{{ media_ad_field('max(ev.ad_creative_id)') }} as creative_id
    ,{{ media_ad_field('max(ev.ad_duration_secs)') }} as duration_secs
    ,{{ media_ad_field('avg(ev.ad_pod_position)') }} as pod_position
    ,{{ media_ad_field('sum(case when ev.ad_skippable then 1 else 0 end) > 0') }} as skippable

    ,max(case when ev.event_type = 'adclick' then 1 else 0 end) > 0 as clicked
    ,max(case when ev.event_type = 'adskip' then 1 else 0 end) > 0 as skipped
    ,{{ media_ad_quartile_event_field("max(case when ev.event_type = 'adcomplete' or (ev.event_type = 'adquartile' and ev.ad_percent_progress >= 25) then 1 else 0 end) > 0") }} as percent_reached_25
    ,{{ media_ad_quartile_event_field("max(case when ev.event_type = 'adcomplete' or (ev.event_type = 'adquartile' and ev.ad_percent_progress >= 50) then 1 else 0 end) > 0") }} as percent_reached_50
    ,{{ media_ad_quartile_event_field("max(case when ev.event_type = 'adcomplete' or (ev.event_type = 'adquartile' and ev.ad_percent_progress >= 75) then 1 else 0 end) > 0") }} as percent_reached_75
    ,max(case when ev.event_type = 'adcomplete' then 1 else 0 end) > 0 as percent_reached_100

    ,min(ev.start_tstamp) as viewed_at
    ,max(ev.start_tstamp) as last_event
    ,{{ snowplow_utils.get_string_agg('original_session_identifier', 'ev', is_distinct=True) }} as domain_sessionid_array

  from events_this_run as ev

  {{ dbt_utils.group_by(n=9+(var('snowplow__ad_views_passthroughs', [])|length)) }}

)

select
  {{ dbt_utils.generate_surrogate_key(['p.play_id', 'p.ad_break_id', 'p.media_ad_id']) }} as media_ad_view_id
  ,p.media_ad_id
  ,p.platform
  ,p.media_identifier
  ,p.media_label
  ,p.user_identifier
  ,p.session_identifier
  ,p.domain_sessionid_array
  ,p.user_id
  ,p.play_id
  ,p.ad_break_id
  ,p.ad_break_name
  ,p.ad_break_type
  ,p.ad_id
  ,p.name
  ,p.creative_id
  ,p.duration_secs
  ,p.pod_position
  ,p.skippable
  ,p.clicked
  ,p.skipped
  ,p.percent_reached_25
  ,p.percent_reached_50
  ,p.percent_reached_75
  ,p.percent_reached_100
  ,p.viewed_at
  ,p.last_event
  {% if target.type in ['databricks', 'spark'] -%}
    , date(p.viewed_at) as viewed_at_date
  {%- endif %}
  -- passthrough fields
  {%- if var('snowplow__ad_views_passthroughs', []) -%}
    {%- for col in passthrough_names %}
      , p.{{col}}
    {%- endfor -%}
  {%- endif %}

from prep as p
