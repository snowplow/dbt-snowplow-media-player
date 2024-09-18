{#
Copyright (c) 2022-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{{
  config(
    materialized = 'table',
    sort = 'start_tstamp',
    dist = 'domain_sessionid',
    partition_by = snowplow_utils.get_value_by_target_type(bigquery_val={
      "field": "start_tstamp",
      "data_type": "timestamp"
    }, databricks_val='start_tstamp_date'),
    cluster_by=snowplow_utils.get_value_by_target_type(bigquery_val=["user_identifier"]),
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
  )
}}

with prep as (

  select
    -- get the first domain_sessionid in the array
    {% if target.type in ('bigquery', 'databricks', 'snowflake') %}
      cast(({{ snowplow_utils.get_split_to_array('domain_sessionid_array', 'b') }})[0] as {{ type_string() }}) as domain_sessionid,
    {% elif target.type == 'redshift' %}
      split_part(domain_sessionid_array, ',', 1) as domain_sessionid,
    {% elif target.type == 'spark' %}
      split(domain_sessionid_array, ',')[0] as domain_sessionid,
    {% else %}
      cast(({{ snowplow_utils.get_split_to_array('domain_sessionid_array', 'b') }})[1] as {{ type_string() }}) as domain_sessionid,
    {% endif %}
    user_identifier,
    count(*) as impressions,
    count(distinct case when media_type = 'video' and is_played then media_identifier end) as videos_played,
    count(distinct case when media_type = 'audio' and is_played then media_identifier end) as audio_played,
    sum(case when media_type = 'video' and is_played then 1 else 0 end) as video_plays,
    sum(case when media_type = 'audio' and is_played then 1 else 0 end) as audio_plays,
    sum(case when media_type = 'video' and is_valid_play then 1 else 0 end) as valid_video_plays,
    sum(case when media_type = 'audio' and is_valid_play then 1 else 0 end) as valid_audio_plays,
    min(start_tstamp) start_tstamp,
    max(end_tstamp) as end_tstamp,
    sum(seeks) as seeks,
    sum(play_time_secs / cast(60 as {{ type_float() }})) as play_time_mins,
    sum(play_time_muted_secs / cast(60 as {{ type_float() }})) as play_time_muted_mins,
    coalesce(avg(case when is_played then play_time_secs / cast(60 as {{ type_float() }}) end), 0) as avg_play_time_mins,
    coalesce(avg(case when is_played then coalesce(play_time_secs / nullif(duration_secs, 0), 0) end),0) as avg_percent_played,
    sum(case when is_complete_play then 1 else 0 end) as complete_plays

  from {{ ref("snowplow_media_player_base") }} as b

  group by 1,2

)

select *

{% if target.type in ['databricks', 'spark'] -%}
, date(start_tstamp) as start_tstamp_date
{%- endif %}

from prep
