{#
Copyright (c) 2022-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{{
  config(
    materialized = 'table',
    sort = 'first_play',
    dist = 'user_identifier',
    partition_by = snowplow_utils.get_value_by_target_type(bigquery_val={
      "field": "first_play",
      "data_type": "timestamp"
    }, databricks_val='first_play_date'),
    cluster_by=snowplow_utils.get_value_by_target_type(bigquery_val=["user_identifier"]),
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
  )
}}

with prep as (

  select
    user_identifier,
    app_id,
    min(case when (video_plays + audio_plays) > 0 then start_tstamp end) as first_play,
    max(case when (video_plays + audio_plays) > 0 then start_tstamp end) as last_play,
    sum(video_plays) as video_plays,
    sum(audio_plays) as audio_plays,
    sum(valid_video_plays) as valid_video_plays,
    sum(valid_audio_plays) as valid_audio_plays,
    sum(complete_plays) as complete_plays,
    sum(seeks) as seeks,
    cast(sum(play_time_mins) as {{ type_int() }}) as play_time_mins,
    -- using session and not page_view as the base for average to save cost by not joining on snowplow_media_player_base for calculating on individual page_view level average
    coalesce(cast(avg(case when (video_plays + audio_plays) > 0 then avg_play_time_mins end) as {{ type_int() }}), 0) as avg_session_play_time_mins,
    coalesce(avg(avg_percent_played),0) as avg_percent_played

  from {{ ref("snowplow_media_player_session_stats") }}

  group by 1,2 

)

select *

{% if target.type in ['databricks', 'spark'] -%}
, date(first_play) as first_play_date
{%- endif %}

from prep
