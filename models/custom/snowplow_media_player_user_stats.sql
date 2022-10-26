{{
  config(
    materialized = 'table',
    sort = 'first_play',
    dist = 'domain_userid',
    partition_by = snowplow_utils.get_partition_by(bigquery_partition_by={
      "field": "first_play",
      "data_type": "timestamp"
    }, databricks_partition_by='first_play_date'),
    cluster_by=snowplow_utils.get_cluster_by(bigquery_cols=["domain_userid"]),
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
  )
}}

with prep as (

  select
    domain_userid,
    min(case when (video_plays + audio_plays) > 0 then start_tstamp end) as first_play,
    max(case when (video_plays + audio_plays) > 0 then start_tstamp end) as last_play,
    sum(video_plays) as video_plays,
    sum(audio_plays) as audio_plays,
    sum(valid_video_plays) as valid_video_plays,
    sum(valid_audio_plays) as valid_audio_plays,
    sum(complete_plays) as complete_plays,
    sum(seeks) as seeks,
    cast(sum(play_time_min) as {{ type_int() }}) as play_time_min,
    -- using session and not page_view as the base for average to save cost by not joining on snowplow_media_player_base for calculating on individual page_view level average
    coalesce(cast(avg(case when (video_plays + audio_plays) > 0 then avg_play_time_min end) as {{ type_int() }}), 0) as avg_session_play_time_min,
    coalesce(avg(avg_percent_played),0) as avg_percent_played

  from {{ ref("snowplow_media_player_session_stats") }}

  group by 1

)

select *

{% if target.type in ['databricks', 'spark'] -%}
, date(first_play) as first_play_date
{%- endif %}

from prep
