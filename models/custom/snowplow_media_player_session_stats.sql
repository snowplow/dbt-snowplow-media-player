{{
  config(
    materialized = 'table',
    sort = 'start_tstamp',
    dist = 'domain_sessionid',
    partition_by = snowplow_utils.get_partition_by(bigquery_partition_by={
      "field": "start_tstamp",
      "data_type": "timestamp"
    }),
    cluster_by=snowplow_utils.get_cluster_by(bigquery_cols=["domain_userid"]),
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
  )
}}

with prep as (

  select
    domain_sessionid,
    domain_userid,
    count(*) as impressions,
    count(distinct case when media_type = 'video' and is_played then media_id end) as videos_played,
    count(distinct case when media_type = 'audio' and is_played then media_id end) as audio_played,
    sum(case when media_type = 'video' and is_played then 1 else 0 end) as video_plays,
    sum(case when media_type = 'audio' and is_played then 1 else 0 end) as audio_plays,
    sum(case when media_type = 'video' and is_valid_play then 1 else 0 end) as valid_video_plays,
    sum(case when media_type = 'audio' and is_valid_play then 1 else 0 end) as valid_audio_plays,
    min(start_tstamp) start_tstamp,
    max(end_tstamp) as end_tstamp,
    sum(seeks) as seeks,
    sum(play_time_sec / cast(60 as {{ dbt_utils.type_float() }})) as play_time_min,
    sum(play_time_sec_muted / cast(60 as {{ dbt_utils.type_float() }})) as play_time_min_muted,
    coalesce(avg(case when is_played then play_time_sec / cast(60 as {{ dbt_utils.type_float() }}) end), 0) as avg_play_time_min,
    coalesce(avg(case when is_played then coalesce(play_time_sec / nullif(duration, 0), 0) end),0) as avg_percent_played,
    sum(case when is_complete_play then 1 else 0 end) as complete_plays

  from {{ ref("snowplow_media_player_base") }}

  group by 1,2

)

select *

from prep
