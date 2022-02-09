{{
  config(
    materialized = 'table',
    sort = 'first_play',
    dist = 'domain_userid'
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
    cast(sum(play_time_min) as {{ dbt_utils.type_int() }}) as play_time_min,
    -- using session and not page_view as the base for average to save cost by not joining on snowplow_media_player_base for calculating on individual page_view level average
    coalesce(cast(avg(case when (video_plays + audio_plays) > 0 then avg_play_time_min end) as {{ dbt_utils.type_int() }}), 0) as avg_session_play_time_min,
    coalesce(avg(avg_percent_played),0) as avg_percent_played

  from {{ ref("snowplow_media_player_session_stats") }}

  group by 1

)

select *

from prep
