{{
  config(
    materialized = 'table',
    unique_key = 'domain_userid',
    sort = 'first_play',
    dist = 'domain_userid'
  )
}}

with prep as (

  select
    domain_userid,
    avg(avg_play_time_sec) / 60 as avg_play_time_min,
    min(start_tstamp) as first_play,
    max(start_tstamp) as last_play,
    sum(play_time_sec) / 60 as total_play_time_min,
    avg(avg_percentage_played) as avg_percentage_played,
    sum(video_plays) as video_plays,
    sum(audio_plays) as audio_plays,
    sum(seeks) as seeks,
    sum(complete_plays) as complete_plays,
    sum(valid_video_plays) as valid_video_plays,
    sum(valid_audio_plays) as valid_audio_plays

  from {{ ref("plays_by_session") }}

  group by domain_userid

)

select
  domain_userid,
  first_play,
  last_play,
  video_plays,
  audio_plays,
  valid_video_plays,
  valid_audio_plays,
  cast(total_play_time_min as {{ dbt_utils.type_int() }}) as play_time_min,
  cast(avg_play_time_min as {{ dbt_utils.type_int() }}) as avg_play_time_min,
  avg_percentage_played,
  seeks,
  complete_plays

from prep
