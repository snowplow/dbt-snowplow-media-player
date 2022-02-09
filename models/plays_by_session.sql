{{ 
  config(
    materialized='table',
    unique_key = 'domain_sessionid',
    sort = 'start_tstamp',
    dist = 'domain_sessionid'
  )
}}

with prep as (

  select
    domain_sessionid,
    domain_userid,
    count(distinct case when media_type = 'video' and is_valid_play then media_id end) as videos_played,
    count(distinct case when media_type = 'audio' and is_valid_play then media_id end) as audio_played,
    sum(case when media_type = 'video' and is_valid_play then 1 else 0 end) as valid_video_plays,
    sum(case when media_type = 'audio' and is_valid_play then 1 else 0 end) as valid_audio_plays,  
    min(start_tstamp) start_tstamp,
    max(end_tstamp) as end_tstamp,
    sum(seeks) seeks,
    sum(play_time_sec) as play_time_sec,
    sum(play_time_sec_muted) as play_time_sec_muted,
    sum(play_time_sec / 60) play_time_min,
    sum(play_time_sec_muted / 60) as play_time_min_muted,
    sum(play_time_sec) / sum(duration) as avg_retention_rate,
    sum(case when is_complete_play then 1 else 0 end) as complete_plays

  from {{ ref("plays_by_pageview") }}

  where play_time_sec > 0

  group by
    domain_sessionid,
    domain_userid

)

select
  domain_sessionid,
  domain_userid,
  videos_played,
  audio_played,
  valid_video_plays,
  valid_audio_plays,  
  start_tstamp,
  end_tstamp,
  seeks,
  play_time_sec,
  play_time_sec_muted,
  play_time_min::int,
  play_time_min_muted::int,
  avg_retention_rate,
  complete_plays

from prep
