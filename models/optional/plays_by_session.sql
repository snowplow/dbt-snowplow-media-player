{{
  config(
    materialized = 'table',
    unique_key = 'domain_sessionid',
    sort = 'start_tstamp',
    dist = 'domain_sessionid'
  )
}}

with prep as (

  select
    domain_sessionid,
    domain_userid,
    count(*) as impressions,
    count(distinct case when media_type = 'video' and is_valid_play then media_id end) as videos_played,
    count(distinct case when media_type = 'audio' and is_valid_play then media_id end) as audio_played,
    sum(case when media_type = 'video' then 1 else 0 end) as video_plays,
    sum(case when media_type = 'audio' then 1 else 0 end) as audio_plays,
    sum(case when media_type = 'video' and is_valid_play then 1 else 0 end) as valid_video_plays,
    sum(case when media_type = 'audio' and is_valid_play then 1 else 0 end) as valid_audio_plays,
    min(start_tstamp) start_tstamp,
    max(end_tstamp) as end_tstamp,
    sum(seeks) seeks,
    sum(play_time_sec) as play_time_sec,
    sum(play_time_sec_muted) as play_time_sec_muted,
    avg(play_time_sec) as avg_play_time_sec,
    avg(play_time_sec / duration) as avg_percentage_played,
    sum(case when is_complete_play then 1 else 0 end) as complete_plays

  from {{ ref("plays_by_pageview") }}

  where is_played

  group by 1,2

)

select
  domain_sessionid,
  domain_userid,
  impressions,
  videos_played,
  audio_played,
  video_plays,
  audio_plays,
  valid_video_plays,
  valid_audio_plays,
  start_tstamp,
  end_tstamp,
  seeks,
  play_time_sec,
  play_time_sec_muted,
  avg_play_time_sec,
  avg_percentage_played,
  complete_plays

from prep
