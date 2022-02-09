{{ 
  config(
    materialized='table',
    unique_key = 'domain_userid',
    sort = 'first_play',
    dist = 'domain_userid'
  )
}}

with prep as (

  select
    domain_userid,
    sum(play_time_sec) / sum(duration) / 60 as avg_play_time_min,
    min(start_tstamp) as first_play,
    max(start_tstamp) as last_play,
    sum(play_time_sec) / 60 as total_play_time_min,
    sum(play_time_sec) / 3600 as total_play_time_h,
    sum(play_time_sec) / sum(duration) as avg_retention_rate,
    count(distinct case when media_type = 'video' then media_id end) as videos_played,
    count(distinct case when media_type = 'audio' then media_id end) as audio_played,
    sum(seeks) as seeks,
    sum(case when is_complete_play then 1 else 0 end) as complete_plays

  from {{ ref("plays_by_pageview") }}

  group by domain_userid

)

, valid_play_stats as (

    select
    domain_userid,
    count(case when media_type = 'video' then 1 end) as valid_video_plays,
    count(case when media_type = 'audio' then 1 end) as valid_audio_plays
    
    from {{ ref("plays_by_pageview") }}

    where is_valid_play

    group by domain_userid
)

select
  p.domain_userid,  
  p.first_play,
  p.last_play,
  p.videos_played,
  p.audio_played,
  v.valid_video_plays,
  v.valid_audio_plays,
  p.total_play_time_min::int,
  p.total_play_time_h::int,
  p.avg_play_time_min::int,
  p.avg_retention_rate,
  p.seeks,
  p.complete_plays
 
from prep p

left join valid_play_stats v
on v.domain_userid = p.domain_userid
