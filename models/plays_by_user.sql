{{ 
  config(
    materialized='table',
    unique_key = 'user_media_id',
    sort = 'first_play',
    dist = 'user_media_id'
  )
}}

with prep as (

  select
    {{ dbt_utils.hash('domain_userid||media_id') }} user_media_id,
    domain_userid,
    media_id,
    count(*) - 1 as returning_plays,
    sum(play_time_sec) as play_time_sec,
    sum(play_time_sec) / 60 as play_time_min,
    max(duration) as duration,
    sum(play_time_sec / duration) as retention_rate,
    min(start_tstamp) as first_play,
    max(start_tstamp) as last_play,
    sum(seeks) as seeks,
    sum(case when is_complete_play then 1 else 0 end) as complete_plays
  
  from {{ ref("plays_by_pageview") }}

  where play_time_sec > 0

  group by 1, 2, 3

)

, percent_progress as (

    select
      {{ dbt_utils.hash('domain_userid||media_id') }} user_media_id,
      coalesce(count(distinct percent_progress), 0) percent_progress_reached
    
    from {{ ref("interactions") }}

    where percent_progress <> 0

    group by 1

)

select
  p.user_media_id,
  p.domain_userid,
  p.media_id,
  p.duration,
  p.play_time_sec,
  p.play_time_min::int,
  p.returning_plays,
  p.retention_rate,
  pp.percent_progress_reached,
  p.first_play,
  p.last_play,
  p.seeks,
  p.complete_plays
  
from prep p

left join percent_progress pp
on pp.user_media_id = p.user_media_id



  