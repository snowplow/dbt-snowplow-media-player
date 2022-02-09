{{ 
  config(
    materialized='table',
  )
}}

with prep as (

 	select 
    p.media_id,
    p.duration,
    p.title,
    p.media_type,
    p.media_player_type,
    sum(p.play_time_sec) / 3600 as play_time_h,
    avg(p.play_time_sec / 60) as avg_play_time_min,
    sum(case when p.is_complete_play then 1 else 0 end) as complete_plays,
    count(distinct p.page_view_id) as views,
    count(distinct p.domain_sessionid) as "sessions",
    avg(coalesce(p.play_time_sec / nullif(p.duration, 0), 0)) as retention_rate,
    count(distinct p.domain_userid) as unique_users

  from {{ ref("plays_by_pageview") }} p
 
  group by 1, 2, 3, 4, 5

)

, valid_play_stats as (

    select
      media_id,
      min(start_tstamp) as first_play,
      max(start_tstamp) as last_play,
      count(*) as valid_plays
    
    from {{ ref("plays_by_pageview") }}

    where is_valid_play

    group by 1

)

, impressions as (

    select 
      media_id,
      count(*) as impressions
    
    from {{ ref("interactions") }}

    where event_type = 'ready'

    group by 1

)

, percent_passed as (

    select
      media_id, 
      {% for key, value in var('snowplow__percent_progress_boundaries').items() %}
        sum(case when _{{ key }}_percent_passed > 0 then 1 else 0 end) as _{{ key }}_percent_passed
        {% if not loop.last %}
          ,
        {% endif %}
      {% endfor %}

    from {{ ref("plays_by_pageview") }}

    group by 1
)

, user_stats as (
    select
      media_id,
      sum(case when complete_plays >= 1 then 1 else 0 end) as complete_plays_by_user,
      sum(case when returning_plays > 0 then 1 else 0 end) as returning_users

    from {{ ref("plays_by_user") }}

    group by 1
)

select
  p.media_id,
  p.duration,
  p.title,
  p.media_type,
  p.media_player_type,
  p.play_time_h,
  p.avg_play_time_min,
  v.first_play,
  v.last_play,
  v.valid_plays,
  p.unique_users,
  u.returning_users,
  i.impressions,
  p.views,
  p."sessions",
  coalesce(v.valid_plays / nullif(i.impressions, 0), 0) as play_rate,
  p.complete_plays,
  coalesce(p.complete_plays::real / nullif(p.views::real, 0::real), 0::real) as completion_rate_by_plays,
  coalesce(u.complete_plays_by_user::real / nullif(p.unique_users::real, 0::real), 0::real) as completion_rate_by_user,
  p.retention_rate,
  {% for key, value in var('snowplow__percent_progress_boundaries').items() %}
    pp._{{ key }}_percent_passed::int
    {% if not loop.last %}
      ,
    {% endif %}
  {% endfor %}

from prep p

left join valid_play_stats v
on v.media_id = p.media_id

left join impressions i
on i.media_id = p.media_id

left join percent_passed pp
on pp.media_id = p.media_id

left join user_stats u
on u.media_id = p.media_id
