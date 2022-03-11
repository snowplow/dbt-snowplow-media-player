with prep as (

  select
    p.media_id,
    p.duration,
    p.media_type,
    p.media_player_type,
    sum(p.play_time_sec) / 3600 as play_time_h,
    avg(p.play_time_sec / 60) as avg_play_time_min,
    min(start_tstamp) as first_play,
    max(start_tstamp) as last_play,
    sum(case when is_played then 1 else 0 end) as plays,
    sum(case when is_valid_play then 1 else 0 end) as valid_plays,
    sum(case when p.is_complete_play then 1 else 0 end) as complete_plays,
    count(distinct p.page_view_id) as impressions,
    avg(coalesce(p.play_time_sec / nullif(p.duration, 0), 0)) as avg_percentage_played,
    avg(case when is_played then p.retention_rate end) as avg_retention_rate,
    coalesce(sum(p.avg_playback_rate * p.play_time_sec) / nullif(sum(p.play_time_sec), 0), 0) as avg_playback_rate

  from {{ ref("plays_by_pageview") }} p

  group by 1,2,3,4

)

, user_stats as (

  select
    media_id,
    domain_userid,
    count(*) as pageviews_w_play,
    sum(case when is_complete_play then 1 else 0 end) as complete_plays

    from {{ ref("plays_by_pageview") }}

    where is_played

    group by 1,2

)

, user_stats_agg as (

  select
    media_id,
    count(distinct domain_userid) as users,
    sum(case when complete_plays >= 1 then 1 else 0 end) as users_w_complete_plays,
    sum(case when pageviews_w_play > 1 then 1 else 0 end) as returning_users

  from user_stats

  group by 1

)

, percent_progress_reached as (

    select
      media_id,
      split_to_array(percent_progress_reached) percent_progress_reached

    from {{ ref("plays_by_pageview") }}

    where percent_progress_reached is not null

)

, unnesting as (

    select media_id, value_reached

    from percent_progress_reached p, p.percent_progress_reached as value_reached

)

select
  p.media_id,
  p.duration,
  p.media_type,
  p.media_player_type,
  p.play_time_h,
  p.avg_play_time_min,
  p.first_play,
  p.last_play,
  p.plays,
  p.valid_plays,
  p.complete_plays,
  u.users,
  u.users_w_complete_plays,
  u.returning_users,
  p.impressions,
  p.avg_playback_rate,
  p.plays / cast(p.impressions as {{ dbt_utils.type_float() }}) as play_rate,
  coalesce(p.complete_plays / cast(nullif(p.plays, 0) as {{ dbt_utils.type_float() }}), 0) as completion_rate_by_plays,
  coalesce(u.users_w_complete_plays/ cast(nullif(u.users, 0) as {{ dbt_utils.type_float() }}), 0) as completion_rate_by_user,
  p.avg_percentage_played,
  p.avg_retention_rate,
  {{ dbt_utils.pivot(
         column='un.value_reached',
         values=dbt_utils.get_column_values( table=ref('pivot_base'), column='percent_progress', default=[]) | sort,
         alias=True,
         agg='sum',
         cmp='=',
         prefix='_',
         suffix='_percent_passed',
         quote_identifiers=FALSE
         ) }}

from prep p

left join user_stats_agg u
on u.media_id = p.media_id

left join unnesting un
on un.media_id = p.media_id

left join {{ ref("pivot_base") }} piv
on un.value_reached = piv.percent_progress

group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
