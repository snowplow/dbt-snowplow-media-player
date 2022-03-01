with prep as (

  select
    p.media_id,
    p.duration,
    p.media_type,
    p.media_player_type,
    sum(p.play_time_sec) / 3600 as play_time_h,
    avg(p.play_time_sec / 60) as avg_play_time_min,
    sum(case when is_played then 1 else 0 end) as plays,
    sum(case when p.is_complete_play then 1 else 0 end) as complete_plays,
    count(distinct p.page_view_id) as impressions,
    avg(coalesce(p.play_time_sec / nullif(p.duration, 0), 0)) as retention_rate,
    coalesce(sum(p.avg_playback_rate * p.play_time_sec) / nullif(sum(p.play_time_sec), 0), 0) as avg_playback_rate

  from {{ ref("plays_by_pageview") }} p

  group by 1, 2, 3, 4

)

, valid_play_stats as (

  select
    media_id,
    min(start_tstamp) as first_play,
    max(start_tstamp) as last_play,
    count(*) as valid_plays,
    count(distinct domain_userid) as unique_users_with_valid_plays,
    count(distinct domain_sessionid) as sessions_with_valid_plays

  from {{ ref("plays_by_pageview") }}

  where is_valid_play

  group by 1

)

, percent_passed as (

  select
    media_id,
    {% for element in var('snowplow__percent_progress_boundaries') %}
      sum(case when _{{ element }}_percent_passed > 0 then 1 else 0 end) as _{{ element }}_percent_passed
      {% if not loop.last %}
        ,
      {% endif %}
    {% endfor %}

    {% if 100 not in var("snowplow__percent_progress_boundaries") %}
      , sum(case when _100_percent_passed > 0 then 1 else 0 end) as _100_percent_passed
    {% endif %}

  from {{ ref("plays_by_pageview") }}

  group by 1
)

, user_stats as (

  select
    media_id,
    domain_userid,
    count(*) as pageviews_with_play,
    sum(case when is_complete_play then 1 else 0 end) as complete_plays

    from {{ ref("plays_by_pageview") }}

    where is_played

    group by 1,2

)

, user_stats_agg as (

  select
    media_id,
    count(distinct domain_userid) as unique_users_with_plays,
    sum(case when complete_plays >= 1 then 1 else 0 end) as users_with_complete_plays,
    sum(case when pageviews_with_play > 1 then 1 else 0 end) as returning_users

  from user_stats

  group by 1
)

select
  p.media_id,
  p.duration,
  p.media_type,
  p.media_player_type,
  p.play_time_h,
  p.avg_play_time_min,
  v.first_play,
  v.last_play,
  p.plays,
  v.valid_plays,
  p.complete_plays,
  u.unique_users_with_plays,
  v.unique_users_with_valid_plays,
  u.returning_users,
  p.impressions,
  v.sessions_with_valid_plays,
  p.avg_playback_rate,
  p.plays / cast(p.impressions as {{ dbt_utils.type_float() }}) as play_rate,
  coalesce(p.complete_plays / cast(nullif(p.plays, 0) as {{ dbt_utils.type_float() }}), 0) as completion_rate_by_plays,
  coalesce(u.users_with_complete_plays/ cast(nullif(u.unique_users_with_plays, 0) as {{ dbt_utils.type_float() }}), 0) as completion_rate_by_user,
  p.retention_rate,
  {% for element in var('snowplow__percent_progress_boundaries') %}
    cast(pp._{{ element }}_percent_passed as {{ dbt_utils.type_int() }}) as _{{ element }}_percent_passed
    {% if not loop.last %}
      ,
    {% endif %}
  {% endfor %}

  {% if 100 not in var("snowplow__percent_progress_boundaries") %}
    , cast(pp._100_percent_passed as {{ dbt_utils.type_int() }}) as _100_percent_passed
  {% endif %}

from prep p

left join valid_play_stats v
on v.media_id = p.media_id

left join percent_passed pp
on pp.media_id = p.media_id

left join user_stats_agg u
on u.media_id = p.media_id
