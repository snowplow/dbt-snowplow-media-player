{{
  config(
    unique_key = 'play_id',
    sort = 'start_tstamp',
    dist = 'play_id'
  )
}}

with prep as (

  select
    i.play_id,
    i.page_view_id,
    i.media_id,
    i.domain_sessionid,
    i.domain_userid,
    max(i.duration) as duration,
    i.media_type,
    i.media_player_type,
    i.page_referrer,
    i.page_url,
    max(i.source_url) as source_url,
    i.geo_region_name,
    i.br_name,
    i.dvce_type,
    i.os_name,
    i.os_timezone,
    min(start_tstamp) as start_tstamp,
    max(start_tstamp) as end_tstamp,
    sum(case when i.event_type = 'play' then 1 else 0 end) as plays,
    sum(case when i.event_type in ('seek', 'seeked') then 1 else 0 end) as seeks,
    sum(i.play_time_sec) as play_time_sec,
    sum(i.play_time_sec_muted) as play_time_sec_muted,
    sum(i.playback_rate * i.play_time_sec) / nullif(sum(i.play_time_sec), 0) as avg_playback_rate,
    listagg(distinct percent_progress, ',') within group (order by percent_progress) as percent_progress_reached,
    min(case when i.event_type in ('seek', 'seeked') then start_tstamp end) as first_seek_time,
    max(i.percent_progress) as max_percent_progress

  from {{ ref("interactions") }} i

  group by 1,2,3,4,5,7,8,9,10,12,13,14,15,16

)

, retention_rate as (

    select
      p.play_id,
      max(i.percent_progress) as retention_rate

    from prep p
    inner join {{ ref("interactions") }} i
    on i.play_id = p.play_id

    where i.event_type in ('percentprogress') and (i.start_tstamp <= p.first_seek_time or p.first_seek_time is null)

    group by 1

    order by 2

)

, dedupe as (

  select
    *,
    row_number() over (partition by page_view_id order by start_tstamp) as duplicate_count

  from prep

)

select
  d.play_id,
  d.page_view_id,
  d.media_id,
  d.domain_sessionid,
  d.domain_userid,
  d.duration,
  d.media_type,
  d.media_player_type,
  d.page_referrer,
  d.page_url,
  d.source_url,
  d.geo_region_name,
  d.br_name,
  d.dvce_type,
  d.os_name,
  d.os_timezone,
  d.start_tstamp,
  d.end_tstamp,
  d.play_time_sec,
  d.play_time_sec_muted,
  case when d.plays > 0 then true else false end is_played,
  case when d.play_time_sec > {{ var("snowplow__valid_play_sec") }} then true else false end is_valid_play,
  case when play_time_sec / duration >= {{ var("snowplow__complete_play_rate") }} then true else false end as is_complete_play,
  d.avg_playback_rate,
  coalesce(case when r.retention_rate > max_percent_progress
          then max_percent_progress / cast(100 as {{ dbt_utils.type_float() }})
          else r.retention_rate / cast(100 as {{ dbt_utils.type_float() }})
          end, 0) as retention_rate, -- to correct incorrect result due to duplicate session_id (one removed)
  d.seeks,
  d.percent_progress_reached

from dedupe d

left join retention_rate r
on r.play_id = d.play_id

where d.duplicate_count = 1
