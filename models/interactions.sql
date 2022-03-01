{{
  config(
    unique_key = 'event_id',
    sort = 'start_tstamp',
    dist = 'event_id'
  )
}}

with prep as (

  select
    e.event_id,
    e.page_view_id,
    e.domain_sessionid,
    e.domain_userid,
    coalesce(y.player_id, me.html_id) as media_id,
    mp.duration,
    case when me.media_type = 'audio' then 'audio' else 'video' end as media_type,
    coalesce(y.schema_vendor||'-'||y.schema_name, me.schema_vendor||'-'||me.schema_name)  as media_player_type,
    e.page_referrer,
    e.page_url,
    coalesce(y.url, me.current_src) as source_url,
    e.geo_region_name,
    e.br_name,
    e.dvce_type,
    e.os_name,
    e.os_timezone,
    mpe.type as event_type,
    e.derived_tstamp as start_tstamp,
    mp.current_time as media_start_pos,
    isnull(mp.playback_rate, 1) as playback_rate,
    coalesce(y.playback_quality, ve.video_height||'x'||ve.video_width) as playback_quality,
    case when event_type = 'ended' then 100 else mp.percent_progress end percent_progress,
    mp.muted as is_muted

    from {{ ref("mp_events") }} as e

    left join {{ source('atomic', 'com_snowplowanalytics_snowplow_media_player_event_1') }} as mpe
    on mpe.root_id = e.event_id

    left join {{ source('atomic', 'com_snowplowanalytics_snowplow_media_player_1') }} as mp
    on mp.root_id = e.event_id

    left join {{ source('atomic', 'com_youtube_youtube_1') }} as y
    on  y.root_id = e.event_id

    left join {{ source('atomic', 'org_whatwg_media_element_1') }} as me
    on me.root_id = e.event_id

    left join {{ source('atomic', 'org_whatwg_video_element_1') }} as ve
    on ve.root_id = e.event_id

    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23

)

, interaction_ends as (

  select
    page_view_id,
    media_id,
    event_id,
    event_type,
    start_tstamp,
    media_start_pos,
    playback_rate,
    coalesce(lead(start_tstamp, 1) over(partition by page_view_id, media_id order by start_tstamp), start_tstamp) as end_tstamp,
    coalesce(lead(media_start_pos, 1) over(partition by page_view_id, media_id order by start_tstamp), media_start_pos) as media_end_pos

  from prep

)

, time_periods as (

  select
    event_id,
    event_type,
    start_tstamp,
    end_tstamp,
    media_start_pos,
    media_end_pos,
    datediff(second,start_tstamp, end_tstamp) as elapsed_time_sec,
    case when media_end_pos - media_start_pos < 0 then 0 else round(media_end_pos - media_start_pos) / playback_rate end as play_time_sec -- this is to remove negative play_time_secs which are anyway the result of seeks which we want to exclude

  from interaction_ends

)

, corrected_play_time as (

  select
    event_id,
    media_end_pos,
    end_tstamp,
    case
      when event_type in ('pause', 'paused', 'ended', 'seek', 'seeked') then 0
      when (play_time_sec - elapsed_time_sec) > 0 then elapsed_time_sec -- to correct cases when seek disrupts the event order based play_time_end_sec calculation
      else play_time_sec
    end as play_time_sec_amended,
    elapsed_time_sec,
    play_time_sec

  from time_periods

)

select
  p.event_id,
  p.media_id,
  {{ dbt_utils.surrogate_key(['p.page_view_id', 'p.media_id']) }} play_id,
  p.page_view_id,
  p.domain_sessionid,
  p.domain_userid,
  p.event_type,
  p.media_type,
  p.media_player_type,
  p.page_referrer,
  p.page_url,
  p.source_url,
  p.geo_region_name,
  p.br_name,
  p.dvce_type,
  p.os_name,
  p.os_timezone,
  p.duration,
  p.playback_rate,
  p.playback_quality,
  p.percent_progress,
  p.is_muted,
  p.media_start_pos,
  c.media_end_pos,
  p.start_tstamp,
  c.end_tstamp,
  c.elapsed_time_sec,
  c.play_time_sec,
  c.play_time_sec_amended

from prep p

left join corrected_play_time c
on p.event_id = c.event_id
