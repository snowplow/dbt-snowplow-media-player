{{ 
  config(
    materialized='table',
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
    coalesce(y.player_id, me.html_id, r.content_title) as media_id, -- replace content_title with id field when available
    round(mp.duration) as duration,
    coalesce(y.player_id, me.html_id, r.content_title) as title, -- replace ids with title fields when available
    case when me.media_type = 'audio' then 'audio' else 'video' end as media_type,
    coalesce(y.schema_vendor||'-'||y.schema_name, me.schema_vendor||'-'||me.schema_name, r.schema_vendor||'-'||r.schema_name)  as media_player_type,
    e.page_referrer,
    coalesce(e.page_url, r.content_url) as content_url,
    e.geo_region_name,
    e.br_name,
    e.dvce_type,
    e.os_name,
    e.os_timezone,
    mpe.type as event_type,
    e.derived_tstamp as start_tstamp, 
    mp.current_time as media_start_pos,
    isnull(mp.playback_rate, 1) as playback_rate,
    coalesce(y.playback_quality, ve.video_height||'x'||ve.video_width, rd.display_height||'x'||rd.display_width) as playback_quality,
    case when event_type = 'ended' then 100 else mp.percent_progress end percent_progress,
    mp.muted
 		
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

    left join {{ source('atomic', 'com_roku_video_1') }} as r
    on r.root_id = e.event_id

    left join {{ source('atomic', 'com_roku_device_info_1') }} as rd
	  on  rd.root_id = e.event_id

)

, interaction_ends as (

	select
		page_view_id,
		media_id,
		event_id,
		event_type,
		start_tstamp,
		media_start_pos,
		lead(start_tstamp, 1) over(partition by page_view_id, media_id order by start_tstamp) end_tstamp,
		lead(media_start_pos, 1) over(partition by page_view_id, media_id order by start_tstamp) media_end_pos	
		
	from prep	

)

, find_nulls_round_1 as (

	select 
		event_id,
    start_tstamp,
    event_type,
    case when event_type in ('pause', 'paused', 'ended') then coalesce(end_tstamp, start_tstamp) else end_tstamp end end_tstamp,
    media_start_pos,
		case when event_type in ('pause', 'paused', 'ended') then coalesce(media_end_pos, media_start_pos) else media_end_pos end media_end_pos
	
	from interaction_ends

)

-- for now default it to start_tstamp, same as above but it could be the page_view end_tstamp
, find_nulls_round_2 as (

	select
		f.event_id,
    f.event_type,
    f.start_tstamp,
		case when f.end_tstamp is null then p.start_tstamp else f.end_tstamp end end_tstamp,
    f.media_start_pos,
		case when f.media_end_pos is null then p.media_start_pos else f.media_end_pos end as media_end_pos 
	
	from find_nulls_round_1 f

	inner join prep p
	on p.event_id = f.event_id

)	 

, time_periods as (

    select
		  r.event_id,
      r.event_type,
      r.start_tstamp,
      r.end_tstamp,
      r.media_start_pos,
      r.media_end_pos,
      datediff(second,r.start_tstamp, r.end_tstamp) as elapsed_time_sec,
	    case when r.media_end_pos - r.media_start_pos < 1 then 0 else round(r.media_end_pos - r.media_start_pos) / p.playback_rate end as play_time_sec -- this is to remove negative play_time_secs which are anyway the result of seeks which we want to exclude

    from find_nulls_round_2 r

    left join prep p
	  on p.event_id = r.event_id

)

, corrected_play_time as (

   select
    event_id,
    media_end_pos,
    end_tstamp,
    case 
      when event_type in ('pause', 'paused', 'ended', 'seek', 'seeked') then 0
      when (play_time_sec - elapsed_time_sec) > 1 then elapsed_time_sec -- to correct cases when seek disrupts the event order based play_time_end_sec calculation
      else play_time_sec
    end as play_time_sec_amended,
    elapsed_time_sec,
    play_time_sec

  from time_periods

)

, corrected_duration as (

  select
    media_id,
    max(duration) as duration

  from prep

  group by media_id

)
	
select
	p.event_id,
  p.media_id,
  p.page_view_id,
  p.domain_sessionid,
  p.domain_userid,
	p.event_type,
  p.title,
  p.media_type,
  p.media_player_type,
  p.page_referrer,
  p.content_url,
  p.geo_region_name,
  p.br_name,
  p.dvce_type,
  p.os_name,
  p.os_timezone,
  d.duration,
  p.playback_rate,
  p.playback_quality,
  p.percent_progress,
  p.muted,
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

left join corrected_duration d
on d.media_id = p.media_id
