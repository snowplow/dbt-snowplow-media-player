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
    {{ dbt_utils.surrogate_key(['e.page_view_id', 'coalesce(y.player_id, me.html_id)' ]) }} play_id,
    round(mp.duration) as duration,
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
    on mpe.root_id = e.event_id and mpe.root_tstamp = e.collector_tstamp

    left join {{ source('atomic', 'com_snowplowanalytics_snowplow_media_player_1') }} as mp
    on mp.root_id = e.event_id and mp.root_tstamp = e.collector_tstamp

    left join {{ source('atomic', 'com_youtube_youtube_1') }} as y
    on  y.root_id = e.event_id and y.root_tstamp = e.collector_tstamp

    left join {{ source('atomic', 'org_whatwg_media_element_1') }} as me
    on me.root_id = e.event_id and me.root_tstamp = e.collector_tstamp

    left join {{ source('atomic', 'org_whatwg_video_element_1') }} as ve
    on ve.root_id = e.event_id and ve.root_tstamp = e.collector_tstamp

)

 select
  p.*,
  cast(piv.weight_rate * p.duration / 100 as {{ dbt_utils.type_int() }}) as play_time_sec,
  cast(case when p.is_muted then piv.weight_rate * p.duration / 100 end as {{ dbt_utils.type_int() }}) as play_time_sec_muted

  from prep p
  left join {{ ref("pivot_base") }} piv
  on p.percent_progress = piv.percent_progress
