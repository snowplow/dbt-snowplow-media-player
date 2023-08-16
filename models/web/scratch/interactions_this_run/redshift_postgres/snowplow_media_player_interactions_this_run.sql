{{
  config(
    materialized='table',
    tags=["this_run"],
    sort = 'start_tstamp',
    dist = 'event_id'
  )
}}

{%- set lower_limit, upper_limit = snowplow_utils.return_limits_from_model(ref('snowplow_web_base_sessions_this_run'),
                                                                          'start_tstamp',
                                                                          'end_tstamp') %}

with mpe_context as (

  select
    root_id,
    root_tstamp,
    label,
    type,
    row_number() over (partition by root_id order by root_tstamp) dedupe_index

  from {{ var('snowplow__media_player_event_context') }}

  where root_tstamp between {{ lower_limit }} and {{ upper_limit }}

)

, mp_context as (

  select
    a.root_id,
    a.root_tstamp,
    a.duration,
    a.playback_rate,
    a.current_time,
    a.percent_progress,
    a.muted,
    a.is_live,
    a.loop,
    a.volume,
    row_number() over (partition by root_id order by root_tstamp) dedupe_index

  from {{ var('snowplow__media_player_context') }} a

  where root_tstamp between {{ lower_limit }} and {{ upper_limit }}

)

{% if var("snowplow__enable_youtube") %}

, yt_context as (

  select
    root_id,
    root_tstamp,
    player_id,
    url,
    playback_quality,
    row_number() over (partition by root_id order by root_tstamp) dedupe_index

  from {{ var('snowplow__youtube_context') }}

  where root_tstamp between {{ lower_limit }} and {{ upper_limit }}

)

{% endif %}

{% if var("snowplow__enable_whatwg_media") %}

, me_context as (

select
    root_id,
    root_tstamp,
    media_type,
    current_src,
    html_id,
    row_number() over (partition by root_id order by root_tstamp) dedupe_index

from {{ var('snowplow__html5_media_element_context') }}

where root_tstamp between {{ lower_limit }} and {{ upper_limit }}

)

{% endif %}

{% if var("snowplow__enable_whatwg_video") %}

, ve_context as (

select
    root_id,
    root_tstamp,
    video_width,
    video_height,
    row_number() over (partition by root_id order by root_tstamp) dedupe_index

from {{ var('snowplow__html5_video_element_context') }}

where root_tstamp between {{ lower_limit }} and {{ upper_limit }}

)

{% endif %}

, prep as (

  select
    e.event_id,
    e.page_view_id,
    e.domain_sessionid,
    e.domain_userid,
    e.page_referrer,
    e.page_url,
    mpe.label as media_label,
    round(mp.duration) as duration,
    e.geo_region_name,
    e.br_name,
    e.dvce_type,
    e.os_name,
    e.os_timezone,
    mpe.type as event_type,
    e.derived_tstamp as start_tstamp,
    mp.current_time as player_current_time,
    coalesce(mp.playback_rate, 1) as playback_rate,
    case when mpe.type = 'ended' then 100 else mp.percent_progress end percent_progress,
    mp.muted as is_muted,
    mp.is_live,
    mp.loop,
    mp.volume,
    {% if var("snowplow__enable_whatwg_media") is false and var("snowplow__enable_whatwg_video") %}
       {{ exceptions.raise_compiler_error("variable: snowplow__enable_whatwg_video is enabled but variable: snowplow__enable_whatwg_media is not, both needs to be enabled for modelling html5 video tracking data.") }}
    {% elif var("snowplow__enable_youtube") %}
      {% if var("snowplow__enable_whatwg_media") %}
        coalesce(y.player_id, me.html_id) as media_id,
        case when y.player_id is not null then 'com.youtube-youtube' when me.html_id is not null then 'org.whatwg-media_element' else 'unknown' end as media_player_type,
        coalesce(y.url, me.current_src) as source_url,
        case when me.media_type = 'audio' then 'audio' else 'video' end as media_type,
        {% if var("snowplow__enable_whatwg_video") %}
          coalesce(y.playback_quality, ve.video_width||'x'||ve.video_height) as playback_quality
        {% else %}
          y.playback_quality
        {% endif %}
      {% else %}
        y.player_id as media_id,
        'com.youtube-youtube' as media_player_type,
        y.url as source_url,
        'video' as media_type,
        y.playback_quality
      {% endif %}
    {% elif var("snowplow__enable_whatwg_media") %}
      me.html_id as media_id,
     'org.whatwg-media_element' as media_player_type,
      me.current_src as source_url,
      case when me.media_type = 'audio' then 'audio' else 'video' end as media_type,
      {% if var("snowplow__enable_whatwg_video") %}
        ve.video_width||'x'||ve.video_height as playback_quality
      {% else %}
        'N/A' as playback_quality
      {% endif %}
    {% else %}
      {{ exceptions.raise_compiler_error("No media context enabled. Please enable as many of the following variables as required: snowplow__enable_youtube, snowplow__enable_whatwg_media, snowplow__enable_whatwg_video") }}
    {% endif %}

    from {{ ref("snowplow_web_base_events_this_run") }} as e

    inner join mpe_context as mpe
    on mpe.root_id = e.event_id and mpe.root_tstamp = e.collector_tstamp
    and mpe.dedupe_index = 1

    inner join mp_context as mp
    on mp.root_id = e.event_id and mp.root_tstamp = e.collector_tstamp
    and mp.dedupe_index = 1

  {% if var("snowplow__enable_youtube") %}
    left join yt_context as y
    on y.root_id = e.event_id and y.root_tstamp = e.collector_tstamp
    and y.dedupe_index = 1
  {% endif %}

  {% if var("snowplow__enable_whatwg_media") %}
    left join me_context as me
    on me.root_id = e.event_id and me.root_tstamp = e.collector_tstamp
    and me.dedupe_index = 1
  {% endif %}

  {% if var("snowplow__enable_whatwg_video") %}
    left join ve_context as ve
    on ve.root_id = e.event_id and ve.root_tstamp = e.collector_tstamp
    and ve.dedupe_index = 1
  {% endif %}

)

 select
 {{ dbt_utils.generate_surrogate_key(['p.page_view_id', 'p.media_id' ]) }} play_id,
  p.*,
  coalesce(cast(round(piv.weight_rate * p.duration / 100) as {{ type_int() }}), 0) as play_time_sec,
  coalesce(cast(case when p.is_muted then round(piv.weight_rate * p.duration / 100) end as {{ type_int() }}), 0) as play_time_sec_muted

  from prep p

  left join {{ ref("snowplow_media_player_pivot_base") }} piv
  on p.percent_progress = piv.percent_progress
