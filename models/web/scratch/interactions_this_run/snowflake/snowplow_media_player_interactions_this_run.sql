{{
  config(
    materialized='table',
    tags=["this_run"],
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
  )
}}

with prep as (

 select
    e.event_id,
    e.page_view_id,
    e.domain_sessionid,
    e.domain_userid,
    e.page_referrer,
    e.page_url,
    e.unstruct_event_com_snowplowanalytics_snowplow_media_player_event_1:label::varchar as media_label,
    round(contexts_com_snowplowanalytics_snowplow_media_player_1[0]:duration::int) as duration,
    e.geo_region_name,
    e.br_name,
    e.dvce_type,
    e.os_name,
    e.os_timezone,
    e.unstruct_event_com_snowplowanalytics_snowplow_media_player_event_1:type::varchar as event_type,
    e.derived_tstamp as start_tstamp,
    contexts_com_snowplowanalytics_snowplow_media_player_1[0]:currentTime::float as player_current_time,
    coalesce(contexts_com_snowplowanalytics_snowplow_media_player_1[0]:playbackRate::varchar, 1) as playback_rate,
    cast(case when e.unstruct_event_com_snowplowanalytics_snowplow_media_player_event_1:type::varchar = 'ended' then '100'
        when contexts_com_snowplowanalytics_snowplow_media_player_1[0]:percentProgress::varchar = '' THEN NULL
        else contexts_com_snowplowanalytics_snowplow_media_player_1[0]:percentProgress::varchar END AS int) percent_progress,
    contexts_com_snowplowanalytics_snowplow_media_player_1[0]:muted::boolean as is_muted,
    contexts_com_snowplowanalytics_snowplow_media_player_1[0]:isLive::varchar as is_live,
    contexts_com_snowplowanalytics_snowplow_media_player_1[0]:loop::varchar as loop,
    contexts_com_snowplowanalytics_snowplow_media_player_1[0]:volume::varchar as volume,
    {% if var("snowplow__enable_whatwg_media") is false and var("snowplow__enable_whatwg_video") %}
      {{ exceptions.raise_compiler_error("variable: snowplow__enable_whatwg_video is enabled but variable: snowplow__enable_whatwg_media is not, both need to be enabled for modelling html5 video tracking data.") }}
    {% elif var("snowplow__enable_youtube") %}
      {% if var("snowplow__enable_whatwg_media") %}
        coalesce(e.contexts_com_youtube_youtube_1[0]:playerId::varchar, e.contexts_org_whatwg_media_element_1[0]:htmlId::varchar) as media_id,
        case when e.contexts_com_youtube_youtube_1[0]:playerId is not null then 'com.youtube-youtube'
        when e.contexts_org_whatwg_media_element_1[0]:htmlId::varchar is not null then 'org.whatwg-media_element' else 'unknown' end as media_player_type,
        coalesce(e.contexts_com_youtube_youtube_1[0]:url::varchar, e.contexts_org_whatwg_media_element_1[0]:currentSrc::varchar) as source_url,
        case when e.contexts_org_whatwg_media_element_1[0]:mediaType::varchar = 'audio' then 'audio' else 'video' end as media_type,
        {% if var("snowplow__enable_whatwg_video") %}
          coalesce(e.contexts_com_youtube_youtube_1[0]:playbackQuality::varchar, e.contexts_org_whatwg_video_element_1[0]:videoWidth::varchar||'x'||e.contexts_org_whatwg_video_element_1[0]:videoHeight::varchar) as playback_quality
        {% else %}
          e.contexts_com_youtube_youtube_1[0]:playbackQuality::varchar
        {% endif %}
      {% else %}
        e.contexts_com_youtube_youtube_1[0]:playerId::varchar as media_id,
        'com.youtube-youtube' as media_player_type,
        e.contexts_com_youtube_youtube_1[0]:url::varchar as source_url,
        'video' as media_type,
        e.contexts_com_youtube_youtube_1[0]:playbackQuality::varchar
      {% endif %}
    {% elif var("snowplow__enable_whatwg_media") %}
      e.contexts_org_whatwg_media_element_1[0]:htmlId::varchar as media_id,
      'org.whatwg-media_element' as media_player_type,
      e.contexts_org_whatwg_media_element_1[0]:currentSrc::varchar as source_url,
      case when e.contexts_org_whatwg_media_element_1[0]:mediaType::varchar = 'audio' then 'audio' else 'video' end as media_type,
      {% if var("snowplow__enable_whatwg_video") %}
        e.contexts_org_whatwg_video_element_1[0]:videoWidth::varchar||'x'||e.contexts_org_whatwg_video_element_1[0]:videoHeight::varchar as playback_quality
      {% else %}
        'N/A' as playback_quality
      {% endif %}
    {% else %}
      {{ exceptions.raise_compiler_error("No media context enabled. Please enable as many of the following variables as required: snowplow__enable_youtube, snowplow__enable_whatwg_media, snowplow__enable_whatwg_video") }}
    {% endif %}

    from {{ ref("snowplow_web_base_events_this_run") }} as e

    where event_name = 'media_player_event'
)

 select
  {{ dbt_utils.generate_surrogate_key(['p.page_view_id', 'p.media_id' ]) }} play_id,
  p.*,
  coalesce(cast(piv.weight_rate * p.duration / 100 as {{ type_int() }}), 0) as play_time_sec,
  coalesce(cast(case when p.is_muted = true then piv.weight_rate * p.duration / 100 else 0 end as {{ type_int() }}), 0) as play_time_sec_muted

  from prep p

  left join {{ ref("snowplow_media_player_pivot_base") }} piv
  on p.percent_progress = piv.percent_progress
