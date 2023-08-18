{{
  config(
    tags=["this_run"]
  )
}}

{%- set lower_limit, upper_limit = snowplow_utils.return_limits_from_model(ref('snowplow_media_player_base_sessions_this_run'),
                                                                          'start_tstamp',
                                                                          'end_tstamp') %}

-- check for exceptions
{% if var("snowplow__enable_whatwg_media") is false and var("snowplow__enable_whatwg_video") %}
  {{ exceptions.raise_compiler_error("variable: snowplow__enable_whatwg_video is enabled but variable: snowplow__enable_whatwg_media is not, both need to be enabled for modelling html5 video tracking data.") }}
{% elif not var("snowplow__enable_youtube") and not var("snowplow__enable_whatwg_media") %}
  {{ exceptions.raise_compiler_error("No media context enabled. Please enable as many of the following variables as required: snowplow__enable_youtube, snowplow__enable_whatwg_media, snowplow__enable_whatwg_video") }}
{% endif %}

with prep AS (

  select

    a.event_id,
    a.contexts_com_snowplowanalytics_snowplow_web_page_1[0].id::string as page_view_id,
    b.session_id,
    b.domain_userid,
    a.page_referrer,
    a.page_url,
    a.geo_region_name,
    a.br_name,
    a.dvce_type,
    a.os_name,
    a.os_timezone,
    a.derived_tstamp as start_tstamp,
    a.collector_tstamp,

    -- unpacking the media player event
    a.unstruct_event_com_snowplowanalytics_snowplow_media_player_event_1.label::STRING as media_label,
    a.unstruct_event_com_snowplowanalytics_snowplow_media_player_event_1.type::STRING as event_type,

    -- unpacking the media player object
    round(contexts_com_snowplowanalytics_snowplow_media_player_1[0].duration::float) as duration_secs,
    contexts_com_snowplowanalytics_snowplow_media_player_1[0].current_time::float as player_current_time,
    coalesce(contexts_com_snowplowanalytics_snowplow_media_player_1[0].playback_rate::STRING, 1) as playback_rate,
    case when a.unstruct_event_com_snowplowanalytics_snowplow_media_player_event_1.type::STRING = 'ended' then 100 else contexts_com_snowplowanalytics_snowplow_media_player_1[0].percent_progress::int end percent_progress,
    contexts_com_snowplowanalytics_snowplow_media_player_1[0].muted::STRING as is_muted,
    contexts_com_snowplowanalytics_snowplow_media_player_1[0].is_live::STRING as is_live,
    contexts_com_snowplowanalytics_snowplow_media_player_1[0].loop::STRING as loop,
    contexts_com_snowplowanalytics_snowplow_media_player_1[0].volume::STRING as volume,

    -- combined media properties
    {{ media_id_col(
      v2_player_label='a.contexts_com_snowplowanalytics_snowplow_media_player_2[0].label::STRING',
      youtube_player_id='a.contexts_com_youtube_youtube_1[0].player_id::STRING',
      media_player_id='a.contexts_org_whatwg_media_element_1[0].html_id::STRING'
    ) }},
    {{ media_player_type_col(
      v2_player_type='a.contexts_com_snowplowanalytics_snowplow_media_player_2[0].player_type::STRING',
      youtube_player_id='a.contexts_com_youtube_youtube_1[0].player_id::STRING',
      media_player_id='a.contexts_org_whatwg_media_element_1[0].html_id::STRING'
    ) }},
    {{ source_url_col(
      youtube_url='a.contexts_com_youtube_youtube_1[0].url::STRING',
      media_current_src='a.contexts_org_whatwg_media_element_1[0].current_src::STRING'
    ) }},
    {{ media_type_col(
      v2_media_type='a.contexts_com_snowplowanalytics_snowplow_media_player_2[0].media_type::STRING',
      media_media_type='a.contexts_org_whatwg_media_element_1[0].media_type::STRING'
    ) }},
    {{ playback_quality_col(
      v2_quality='a.contexts_com_snowplowanalytics_snowplow_media_player_2[0].quality::STRING',
      youtube_quality='a.contexts_com_youtube_youtube_1[0].playback_quality::STRING',
      video_width='a.contexts_org_whatwg_video_element_1[0].video_width::STRING',
      video_height='a.contexts_org_whatwg_video_element_1[0].video_height::STRING'
    ) }}

  from {{ var('snowplow__events') }} as a
  inner join {{ ref('snowplow_media_player_base_sessions_this_run') }} as b
  on a.domain_sessionid = b.session_id

  where a.collector_tstamp <= {{ snowplow_utils.timestamp_add('day', var("snowplow__max_session_days", 3), 'b.start_tstamp') }}
  and a.dvce_sent_tstamp <= {{ snowplow_utils.timestamp_add('day', var("snowplow__days_late_allowed", 3), 'a.dvce_created_tstamp') }}
  and a.collector_tstamp >= {{ lower_limit }}
  and a.collector_tstamp <= {{ upper_limit }}
  and {{ snowplow_utils.app_id_filter(var("snowplow__app_id",[])) }}
  and {{ snowplow_media_player.event_name_filter(var("snowplow__media_event_names", "['media_player_event']")) }}

  qualify row_number() over (partition by a.event_id order by a.collector_tstamp) = 1
)

select
  {{ dbt_utils.generate_surrogate_key(['p.page_view_id', 'p.media_id' ]) }} play_id,
  p.*,
  coalesce(cast(round(piv.weight_rate * p.duration_secs / 100) as {{ type_int() }}), 0) as play_time_secs,
  coalesce(cast(case when p.is_muted = true then round(piv.weight_rate * p.duration_secs / 100) else 0 end as {{ type_int() }}), 0) as play_time_muted_secs,

  dense_rank() over (partition by session_id order by start_tstamp) AS event_in_session_index

  from prep p

  left join {{ ref("snowplow_media_player_pivot_base") }} piv
  on p.percent_progress = piv.percent_progress
