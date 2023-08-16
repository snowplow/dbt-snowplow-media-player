{{
  config(
    tags=["this_run"],
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
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

with prep as (

  select

    a.event_id,
    a.contexts_com_snowplowanalytics_snowplow_web_page_1[0]:id::varchar as page_view_id,
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
    a.unstruct_event_com_snowplowanalytics_snowplow_media_player_event_1:label::varchar as media_label,
    a.unstruct_event_com_snowplowanalytics_snowplow_media_player_event_1:type::varchar as event_type,

    -- unpacking the media player object
    round(a.contexts_com_snowplowanalytics_snowplow_media_player_1[0]:duration::float) as duration_secs,
    a.contexts_com_snowplowanalytics_snowplow_media_player_1[0]:currentTime::float as player_current_time,
    coalesce(a.contexts_com_snowplowanalytics_snowplow_media_player_1[0]:playbackRate::varchar, 1) as playback_rate,
    cast(
      case
        when a.unstruct_event_com_snowplowanalytics_snowplow_media_player_event_1:type::varchar = 'ended' then '100'
        when a.contexts_com_snowplowanalytics_snowplow_media_player_1[0]:percentProgress::varchar = '' then null
        else a.contexts_com_snowplowanalytics_snowplow_media_player_1[0]:percentProgress::varchar
      end as int
    ) as percent_progress,
    a.contexts_com_snowplowanalytics_snowplow_media_player_1[0]:muted::boolean as is_muted,
    a.contexts_com_snowplowanalytics_snowplow_media_player_1[0]:isLive::varchar as is_live,
    a.contexts_com_snowplowanalytics_snowplow_media_player_1[0]:loop::varchar as loop,
    a.contexts_com_snowplowanalytics_snowplow_media_player_1[0]:volume::varchar as volume,

    -- combined media properties
    {{ media_id_col(
      youtube_player_id='a.contexts_com_youtube_youtube_1[0]:playerId',
      media_player_id='a.contexts_org_whatwg_media_element_1[0]:htmlId::varchar'
    ) }},
    {{ media_player_type_col(
      youtube_player_id='a.contexts_com_youtube_youtube_1[0]:playerId',
      media_player_id='a.contexts_org_whatwg_media_element_1[0]:htmlId::varchar'
    ) }},
    {{ source_url_col(
      youtube_url='a.contexts_com_youtube_youtube_1[0]:url::varchar',
      media_current_src='a.contexts_org_whatwg_media_element_1[0]:currentSrc::varchar'
    ) }},
    {{ media_type_col(
      media_media_type='a.contexts_org_whatwg_media_element_1[0]:mediaType::varchar'
    ) }},
    {{ playback_quality_col(
      youtube_quality='a.contexts_com_youtube_youtube_1[0]:playbackQuality::varchar',
      video_width='a.contexts_org_whatwg_video_element_1[0]:videoWidth::varchar',
      video_height='a.contexts_org_whatwg_video_element_1[0]:videoHeight::varchar'
    )}}

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
  {{ dbt_utils.generate_surrogate_key(['p.page_view_id', 'p.media_id' ]) }} as play_id,
  p.*,
  coalesce(
    cast(piv.weight_rate * p.duration_secs / 100 as {{ type_int() }}),
    0
  ) as play_time_sec,
  coalesce(
    cast(
      case
        when p.is_muted = true then piv.weight_rate * p.duration_secs / 100
        else 0
      end as {{ type_int() }}
    ),
    0
  ) as play_time_sec_muted,

  dense_rank() over (partition by session_id order by start_tstamp) AS event_in_session_index

  from prep as p

  left join {{ ref("snowplow_media_player_pivot_base") }} piv
  on p.percent_progress = piv.percent_progress
