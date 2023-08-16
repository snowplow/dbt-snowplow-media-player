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
{% elif not var("snowplow__enable_media_player_v1") and not var("snowplow__enable_media_player_v2") %}
  {{ exceptions.raise_compiler_error("No media player context enabled. Please enable at least one media player context: snowplow__enable_media_player_v1 or snowplow__enable_media_player_v2") }}
{% elif not var("snowplow__enable_youtube") and not var("snowplow__enable_whatwg_media") and not var("snowplow__enable_media_player_v2") %}
  {{ exceptions.raise_compiler_error("No media context enabled. Please enable as many of the following variables as required: snowplow__enable_media_player_v2, snowplow__enable_youtube, snowplow__enable_whatwg_media, snowplow__enable_whatwg_video") }}
{% endif %}

with prep as (

  select

    a.* exclude (
      domain_userid,
      domain_sessionid,
      derived_tstamp

      {% if not var('snowplow__enable_load_tstamp', true) %}
      , load_tstamp
      {% endif %}
    ),

    a.derived_tstamp as start_tstamp,
    b.domain_userid, -- take domain_userid from manifest. This ensures only 1 domain_userid per session.
    b.session_id as session_identifier,

    {{ web_or_mobile_field(
      web={ 'field': 'id', 'col_prefix': 'contexts_com_snowplowanalytics_snowplow_web_page_1', 'dtype': 'varchar' },
      mobile={ 'field': 'id', 'col_prefix': 'contexts_com_snowplowanalytics_mobile_screen_1', 'dtype': 'varchar' }
    ) }} as page_view_id,

    -- unpacking the media player event
    {{ media_player_field(
      v1={ 'field': 'label', 'col_prefix': 'unstruct_event_com_snowplowanalytics_snowplow_media_player_event_1', 'dtype': 'varchar' },
      v2={ 'field': 'label', 'dtype': 'varchar' }
    ) }} as media_label,
    {{ media_event_type_field(media_player_event_type={ 'dtype': 'varchar' }, event_name='a.event_name') }} as event_type,

    -- unpacking the media player object
    round({{ media_player_field(
      v1={ 'field': 'duration', 'dtype': 'float' },
      v2={ 'field': 'duration', 'dtype': 'float' }
    ) }}) as duration_secs,
    {{ media_player_field(
      v1={ 'field': 'currentTime', 'dtype': 'float' },
      v2={ 'field': 'currentTime', 'dtype': 'float' }
    ) }} as current_time,
    {{ media_player_field(
      v1={ 'field': 'playbackRate', 'dtype': 'float' },
      v2={ 'field': 'playbackRate', 'dtype': 'float' },
      default='1.0'
    ) }} as playback_rate,
    {{ percent_progress_field(
        v1_percent_progress={ 'field': 'percentProgress', 'dtype': 'varchar' },
        v1_event_type={ 'field': 'type', 'dtype': 'varchar' },
        event_name='a.event_name',
        v2_current_time={ 'field': 'currentTime', 'dtype': 'float' },
        v2_duration={ 'field': 'duration', 'dtype': 'float' }
    ) }} as percent_progress,
    {{ media_player_field(
      v1={ 'field': 'muted', 'dtype': 'boolean' },
      v2={ 'field': 'muted', 'dtype': 'boolean' }
    ) }} as is_muted,

    -- media session properties
    {{ media_session_field({ 'field': 'mediaSessionId', 'dtype': 'varchar' }) }} as media_session_id,
    {{ media_session_field({ 'field': 'timePlayed', 'dtype': 'float' }) }} as media_session_time_played,
    {{ media_session_field({ 'field': 'timePlayedMuted', 'dtype': 'float' }) }} as media_session_time_played_muted,
    {{ media_session_field({ 'field': 'timePaused', 'dtype': 'float' }) }} as media_session_time_paused,
    {{ media_session_field({ 'field': 'contentWatched', 'dtype': 'float' }) }} as media_session_content_watched,
    {{ media_session_field({ 'field': 'timeBuffering', 'dtype': 'float' }) }} as media_session_time_buffering,
    {{ media_session_field({ 'field': 'timeSpentAds', 'dtype': 'float' }) }} as media_session_time_spent_ads,
    {{ media_session_field({ 'field': 'ads', 'dtype': 'integer' }) }} as media_session_ads,
    {{ media_session_field({ 'field': 'adsClicked', 'dtype': 'integer' }) }} as media_session_ads_clicked,
    {{ media_session_field({ 'field': 'adsSkipped', 'dtype': 'integer' }) }} as media_session_ads_skipped,
    {{ media_session_field({ 'field': 'adBreaks', 'dtype': 'integer' }) }} as media_session_ad_breaks,
    {{ media_session_field({ 'field': 'avgPlaybackRate', 'dtype': 'float' }) }} as media_session_avg_playback_rate,

    -- ad properties
    {{ media_ad_field({ 'field': 'name', 'dtype': 'varchar' }) }} as ad_name,
    {{ media_ad_field({ 'field': 'adId', 'dtype': 'varchar' }) }} as ad_id,
    {{ media_ad_field({ 'field': 'creativeId', 'dtype': 'varchar' }) }} as ad_creative_id,
    {{ media_ad_field({ 'field': 'podPosition', 'dtype': 'integer' }) }} as ad_pod_position,
    {{ media_ad_field({ 'field': 'duration', 'dtype': 'float' }) }} as ad_duration_secs,
    {{ media_ad_field({ 'field': 'skippable', 'dtype': 'boolean' }) }} as ad_skippable,

    -- ad break properties
    {{ media_ad_break_field({ 'field': 'name', 'dtype': 'varchar' }) }} as ad_break_name,
    {{ media_ad_break_field({ 'field': 'breakId', 'dtype': 'varchar' }) }} as ad_break_id,
    {{ media_ad_break_field({ 'field': 'breakType', 'dtype': 'varchar' }) }} as ad_break_type,

    -- ad quartile event
    {{ media_ad_quartile_event_field({ 'field': 'percentProgress', 'dtype': 'integer' }) }} as ad_percent_progress,

    -- combined media properties
    {{ media_id_field(
      v2_player_label={ 'field': 'label', 'dtype': 'varchar' },
      youtube_player_id={ 'field': 'playerId', 'dtype': 'varchar' },
      media_player_id={ 'field': 'htmlId', 'dtype': 'varchar' }
    ) }} as media_id,
    {{ media_player_type_field(
      v2_player_type={ 'field': 'playerType', 'dtype': 'varchar' },
      youtube_player_id={ 'field': 'playerId', 'dtype': 'varchar' },
      media_player_id={ 'field': 'htmlId', 'dtype': 'varchar' }
    ) }} as media_player_type,
    {{ source_url_field(
      youtube_url={ 'field': 'url', 'dtype': 'varchar' },
      media_current_src={ 'field': 'currentSrc', 'dtype': 'varchar' }
    ) }} as source_url,
    {{ media_type_field(
      v2_media_type={ 'field': 'mediaType', 'dtype': 'varchar' },
      media_media_type={ 'field': 'mediaType', 'dtype': 'varchar' }
    ) }} as media_type,
    {{ playback_quality_field(
        v2_quality={ 'field': 'quality', 'dtype': 'varchar' },
        youtube_quality={ 'field': 'playbackQuality', 'dtype': 'varchar' },
        video_width={ 'field': 'videoWidth', 'dtype': 'integer' },
        video_height={ 'field': 'videoHeight', 'dtype': 'integer' }
    )}} as playback_quality

    from {{ var('snowplow__events') }} as a
    inner join {{ ref('snowplow_media_player_base_sessions_this_run') }} as b
    on {{ web_or_mobile_field(
      web='a.domain_sessionid',
      mobile={ 'field': 'sessionId', 'col_prefix': 'contexts_com_snowplowanalytics_snowplow_client_session_1', 'dtype': 'varchar' }
    ) }} = b.session_id

    where a.collector_tstamp <= {{ snowplow_utils.timestamp_add('day', var("snowplow__max_session_days", 3), 'b.start_tstamp') }}
    and a.dvce_sent_tstamp <= {{ snowplow_utils.timestamp_add('day', var("snowplow__days_late_allowed", 3), 'a.dvce_created_tstamp') }}
    and a.collector_tstamp >= {{ lower_limit }}
    and a.collector_tstamp <= {{ upper_limit }}
    and {{ snowplow_utils.app_id_filter(var("snowplow__app_id",[])) }}
    and {{ snowplow_media_player.event_name_filter(var("snowplow__media_event_names", "['media_player_event']")) }}

    qualify row_number() over (partition by a.event_id order by a.collector_tstamp) = 1
)

select
  coalesce(
    p.media_session_id,
    {{ dbt_utils.generate_surrogate_key(['p.page_view_id', 'p.media_id' ]) }}
  ) as play_id,
  p.* exclude (percent_progress),

  cast(p.percent_progress as integer) as percent_progress,

  coalesce(
    cast(piv.weight_rate * p.duration_secs / 100 as {{ type_int() }}),
    0
  ) as play_time_secs,
  coalesce(
    cast(
      case
        when p.is_muted = true then piv.weight_rate * p.duration_secs / 100
        else 0
      end as {{ type_int() }}
    ),
    0
  ) as play_time_muted_secs,

  dense_rank() over (partition by session_identifier order by start_tstamp) AS event_in_session_index

  from prep as p

  left join {{ ref("snowplow_media_player_pivot_base") }} piv
  on p.percent_progress = piv.percent_progress
