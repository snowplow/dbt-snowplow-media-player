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

{%- set player_1_prefix = 'a.contexts_com_snowplowanalytics_snowplow_media_player_1[0]:' %}
{%- set player_2_prefix = 'a.contexts_com_snowplowanalytics_snowplow_media_player_2[0]:' %}
{%- set media_event_prefix = 'a.unstruct_event_com_snowplowanalytics_snowplow_media_player_event_1:' %}
{%- set session_prefix = 'a.contexts_com_snowplowanalytics_snowplow_media_session_1[0]:' %}
{%- set ad_prefix = 'a.contexts_com_snowplowanalytics_snowplow_media_ad_1[0]:' %}
{%- set ad_break_prefix = 'a.contexts_com_snowplowanalytics_snowplow_media_ad_break_1[0]:' %}
{%- set ad_quartile_prefix = 'a.unstruct_event_com_snowplowanalytics_snowplow_media_ad_quartile_event_1:' %}
{%- set youtube_prefix = 'a.contexts_com_youtube_youtube_1[0]:' %}
{%- set whatwg_media_prefix = 'a.contexts_org_whatwg_media_element_1[0]:' %}
{%- set whatwg_video_prefix = 'a.contexts_org_whatwg_video_element_1[0]:' %}

with prep AS (

  select

    a.event_id,
    a.contexts_com_snowplowanalytics_snowplow_web_page_1[0].id::string as page_view_id,
    b.session_id,
    b.domain_userid,
    a.user_id,
    a.platform,
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
    {{ media_player_property_col(
      v1_property=media_event_prefix + 'label::STRING',
      v2_property=player_2_prefix + 'label::STRING'
    ) }} as media_label,
    {{ media_event_type_col(
      media_player_event_type=media_event_prefix + ':type::STRING',
      event_name='a.event_name'
    ) }},

    -- unpacking the media player object
    round({{ media_player_property_col(v1_property=player_1_prefix + 'duration::float', v2_property=player_2_prefix + 'duration::float') }}) as duration_secs,
    {{ media_player_property_col(v1_property=player_1_prefix + 'current_time::float', v2_property=player_2_prefix + 'current_time::float') }} as current_time,
    {{ media_player_property_col(
      v1_property=player_1_prefix + 'playback_rate::float',
      v2_property=player_2_prefix + 'playback_rate::float',
      default='1.0'
    ) }} as playback_rate,
    {{ percent_progress_col(
        v1_percent_progress=player_1_prefix + 'percent_progress::STRING',
        v1_event_type=media_event_prefix + 'type::STRING',
        event_name='a.event_name',
        v2_current_time=player_2_prefix + 'current_time::float',
        v2_duration=player_2_prefix + 'duration::float'
    ) }},
    {{ media_player_property_col(v1_property=player_1_prefix + 'muted::boolean', v2_property=player_2_prefix + 'muted::boolean') }} as is_muted,

    -- media session properties
    {{ media_session_property_col(property=session_prefix + 'media_session_id::STRING') }} as media_session_id,
    {{ media_session_property_col(property=session_prefix + 'time_played::float') }} as media_session_time_played,
    {{ media_session_property_col(property=session_prefix + 'time_played_muted::float') }} as media_session_time_played_muted,
    {{ media_session_property_col(property=session_prefix + 'time_paused::float') }} as media_session_time_paused,
    {{ media_session_property_col(property=session_prefix + 'content_watched::float') }} as media_session_content_watched,
    {{ media_session_property_col(property=session_prefix + 'time_buffering::float') }} as media_session_time_buffering,
    {{ media_session_property_col(property=session_prefix + 'time_spent_ads::float') }} as media_session_time_spent_ads,
    {{ media_session_property_col(property=session_prefix + 'ads::integer') }} as media_session_ads,
    {{ media_session_property_col(property=session_prefix + 'ads_clicked::integer') }} as media_session_ads_clicked,
    {{ media_session_property_col(property=session_prefix + 'ads_skipped::integer') }} as media_session_ads_skipped,
    {{ media_session_property_col(property=session_prefix + 'ad_breaks::integer') }} as media_session_ad_breaks,
    {{ media_session_property_col(property=session_prefix + 'avg_playback_rate::float') }} as media_session_avg_playback_rate,

    -- ad properties
    {{ media_ad_property_col(property=ad_prefix + 'name::STRING') }} as ad_name,
    {{ media_ad_property_col(property=ad_prefix + 'ad_id::STRING') }} as ad_id,
    {{ media_ad_property_col(property=ad_prefix + 'creative_id::STRING') }} as ad_creative_id,
    {{ media_ad_property_col(property=ad_prefix + 'pod_position::integer') }} as ad_pod_position,
    {{ media_ad_property_col(property=ad_prefix + 'duration::float') }} as ad_duration,
    {{ media_ad_property_col(property=ad_prefix + 'skippable::boolean') }} as ad_skippable,

    -- ad break properties
    {{ media_ad_break_property_col(property=ad_break_prefix + 'name::STRING') }} as ad_break_name,
    {{ media_ad_break_property_col(property=ad_break_prefix + 'break_id::STRING') }} as ad_break_id,
    {{ media_ad_break_property_col(property=ad_break_prefix + 'break_type::STRING') }} as ad_break_type,

    -- ad quartile event
    {{ media_ad_quartile_event_property_col(property=ad_quartile_prefix + 'percent_progress::integer') }} as ad_percent_progress,

    -- combined media properties
    {{ media_id_col(
      v2_player_label=player_2_prefix + 'label::STRING',
      youtube_player_id=youtube_prefix + 'player_id::STRING',
      media_player_id=whatwg_media_prefix + 'html_id::STRING'
    ) }},
    {{ media_player_type_col(
      v2_player_type=player_2_prefix + 'player_type::STRING',
      youtube_player_id=youtube_prefix + 'player_id::STRING',
      media_player_id=whatwg_media_prefix + 'html_id::STRING'
    ) }},
    {{ source_url_col(
      youtube_url=youtube_prefix + 'url::STRING',
      media_current_src=whatwg_media_prefix + 'current_src::STRING'
    ) }},
    {{ media_type_col(
      v2_media_type=player_2_prefix + 'media_type::STRING',
      media_media_type=whatwg_media_prefix + 'media_type::STRING'
    ) }},
    {{ playback_quality_col(
        v2_quality=player_2_prefix + 'quality::STRING',
        youtube_quality=youtube_prefix + 'playback_quality::STRING',
        video_width=whatwg_video_prefix + 'video_width::integer',
        video_height=whatwg_video_prefix + 'video_height::integer'
    )}}

  from {{ var('snowplow__events') }} as a
  inner join {{ ref('snowplow_media_player_base_sessions_this_run') }} as b
  on {{ web_or_mobile_col(
    web_property='a.domain_sessionid',
    mobile_property='a.contexts_com_snowplowanalytics_snowplow_client_session_1[0]:sessionId::STRING'
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
  p.*,

  coalesce(cast(round(piv.weight_rate * p.duration_secs / 100) as {{ type_int() }}), 0) as play_time_secs,
  coalesce(cast(case when p.is_muted = true then round(piv.weight_rate * p.duration_secs / 100) else 0 end as {{ type_int() }}), 0) as play_time_muted_secs,

  dense_rank() over (partition by session_id order by start_tstamp) AS event_in_session_index

  from prep p

  left join {{ ref("snowplow_media_player_pivot_base") }} piv
  on p.percent_progress = piv.percent_progress
