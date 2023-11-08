{#
Copyright (c) 2022-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

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
{% elif not var("snowplow__enable_media_player_v1") and not var("snowplow__enable_media_player_v2") %}
  {{ exceptions.raise_compiler_error("No media player context enabled. Please enable at least one media player context: snowplow__enable_media_player_v1 or snowplow__enable_media_player_v2") }}
{% elif not var("snowplow__enable_youtube") and not var("snowplow__enable_whatwg_media") and not var("snowplow__enable_media_player_v2") %}
  {{ exceptions.raise_compiler_error("No media context enabled. Please enable as many of the following variables as required: snowplow__enable_media_player_v2, snowplow__enable_youtube, snowplow__enable_whatwg_media, snowplow__enable_whatwg_video") }}
{% endif %}

with prep as (

  select

    a.* except (
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
      web={ 'field': 'id', 'col_prefix': 'contexts_com_snowplowanalytics_snowplow_web_page_1' },
      mobile={'field': 'id', 'col_prefix': 'contexts_com_snowplowanalytics_mobile_screen_1' }
    ) }} as page_view_id,

    -- unpacking the media player event
    {{ media_player_field(
      v1={ 'field': 'label', 'col_prefix': 'unstruct_event_com_snowplowanalytics_snowplow_media_player_event_1' },
      v2={ 'field': 'label' },
    ) }} as media_label,
    {{ media_event_type_field(media_player_event_type={}, event_name='a.event_name') }} as event_type,

    -- unpacking the media player object
    round({{ media_player_field(
      v1={ 'field': 'duration', 'dtype': 'numeric' },
      v2={ 'field': 'duration', 'dtype': 'numeric' }
    ) }}) as duration_secs,
    {{ media_player_field(
      v1={ 'field': 'current_time', 'dtype': 'numeric' },
      v2={ 'field': 'current_time', 'dtype': 'numeric' }
    ) }} as current_time,
    {{ media_player_field(
      v1={ 'field': 'playback_rate', 'dtype': 'numeric' },
      v2={ 'field': 'playback_rate', 'dtype': 'numeric' },
      default='1.0'
    ) }} as playback_rate,
    {{ percent_progress_field(
        v1_percent_progress={ 'field': 'percent_progress', 'dtype': 'int' },
        v1_event_type={},
        event_name='a.event_name',
        v2_current_time={ 'field': 'current_time', 'dtype': 'numeric' },
        v2_duration={ 'field': 'duration', 'dtype': 'numeric'}
    ) }} as percent_progress,
    {{ media_player_field(
      v1={ 'field': 'muted', 'dtype': 'boolean' },
      v2={ 'field': 'muted', 'dtype': 'boolean' }
    ) }} as is_muted,

    -- media session properties
    {{ media_session_field({ 'field': 'media_session_id' }) }} as media_session_id,
    {{ media_session_field({ 'field': 'time_played', 'dtype': 'numeric' }) }} as media_session_time_played,
    {{ media_session_field({ 'field': 'time_played_muted', 'dtype': 'numeric' }) }} as media_session_time_played_muted,
    {{ media_session_field({ 'field': 'time_paused', 'dtype': 'numeric' }) }} as media_session_time_paused,
    {{ media_session_field({ 'field': 'content_watched', 'dtype': 'numeric' }) }} as media_session_content_watched,
    {{ media_session_field({ 'field': 'time_buffering', 'dtype': 'numeric' }) }} as media_session_time_buffering,
    {{ media_session_field({ 'field': 'time_spent_ads', 'dtype': 'numeric' }) }} as media_session_time_spent_ads,
    {{ media_session_field({ 'field': 'ads', 'dtype': 'int' }) }} as media_session_ads,
    {{ media_session_field({ 'field': 'ads_clicked', 'dtype': 'int' }) }} as media_session_ads_clicked,
    {{ media_session_field({ 'field': 'ads_skipped', 'dtype': 'int' }) }} as media_session_ads_skipped,
    {{ media_session_field({ 'field': 'ad_breaks', 'dtype': 'int' }) }} as media_session_ad_breaks,
    {{ media_session_field({ 'field': 'avg_playback_rate', 'dtype': 'numeric' }) }} as media_session_avg_playback_rate,

    -- ad properties
    {{ media_ad_field({ 'field': 'name' }) }} as ad_name,
    {{ media_ad_field({ 'field': 'ad_id' }) }} as ad_id,
    {{ media_ad_field({ 'field': 'creative_id' }) }} as ad_creative_id,
    {{ media_ad_field({ 'field': 'pod_position', 'dtype': 'int' }) }} as ad_pod_position,
    {{ media_ad_field({ 'field': 'duration', 'dtype': 'numeric' }) }} as ad_duration_secs,
    {{ media_ad_field({ 'field': 'skippable', 'dtype': 'boolean' }) }} as ad_skippable,

    -- ad break properties
    {{ media_ad_break_field({ 'field': 'name' }) }} as ad_break_name,
    {{ media_ad_break_field({ 'field': 'break_id' }) }} as ad_break_id,
    {{ media_ad_break_field({ 'field': 'break_type' }) }} as ad_break_type,

    -- ad quartile event
    {{ media_ad_quartile_event_field({ 'field': 'percent_progress', 'dtype': 'int' }) }} as ad_percent_progress,

    -- combined media properties
    {{ player_id_field(
      youtube_player_id={ 'field': 'player_id' },
      media_player_id={ 'field': 'html_id' }
    ) }} as player_id,
    {{ media_player_type_field(
      v2_player_type={ 'field': 'player_type' },
      youtube_player_id={ 'field': 'player_id' },
      media_player_id={ 'field': 'html_id' }
    ) }} as media_player_type,
    {{ source_url_field(
      youtube_url={ 'field': 'url' },
      media_current_src={ 'field': 'current_src' }
    )}} as source_url,
    {{ media_type_field(
      v2_media_type={ 'field': 'media_type' },
      media_media_type={ 'field': 'media_type' }
    ) }} as media_type,
    {{ playback_quality_field(
        v2_quality={ 'field': 'quality' },
        youtube_quality={ 'field': 'playback_quality' },
        video_width={ 'field': 'video_width', 'dtype': 'int' },
        video_height={ 'field': 'video_height', 'dtype': 'int' }
    ) }} as playback_quality

    from {{ var('snowplow__events') }} as a
    inner join {{ ref('snowplow_media_player_base_sessions_this_run') }} as b
      on {{ web_or_mobile_field(
        web='a.domain_sessionid',
        mobile={ 'field': 'session_id', 'col_prefix': 'contexts_com_snowplowanalytics_snowplow_client_session_1', 'dtype': 'string' }
      ) }} = b.session_id

    where a.collector_tstamp <= {{ snowplow_utils.timestamp_add('day', var("snowplow__max_session_days", 3), 'b.start_tstamp') }}
    and a.dvce_sent_tstamp <= {{ snowplow_utils.timestamp_add('day', var("snowplow__days_late_allowed", 3), 'a.dvce_created_tstamp') }}
    and a.collector_tstamp >= {{ lower_limit }}
    and a.collector_tstamp <= {{ upper_limit }}
    {% if var('snowplow__derived_tstamp_partitioned', true) and target.type == 'bigquery' | as_bool() %}
      and a.derived_tstamp >= {{ snowplow_utils.timestamp_add('hour', -1, lower_limit) }}
      and a.derived_tstamp <= {{ upper_limit }}
    {% endif %}
    and {{ snowplow_utils.app_id_filter(var("snowplow__app_id",[])) }}
    and {{ snowplow_media_player.event_name_filter(var("snowplow__media_event_names", "['media_player_event']")) }}

    qualify row_number() over (partition by a.event_id order by a.collector_tstamp) = 1

)

, ranked as (

  select
    *,
    dense_rank() over (partition by ev.session_identifier order by ev.start_tstamp) AS event_in_session_index,
  from prep as ev

)

select
  coalesce(
    p.media_session_id,
    {{ dbt_utils.generate_surrogate_key(['p.page_view_id', 'p.player_id', 'p.media_label', 'p.media_type', 'p.media_player_type']) }}
  ) as play_id,
  {{ dbt_utils.generate_surrogate_key(['p.player_id', 'p.media_label', 'p.media_type', 'p.media_player_type']) }} as media_identifier,
  p.*,

  coalesce(cast(piv.weight_rate * p.duration_secs / 100 as {{ type_int() }}), 0) as play_time_secs,
  coalesce(cast(case when p.is_muted = true then piv.weight_rate * p.duration_secs / 100 else 0 end as {{ type_int() }}), 0) as play_time_muted_secs

  from ranked p

  left join {{ ref("snowplow_media_player_pivot_base") }} piv
  on p.percent_progress = piv.percent_progress
