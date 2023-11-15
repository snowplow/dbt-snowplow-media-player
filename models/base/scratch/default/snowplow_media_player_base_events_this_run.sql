{#
Copyright (c) 2022-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{{
    config(
        sort='collector_tstamp',
        dist='event_id',
        tags=["this_run"]
    )
}}



{# Check for exceptions #}
{% if var("snowplow__enable_whatwg_media") is false and var("snowplow__enable_whatwg_video") %}
  {{ exceptions.raise_compiler_error("variable: snowplow__enable_whatwg_video is enabled but variable: snowplow__enable_whatwg_media is not, both need to be enabled for modelling html5 video tracking data.") }}
{% elif not var("snowplow__enable_media_player_v1") and not var("snowplow__enable_media_player_v2") %}
  {{ exceptions.raise_compiler_error("No media player context enabled. Please enable at least one media player context: snowplow__enable_media_player_v1 or snowplow__enable_media_player_v2") }}
{% elif not var("snowplow__enable_youtube") and not var("snowplow__enable_whatwg_media") and not var("snowplow__enable_media_player_v2") %}
  {{ exceptions.raise_compiler_error("No media context enabled. Please enable as many of the following variables as required: snowplow__enable_media_player_v2, snowplow__enable_youtube, snowplow__enable_whatwg_media, snowplow__enable_whatwg_video") }}
{% endif %}

{# Setting sdes or contexts for Postgres / Redshift. dbt passes variables by reference so need to use copy to avoid altering the list multiple times #}
{% set contexts = var('snowplow__entities_or_sdes', []).copy() %}

{% if var("snowplow__enable_mobile_events") %}
  {% do contexts.append({'schema': var('snowplow__context_screen'), 'prefix': 'mobile_screen_', 'single_entity': True}) %}
  {% do contexts.append({'schema': var('snowplow__context_mobile_session'), 'prefix': 'mobile_session_', 'single_entity': True}) %}
{% endif %}

{% if var("snowplow__enable_media_player_v1") %}
  {% do contexts.append({'schema': var('snowplow__media_player_event_context'), 'prefix': 'media_player_event_', 'single_entity': True}) %}
  {% do contexts.append({'schema': var('snowplow__media_player_context'), 'prefix': 'media_player_', 'single_entity': True}) %}
{% endif %}

{% if var("snowplow__enable_media_player_v2") %}
  {% do contexts.append({'schema': var('snowplow__media_player_v2_context'), 'prefix': 'media_player_v2_', 'single_entity': True}) %}
{% endif %}

{% if var("snowplow__enable_media_session") %}
  {% do contexts.append({'schema': var('snowplow__media_session_context'), 'prefix': 'media_session_', 'single_entity': True}) %}
{% endif %}

{% if var("snowplow__enable_media_ad") %}
  {% do contexts.append({'schema': var('snowplow__media_ad_context'), 'prefix': 'media_ad_', 'single_entity': True}) %}
{% endif %}

{% if var("snowplow__enable_media_ad_break") %}
  {% do contexts.append({'schema': var('snowplow__media_ad_break_context'), 'prefix': 'media_ad_break_', 'single_entity': True}) %}
{% endif %}

{%- if var("snowplow__enable_youtube") -%}
  {% do contexts.append({'schema': var('snowplow__youtube_context'), 'prefix': 'youtube_', 'single_entity': True}) %}
{%- endif %}

{% if var("snowplow__enable_whatwg_media") -%}
  {% do contexts.append({'schema': var('snowplow__html5_media_element_context'), 'prefix': 'html5_media_element_', 'single_entity': True}) %}
{%- endif %}

{% if var("snowplow__enable_whatwg_video") -%}
  {% do contexts.append({'schema': var('snowplow__html5_video_element_context'), 'prefix': 'html5_video_element_', 'single_entity': True}) %}
{%- endif %}

{% if var("snowplow__enable_web_events") %}
  {% do contexts.append({'schema': var('snowplow__context_web_page'), 'prefix': 'web_page_', 'single_entity': True}) %}
{% endif %}

{% if var("snowplow__enable_ad_quartile_event") %}
  {% do contexts.append({'schema': var('snowplow__media_ad_quartile_event'), 'prefix': 'ad_quartile_event_', 'single_entity': True}) %}
{% endif %}

{% set base_events_query = snowplow_utils.base_create_snowplow_events_this_run(
    sessions_this_run_table='snowplow_media_player_base_sessions_this_run',
    session_identifiers=session_identifiers(),
    session_sql=var('snowplow__session_sql', none),
    session_timestamp=var('snowplow__session_timestamp', 'collector_tstamp'),
    derived_tstamp_partitioned=var('snowplow__derived_tstamp_partitioned', true),
    days_late_allowed=var('snowplow__days_late_allowed', 3),
    max_session_days=var('snowplow__max_session_days', 3),
    app_ids=var('snowplow__app_id', []),
    snowplow_events_database=var('snowplow__database', target.database) if target.type not in ['databricks', 'spark'] else var('snowplow__databricks_catalog', 'hive_metastore') if target.type in ['databricks'] else var('snowplow__atomic_schema', 'atomic'),
    snowplow_events_schema=var('snowplow__atomic_schema', 'atomic'),
    snowplow_events_table=var('snowplow__events_table', 'events'),
    entities_or_sdes=contexts,
    custom_sql=var('snowplow__custom_sql', '')
) %}

with base_query as (
  {{ base_events_query }}
),

prep as (
  select
    ev.*,

    {{ web_or_mobile_field(web='ev.web_page__id', mobile='ev.mobile_screen__id') }} as page_view_id,
    {{ web_or_mobile_field(web='ev.domain_sessionid', mobile='ev.mobile_session__session_id') }} as original_session_identifier,

    -- unpacking the media player event
    {{ media_player_field(v1='ev.media_player_event__label', v2='ev.media_player_v2__label') }} as media_label,
    {{ media_event_type_field(media_player_event_type='ev.media_player_event__type', event_name='ev.event_name') }} as event_type,

    -- unpacking the media player object
    round({{ media_player_field(v1='ev.media_player__duration', v2='ev.media_player_v2__duration') }}) as duration_secs,
    {{ media_player_field(v1='ev.media_player__current_time', v2='ev.media_player_v2__current_time') }} as player_current_time,
    {{ media_player_field(
        v1='ev.media_player__playback_rate',
        v2='ev.media_player_v2__playback_rate',
        default='1'
    ) }} as playback_rate,
    {{ percent_progress_field(
        v1_percent_progress='ev.media_player__percent_progress',
        v1_event_type='ev.media_player_event__type',
        event_name='ev.event_name',
        v2_current_time='ev.media_player_v2__current_time',
        v2_duration='ev.media_player_v2__duration'
    ) }} as percent_progress,
    {{ media_player_field(v1='ev.media_player__muted', v2='ev.media_player_v2__muted') }} as is_muted,

    -- media session properties
    cast({{ media_session_field('ev.media_session__media_session_id') }} as {{ type_string() }}) as media_session_id, -- This is the only key actually used regardless, redshift doesn't like casting a null at a later time
    {{ media_session_field('ev.media_session__time_played') }} as media_session_time_played,
    {{ media_session_field('ev.media_session__time_played_muted') }} as media_session_time_played_muted,
    {{ media_session_field('ev.media_session__time_paused') }} as media_session_time_paused,
    {{ media_session_field('ev.media_session__content_watched') }} as media_session_content_watched,
    {{ media_session_field('ev.media_session__time_buffering') }} as media_session_time_buffering,
    {{ media_session_field('ev.media_session__time_spent_ads') }} as media_session_time_spent_ads,
    {{ media_session_field('ev.media_session__ads') }} as media_session_ads,
    {{ media_session_field('ev.media_session__ads_clicked') }} as media_session_ads_clicked,
    {{ media_session_field('ev.media_session__ads_skipped') }} as media_session_ads_skipped,
    {{ media_session_field('ev.media_session__ad_breaks') }} as media_session_ad_breaks,
    {{ media_session_field('ev.media_session__avg_playback_rate') }} as media_session_avg_playback_rate,

    -- ad properties
    {{ media_ad_field('ev.media_ad__name') }} as ad_name,
    {{ media_ad_field('ev.media_ad__ad_id') }} as ad_id,
    {{ media_ad_field('ev.media_ad__creative_id') }} as ad_creative_id,
    {{ media_ad_field('ev.media_ad__pod_position') }} as ad_pod_position,
    {{ media_ad_field('ev.media_ad__duration') }} as ad_duration_secs,
    {{ media_ad_field('ev.media_ad__skippable') }} as ad_skippable,

    -- ad break properties
    {{ media_ad_break_field('ev.media_ad_break__name') }} as ad_break_name,
    {{ media_ad_break_field('ev.media_ad_break__break_id') }} as ad_break_id,
    {{ media_ad_break_field('ev.media_ad_break__break_type') }} as ad_break_type,

    -- ad quartile event
    {{ media_ad_quartile_event_field('ev.ad_quartile_event__percent_progress') }} as ad_percent_progress,

    -- combined media properties
    {{ player_id_field(youtube_player_id='ev.youtube__player_id', media_player_id='ev.html5_media_element__html_id') }} as player_id,
    {{ media_player_type_field(v2_player_type='ev.media_player_v2__player_type', youtube_player_id='youtube__player_id', media_player_id='ev.html5_media_element__html_id') }} as media_player_type,
    {{ source_url_field(youtube_url='ev.youtube__url', media_current_src='ev.html5_media_element__current_src')}} as source_url,
    {{ media_type_field(v2_media_type='ev.media_player_v2__media_type', media_media_type='ev.html5_media_element__media_type')}} as media_type,
    {{ playback_quality_field(
        v2_quality='ev.media_player_v2__quality',
        youtube_quality='ev.youtube__playback_quality',
        video_width='ev.html5_video_element__video_width',
        video_height='ev.html5_video_element__video_height'
    )}} as playback_quality,

    dense_rank() over (partition by ev.session_identifier order by ev.derived_tstamp) as event_in_session_index,
    ev.derived_tstamp as start_tstamp

    from base_query ev

    where
      {{ snowplow_media_player.event_name_filter(var("snowplow__media_event_names", "['media_player_event']")) }}

)

select
  coalesce(
    p.media_session_id,
    {{ dbt_utils.generate_surrogate_key(['p.page_view_id', 'p.player_id', 'p.media_label', 'p.media_type', 'p.media_player_type']) }}
  ) as play_id,
  {{ dbt_utils.generate_surrogate_key(['p.player_id', 'p.media_label', 'p.media_type', 'p.media_player_type']) }} as media_identifier,
  p.*,
  coalesce(cast(round(piv.weight_rate * p.duration_secs / 100) as {{ type_int() }}), 0) as play_time_secs,
  coalesce(cast(case when p.is_muted then round(piv.weight_rate * p.duration_secs / 100) end as {{ type_int() }}), 0) as play_time_muted_secs

  from prep p

  left join {{ ref("snowplow_media_player_pivot_base") }} piv
  on p.percent_progress = piv.percent_progress
