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

{# Setting sdes or contexts for Postgres / Redshift. dbt passes variables by reference so need to use copy to avoid altering the list multiple times #}
{% set contexts = var('snowplow__entities_or_sdes', []).copy() %}

{% if var("snowplow__enable_mobile_events") %}
  {% do contexts.append({'schema': var('snowplow__context_screen'), 'prefix': 'mobile_screen_', 'single_entity': True}) %}
  {% do contexts.append({'schema': var('snowplow__context_mobile_session'), 'prefix': 'mobile_session_', 'single_entity': True}) %}
{% endif %}

{% if var("snowplow__enable_media_player_v1") %}
  {% do contexts.append({'schema': var('snowplow__media_player_event_context'), 'prefix': 'media_player_event_', 'single_entity': True}) %}
  {% do contexts.append({'schema': var('snowplow__media_player_context'), 'prefix': 'media_player_v1_', 'single_entity': True}) %}
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
)

, prep as (
  select
    ev.*

    {{ get_context_fields(
      enabled=var('snowplow__enable_web_events', false),
      context='contexts_com_snowplowanalytics_snowplow_web_page_1',
      prefix='web_page_',
      fields=[
        {'field':'id', 'dtype':'string'},
      ]) }}

    {{ get_context_fields(
      enabled=var('snowplow__enable_mobile_events', false),
      context='contexts_com_snowplowanalytics_mobile_screen_1',
      prefix='mobile_screen_',
      fields=[
        {'field':'id', 'dtype':'string'},
      ]) }}

    {{ get_context_fields(
      enabled=var('snowplow__enable_mobile_events', false),
      context='contexts_com_snowplowanalytics_snowplow_client_session_1',
      prefix='mobile_session_',
      fields=[
        {'field':'sessionId', 'dtype':'string'},
      ]) }}

    {{ get_context_fields(
      enabled=var('snowplow__enable_media_session', false),
      context='contexts_com_snowplowanalytics_snowplow_media_session_1',
      prefix='media_session_',
      fields=[
        {'field':'mediaSessionId', 'dtype': 'string'},
        {'field':'timePlayed', 'dtype': 'number'},
        {'field':'timePlayedMuted', 'dtype': 'number'},
        {'field':'timePaused', 'dtype': 'number'},
        {'field':'contentWatched', 'dtype': 'number'},
        {'field':'timeBuffering', 'dtype': 'number'},
        {'field':'timeSpentAds', 'dtype': 'number'},
        {'field':'ads', 'dtype': 'integer'},
        {'field':'adsClicked', 'dtype': 'integer'},
        {'field':'adsSkipped', 'dtype': 'integer'},
        {'field':'adBreaks', 'dtype': 'integer'},
        {'field':'avgPlaybackRate', 'dtype': 'number'},
      ]) }}

    {{ get_context_fields(
      enabled=var('snowplow__enable_media_ad', false),
      context='contexts_com_snowplowanalytics_snowplow_media_ad_1',
      prefix='media_ad_',
      fields=[
        {'field':'name', 'dtype':'string'},
        {'field':'adId', 'dtype':'string'},
        {'field':'creativeId', 'dtype':'string'},
        {'field':'podPosition', 'dtype':'integer'},
        {'field':'duration', 'dtype':'integer'},
        {'field':'skippable', 'dtype':'boolean'},
      ]) }}

    {{ get_context_fields(
      enabled=var('snowplow__enable_media_ad_break', false),
      context='contexts_com_snowplowanalytics_snowplow_media_ad_break_1',
      prefix='media_ad_break_',
      fields=[
        {'field':'name', 'dtype':'string'},
        {'field':'breakId', 'dtype':'string'},
        {'field':'breakType', 'dtype':'string'},
      ]) }}

    {{ get_context_fields(
      enabled=var('snowplow__enable_ad_quartile_event', false),
      context='unstruct_event_com_snowplowanalytics_snowplow_media_ad_quartile_event_1',
      prefix='ad_quartile_event_',
      fields=[
        {'field':'percentProgress', 'dtype':'integer'}
      ]) }}

    {{ get_context_fields(
      enabled=var('snowplow__enable_media_player_v1', false),
      context='contexts_com_snowplowanalytics_snowplow_media_player_1',
      prefix='media_player_v1_',
      fields=[
        {'field':'duration', 'dtype':'float'},
        {'field':'currentTime', 'dtype':'float'},
        {'field':'playbackRate', 'dtype':'number'},
        {'field':'muted', 'dtype':'boolean'},
        {'field':'percentProgress', 'dtype':'integer'},
      ]) }}

    {{ get_context_fields(
      enabled=var('snowplow__enable_media_player_v2', false),
      context='contexts_com_snowplowanalytics_snowplow_media_player_2',
      prefix='media_player_v2_',
      fields=[
        {'field':'duration', 'dtype':'float'},
        {'field':'currentTime', 'dtype':'float'},
        {'field':'playbackRate', 'dtype':'number'},
        {'field':'muted', 'dtype':'boolean'},
        {'field':'label', 'dtype':'string'},
        {'field':'playerType', 'dtype':'string'},
        {'field':'mediaType', 'dtype':'string'},
        {'field':'quality', 'dtype':'string'},
      ]) }}

    {{ get_context_fields(
      enabled=var('snowplow__enable_media_player_v1', false),
      context='unstruct_event_com_snowplowanalytics_snowplow_media_player_event_1',
      prefix='media_player_event_',
      fields=[
        {'field':'label', 'dtype':'string'},
        {'field':'type', 'dtype':'string'},
      ]) }}

    {{ get_context_fields(
      enabled=var('snowplow__enable_youtube', false),
      context='contexts_com_youtube_youtube_1',
      prefix='youtube_',
      fields=[
        {'field':'playerId', 'dtype':'string'},
        {'field':'url', 'dtype':'string'},
        {'field':'playbackQuality', 'dtype':'string'},
      ]) }}

    {{ get_context_fields(
      enabled=var('snowplow__enable_whatwg_media', false),
      context='contexts_org_whatwg_media_element_1',
      prefix='html5_media_element_',
      fields=[
        {'field':'htmlId', 'dtype':'string'},
        {'field':'currentSrc', 'dtype':'string'},
        {'field':'mediaType', 'dtype':'string'},
      ]) }}

    {{ get_context_fields(
      enabled=var('snowplow__enable_whatwg_video', false),
      context='contexts_org_whatwg_video_element_1',
      prefix='html5_video_element_',
      fields=[
        {'field':'videoWidth', 'dtype':'integer'},
        {'field':'videoHeight', 'dtype':'integer'},
      ]) }}

  from base_query ev

  where
    {{ snowplow_media_player.event_name_filter(var("snowplow__media_event_names", "['media_player_event']")) }}
)

, combined_fields as (
  select
    p.*
    -- combined web and mobile properties
    , coalesce(p.web_page__id, p.mobile_screen__id) as page_view_id
    , coalesce(p.domain_sessionid, p.mobile_session__session_id) as original_session_identifier

    --combined media properties
    , coalesce(p.media_player_v2__label, p.media_player_event__label) as media_label
    , round(coalesce(p.media_player_v2__duration, p.media_player_v1__duration)) as duration_secs
    , coalesce(p.media_player_v2__current_time, p.media_player_v1__current_time) as player_current_time
    , coalesce(p.media_player_v2__playback_rate, p.media_player_v1__playback_rate, 1.0) as playback_rate
    , coalesce(p.media_player_v2__muted, p.media_player_v1__muted) as is_muted
    , cast({{ percent_progress_field() }} as {{ type_int() }}) as percent_progress
    , coalesce(p.youtube__player_id, p.html5_media_element__html_id) as player_id
    , {{ media_player_type_field() }} as media_player_type
    , coalesce(p.youtube__url, p.html5_media_element__current_src) as source_url
    , {{ media_type_field() }} as media_type
    , {{ playback_quality_field() }} as playback_quality
    , {{ media_event_type_field() }} as event_type

  from prep p
)

select
  coalesce(
    cf.media_session__media_session_id,
    {{ dbt_utils.generate_surrogate_key(['cf.page_view_id', 'cf.player_id', 'cf.media_label', 'cf.media_type', 'cf.media_player_type']) }}
  ) as play_id
  , {{ dbt_utils.generate_surrogate_key(['cf.player_id', 'cf.media_label', 'cf.media_type', 'cf.media_player_type']) }} as media_identifier
  , cf.*
  , coalesce(
    cast(round(piv.weight_rate * cf.duration_secs / 100) as {{ type_int() }}),
    0
  ) as play_time_secs
  , coalesce(
    cast(
      round(
        case
          when cf.is_muted = true then piv.weight_rate * cf.duration_secs / 100
          else 0
        end
      ) as {{ type_int() }}
    ),
    0
  ) as play_time_muted_secs
  , cf.derived_tstamp as start_tstamp
  , dense_rank() over (partition by cf.session_identifier order by cf.derived_tstamp) AS event_in_session_index

from combined_fields as cf

left join {{ ref("snowplow_media_player_pivot_base") }} piv
  on cf.percent_progress = piv.percent_progress
