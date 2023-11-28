{#
Copyright (c) 2022-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{% macro config_check() %}
  {{ return(adapter.dispatch('config_check', 'snowplow_media_player')()) }}
{% endmacro %}

{% macro default__config_check() -%}

  {% if not var('snowplow__enable_web_events') and not var('snowplow__enable_mobile_events') %}
    {{ exceptions.raise_compiler_error(
      "Snowplow Error: No platform to process. Please set at least one of the variables `snowplow__enable_web_events` or `snowplow__enable_mobile_events` to true."
    ) }}
  {% endif -%}

  {% if var("snowplow__enable_whatwg_media") is false and var("snowplow__enable_whatwg_video") %}
    {{ exceptions.raise_compiler_error(
      "Snowplow Error: Variable: `snowplow__enable_whatwg_video` is enabled but variable: `snowplow__enable_whatwg_media` is not, both need to be enabled for modelling html5 video tracking data."
      ) }}
  {% elif not var("snowplow__enable_media_player_v1") and not var("snowplow__enable_media_player_v2") %}
    {{ exceptions.raise_compiler_error(
      "Snowplow Error: No media player context enabled. Please enable at least one media player context: `snowplow__enable_media_player_v1` or `snowplow__enable_media_player_v2`"
      ) }}
  {% elif not var("snowplow__enable_youtube") and not var("snowplow__enable_whatwg_media") and not var("snowplow__enable_media_player_v2") %}
    {{ exceptions.raise_compiler_error(
      "Snowplow Error: No media context enabled. Please enable as many of the following variables as required: `snowplow__enable_media_player_v2`, `snowplow__enable_youtube`, `snowplow__enable_whatwg_media`, `snowplow__enable_whatwg_video`"
      ) }}
  {% endif %}

{%- endmacro %}
