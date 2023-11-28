{#
Copyright (c) 2022-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{% macro session_identifiers() %}
  {{ return(adapter.dispatch('session_identifiers', 'snowplow_media_player')()) }}
{% endmacro %}


{% macro default__session_identifiers() %}
  {% do exceptions.raise_compiler_error("This macro is not supported for adapter " ~ target.type) %}
{% endmacro %}


{% macro databricks__session_identifiers() %}

  {% if var('snowplow__session_identifiers') %}
    {{ return(var('snowplow__session_identifiers')) }}

  {% else %}
    {% set identifiers = [] %}

    {% if var('snowplow__enable_media_session') %}
      {% do identifiers.append({'schema': 'contexts_com_snowplowanalytics_snowplow_media_session_1', 'field': 'media_session_id', 'prefix': 'media_session_'}) %}
    {% endif %}

    {% if var('snowplow__enable_mobile_events') %}
      {% do identifiers.append({'schema': 'contexts_com_snowplowanalytics_mobile_screen_1', 'field': 'id', 'prefix': 'mobile_screen_'}) %}
    {% endif %}

    {% if var('snowplow__enable_web_events') %}
      {% do identifiers.append({'schema': 'contexts_com_snowplowanalytics_snowplow_web_page_1', 'field': 'id', 'prefix': 'web_page_'}) %}
    {% endif %}

  {% endif %}

  {{ return(identifiers) }}

{% endmacro %}


{% macro snowflake__session_identifiers() %}

  {% if var('snowplow__session_identifiers') %}
    {{ return(var('snowplow__session_identifiers')) }}

  {% else %}
    {% set identifiers = [] %}

    {% if var('snowplow__enable_media_session') %}
      {% do identifiers.append({'schema': 'contexts_com_snowplowanalytics_snowplow_media_session_1', 'field': 'mediaSessionId', 'prefix': 'media_session_'}) %}
    {% endif %}

    {% if var('snowplow__enable_mobile_events') %}
      {% do identifiers.append({'schema': 'contexts_com_snowplowanalytics_mobile_screen_1', 'field': 'id', 'prefix': 'mobile_screen_'}) %}
    {% endif %}

    {% if var('snowplow__enable_web_events') %}
      {% do identifiers.append({'schema': 'contexts_com_snowplowanalytics_snowplow_web_page_1', 'field': 'id', 'prefix': 'web_page_'}) %}
    {% endif %}

  {% endif %}

  {{ return(identifiers) }}

{% endmacro %}


{% macro bigquery__session_identifiers() %}

  {% if var('snowplow__session_identifiers') %}
    {{ return(var('snowplow__session_identifiers')) }}

  {% else %}
    {% set identifiers = [] %}

    {% if var('snowplow__enable_media_session') %}
      {% do identifiers.append({'schema': 'contexts_com_snowplowanalytics_snowplow_media_session_1_*', 'field': 'media_session_id', 'prefix': 'media_session_'}) %}
    {% endif %}

    {% if var('snowplow__enable_mobile_events') %}
      {% do identifiers.append({'schema': 'contexts_com_snowplowanalytics_mobile_screen_1_*', 'field': 'id', 'prefix': 'mobile_screen_'}) %}
    {% endif %}

    {% if var('snowplow__enable_web_events') %}
      {% do identifiers.append({'schema': 'contexts_com_snowplowanalytics_snowplow_web_page_1_*', 'field': 'id', 'prefix': 'web_page_'}) %}
    {% endif %}
  {% endif %}

  {{ return(identifiers) }}

{% endmacro %}


{% macro postgres__session_identifiers() %}

  {% if var('snowplow__session_identifiers') %}
    {{ return(var('snowplow__session_identifiers')) }}

  {% else %}
    {% set identifiers = [] %}

    {% if var('snowplow__enable_media_session') %}
      {% do identifiers.append({'schema': 'com_snowplowanalytics_snowplow_media_session_1', 'field': 'media_session_id', 'prefix': 'media_session_'}) %}
    {% endif %}

    {% if var('snowplow__enable_mobile_events') %}
      {% do identifiers.append({'schema': 'com_snowplowanalytics_mobile_screen_1', 'field': 'id', 'prefix': 'mobile_screen_'}) %}
    {% endif %}

    {% if var('snowplow__enable_web_events') %}
      {% do identifiers.append({'schema': 'com_snowplowanalytics_snowplow_web_page_1', 'field': 'id', 'prefix': 'web_page_'}) %}
    {% endif %}

  {% endif %}

  {{ return(identifiers) }}

{% endmacro %}


{% macro user_identifiers() %}
  {{ return(adapter.dispatch('user_identifiers', 'snowplow_media_player')()) }}
{% endmacro %}


{% macro default__user_identifiers() %}
  {% do exceptions.raise_compiler_error("This macro is not supported for adapter " ~ target.type) %}
{% endmacro %}


{% macro databricks__user_identifiers() %}

  {% if var('snowplow__user_identifiers') %}
    {{ return(var('snowplow__user_identifiers')) }}

  {% else %}
    {% set identifiers = [] %}

    {% if var('snowplow__enable_web_events') %}
      {% do identifiers.append( {'schema': 'atomic', 'field': 'domain_userid', 'prefix': 'user_'}) %}
    {% endif %}

    {% if var('snowplow__enable_mobile_events') %}
      {% do identifiers.append({'schema': 'contexts_com_snowplowanalytics_snowplow_client_session_1', 'field': 'user_id', 'prefix': 'user_'}) %}
    {% endif %}

    {{ return(identifiers) }}

  {% endif %}

{% endmacro %}


{% macro snowflake__user_identifiers() %}

  {% if var('snowplow__user_identifiers') %}
    {{ return(var('snowplow__user_identifiers')) }}

  {% else %}
    {% set identifiers = [] %}

    {% if var('snowplow__enable_web_events') %}
      {% do identifiers.append( {'schema': 'atomic', 'field': 'domain_userid', 'prefix': 'user_'}) %}
    {% endif %}

    {% if var('snowplow__enable_mobile_events') %}
      {% do identifiers.append({'schema': 'contexts_com_snowplowanalytics_snowplow_client_session_1', 'field': 'userId', 'prefix': 'user_'}) %}
    {% endif %}

    {{ return(identifiers) }}

  {% endif %}

{% endmacro %}

{% macro bigquery__user_identifiers() %}

  {% if var('snowplow__user_identifiers') %}
    {{ return(var('snowplow__user_identifiers')) }}

  {% else %}
    {% set identifiers = [] %}

    {% if var('snowplow__enable_web_events') %}
      {% do identifiers.append( {'schema': 'atomic', 'field': 'domain_userid', 'prefix': 'user_'}) %}
    {% endif %}

    {% if var('snowplow__enable_mobile_events') %}
      {% do identifiers.append({'schema': 'contexts_com_snowplowanalytics_snowplow_client_session_1_*', 'field': 'user_id', 'prefix': 'user_'}) %}
    {% endif %}

    {{ return(identifiers) }}

  {% endif %}

{% endmacro %}

{% macro postgres__user_identifiers() %}

  {% if var('snowplow__user_identifiers') %}
    {{ return(var('snowplow__user_identifiers')) }}

  {% else %}
    {% set identifiers = [] %}

    {% if var('snowplow__enable_web_events') %}
      {% do identifiers.append( {'schema': 'atomic', 'field': 'domain_userid', 'prefix': 'user_'}) %}
    {% endif %}

    {% if var('snowplow__enable_mobile_events') %}
      {% do identifiers.append({'schema': 'com_snowplowanalytics_snowplow_client_session_1', 'field': 'user_id', 'prefix': 'user_'}) %}
    {% endif %}

    {{ return(identifiers) }}

  {% endif %}

{% endmacro %}
