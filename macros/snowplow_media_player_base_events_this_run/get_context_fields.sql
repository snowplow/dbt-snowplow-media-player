{#
Copyright (c) 2022-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{% macro get_context_fields(fields, enabled, context, prefix=None) %}
  {{ return(adapter.dispatch('get_context_fields', 'snowplow_media_player')(fields, enabled, context, prefix)) }}
{% endmacro %}

{% macro default__get_context_fields(fields, enabled, context, prefix) %}

  {%- if enabled -%}
    {{ get_enabled_context_fields(fields, context, prefix) }}
  {%- else -%}
    {% for f in fields %}
      , cast(null as {{ dtype_to_type(f.get('dtype')) }}) as {{ field_alias(f, prefix) }}
    {%- endfor %}

  {%- endif -%}

{% endmacro %}
