{#
Copyright (c) 2022-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{% macro field_alias(field, prefix=None) -%}
    {{ return(adapter.dispatch('field_alias', 'snowplow_media_player')(field, prefix)) }}
{%- endmacro %}

{% macro default__field_alias(field, prefix) -%}

  {% set alias = (prefix~'_' if prefix else '')~(snakeify_case(field.get('field'))) -%}

  {{ alias }}

{%- endmacro %}
