{#
Copyright (c) 2022-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{% macro dtype_to_type(dtype) -%}
    {{ return(adapter.dispatch('dtype_to_type', 'snowplow_media_player')(dtype)) }}
{%- endmacro %}

{% macro default__dtype_to_type(dtype) -%}

  {%- if 'string' in dtype -%}
    {{ type_string() }}

  {%- elif 'integer' in dtype -%}
    {{ type_int() }}

  {%- elif 'number' in dtype -%}
    {{ type_numeric() }}

  {%- elif 'float' in dtype -%}
    {{ type_float() }}

  {%- elif 'boolean' in dtype -%}
    {{ type_boolean() }}

  {%- else -%}
    {{ exceptions.raise_compiler_error(dtype ~ ' dtype is not supported, please use data type specified in schema') }}

  {%- endif -%}

{%- endmacro %}
