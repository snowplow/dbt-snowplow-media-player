{#
Copyright (c) 2022-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{% macro get_enabled_context_fields(fields, col_prefix, field_prefix) -%}
    {{ return(adapter.dispatch('get_enabled_context_fields')(fields, col_prefix, field_prefix)) }}
{%- endmacro %}


{% macro postgres__get_enabled_context_fields(fields, col_prefix, field_prefix) -%}
  {# For Postgres/Redshift enabled contexts are already joined via the base table macro, so no need to do anything here #}
{%- endmacro %}


{% macro bigquery__get_enabled_context_fields(fields, col_prefix, field_prefix) -%}
  {% for f in fields %}
    , {{ snowplow_utils.get_optional_fields(
          enabled=true,
          fields=[{'field': snakeify_case(f.get('field')), 'dtype': f.get('dtype') }],
          col_prefix=col_prefix,
          relation=source('atomic', 'events'),
          relation_alias='ev',
          include_field_alias=false
      ) }} as {{ field_alias(f, field_prefix) }}
  {%- endfor %}
{%- endmacro %}


{% macro snowflake__get_enabled_context_fields(fields, col_prefix, field_prefix) -%}
  {% for f in fields %}
    {% set type = dtype_to_type(f.get('dtype')) %}
    , {{ snowplow_utils.get_field(
          column_name=col_prefix,
          field_name=f.get('field'),
          table_alias='ev',
          type=type,
          array_index='0' if 'contexts_' in col_prefix else none
      ) }} as {{ field_alias(f, field_prefix) }}
  {%- endfor %}
{%- endmacro %}


{% macro spark__get_enabled_context_fields(fields, col_prefix, field_prefix) -%}
  {% for f in fields %}
    {% set type = dtype_to_type(f.get('dtype')) %}
    , {{ snowplow_utils.get_field(
          column_name=col_prefix,
          field_name=snakeify_case(f.get('field')),
          table_alias='ev',
          type=type,
          array_index='0' if 'contexts_' in col_prefix else none
      ) }} as {{ field_alias(f, field_prefix) }}
  {%- endfor %}
{%- endmacro %}
