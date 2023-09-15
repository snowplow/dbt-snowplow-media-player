{#
Copyright (c) 2022-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{% macro field(property, col_prefix=None, field=None) -%}
    {{ return(adapter.dispatch('field')(property, col_prefix, field)) }}
{%- endmacro %}

{% macro bigquery__field(property, col_prefix, field) -%}
    {% if property is string -%}
    {{ property }}
    {%- else -%}
    {{ snowplow_utils.get_optional_fields(
        enabled=true,
        fields=[{'field': property.get('field', field), 'dtype': property.get('dtype', 'string') }],
        col_prefix=property.get('col_prefix', col_prefix),
        relation=source('atomic', 'events'),
        relation_alias=property.get('relation_alias', 'a'),
        include_field_alias=false
    ) }}
    {%- endif %}
{%- endmacro %}

{% macro default__field(property, col_prefix, field) -%}
    {% if property is string -%}
    {{ property }}
    {%- else -%}
    {{ snowplow_utils.get_field(
        column_name=property.get('col_prefix', col_prefix),
        field_name=property.get('field', field),
        table_alias=property.get('relation_alias', 'a'),
        type=property.get('dtype', 'string'),
        array_index='0' if 'contexts_' in property.get('col_prefix', col_prefix) else none
    ) }}
    {%- endif %}
{%- endmacro %}
