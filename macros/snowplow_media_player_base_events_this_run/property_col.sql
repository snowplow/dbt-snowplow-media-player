{% macro property_col(property, col_prefix=None, field=None) %}
    {% if property is string -%}
        {{ property }}
    {% else -%}
        {{ snowplow_utils.get_optional_fields(
          enabled=true,
          fields=[{'field': property.get('field', field), 'dtype': property.get('dtype', 'string') }],
          col_prefix=property.get('col_prefix', col_prefix),
          relation=source('atomic', 'events'),
          relation_alias=property.get('relation_alias', 'a'),
          include_field_alias=false
        ) }}
    {% endif %}
{% endmacro %}
