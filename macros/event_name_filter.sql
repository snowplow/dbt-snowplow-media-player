{# Filters on event_name if provided #}
{% macro event_name_filter(event_names) %}

  (
  {%- if event_names|length -%}
    lower(event_name) in ('{{ event_names|map("lower")|join("','") }}') --filter on event_name if provided
  {%- else -%}
    true
  {%- endif %}
  or lower(event_vendor) = 'com.snowplowanalytics.snowplow.media'
  )

{% endmacro %}
