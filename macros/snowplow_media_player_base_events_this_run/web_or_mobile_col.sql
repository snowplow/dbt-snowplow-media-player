{% macro web_or_mobile_col(web_property, mobile_property) %}
    coalesce(
      {% if var("snowplow__enable_web_events") %}{{ property_col(web_property) }},{% endif %}
      {% if var("snowplow__enable_mobile_events") %}{{ property_col(mobile_property) }},{% endif %}
      null
    )
{% endmacro %}
