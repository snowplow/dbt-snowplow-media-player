{% macro web_or_mobile_col(web_property, mobile_property) %}
    coalesce(
      {% if var("snowplow__enable_web_events") %}{{ web_property }},{% endif %}
      {% if var("snowplow__enable_mobile_events") %}{{ mobile_property }},{% endif %}
      null
    )
{% endmacro %}
