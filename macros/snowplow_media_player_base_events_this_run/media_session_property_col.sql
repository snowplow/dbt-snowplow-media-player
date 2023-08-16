{% macro media_session_property_col(property) %}
    coalesce(
      {% if var("snowplow__enable_media_session") %}{{ property }},{% endif %}
      null
    )
{% endmacro %}
