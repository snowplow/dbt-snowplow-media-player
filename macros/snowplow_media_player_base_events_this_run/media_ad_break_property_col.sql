{% macro media_ad_break_property_col(property) %}
    coalesce(
      {% if var("snowplow__enable_media_ad_break") %}{{ property }},{% endif %}
      null
    )
{% endmacro %}
