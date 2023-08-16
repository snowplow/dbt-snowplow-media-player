{% macro media_ad_property_col(property) %}
    coalesce(
      {% if var("snowplow__enable_media_ad") %}{{ property }},{% endif %}
      null
    )
{% endmacro %}
