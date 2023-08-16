{% macro media_ad_quartile_event_property_col(property) %}
    coalesce(
      {% if var("snowplow__enable_ad_quartile_event") %}{{ property }},{% endif %}
      null
    )
{% endmacro %}
