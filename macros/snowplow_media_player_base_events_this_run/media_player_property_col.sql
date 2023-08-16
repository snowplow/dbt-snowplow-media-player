{% macro media_player_property_col(v1_property, v2_property) %}
    coalesce(
      {% if var("snowplow__enable_media_player_v2") %}{{ v2_property }},{% endif %}
      {% if v1_property is not none and var("snowplow__enable_media_player_v1") %}{{ v1_property }},{% endif %}
      null
    )
{% endmacro %}
