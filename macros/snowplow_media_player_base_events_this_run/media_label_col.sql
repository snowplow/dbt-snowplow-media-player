{% macro media_label_col(v1_event_label, v2_player_label) %}
    coalesce(
      {% if var("snowplow__enable_media_player_v1") %}{{ v1_event_label }},{% endif %}
      {% if var("snowplow__enable_media_player_v2") %}{{ v2_player_label }},{% endif %}
      null
    ) as media_label
{% endmacro %}
