{% macro media_type_col(v2_media_type, media_media_type) %}
  coalesce(
    {% if var("snowplow__enable_media_player_v2") %}
      {{ v2_media_type }},
    {% endif %}
    {% if var("snowplow__enable_whatwg_media") %}
      case when {{ media_media_type }} = 'audio' then 'audio' else 'video' end,
    {% elif var("snowplow__enable_youtube") %}
      'video',
    {% endif %}
    null
  ) as media_type
{% endmacro %}
