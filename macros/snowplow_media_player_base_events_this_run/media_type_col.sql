{% macro media_type_col(media_media_type) %}
  {% if var("snowplow__enable_whatwg_media") %}
    case when {{ media_media_type }} = 'audio' then 'audio' else 'video' end
  {% elif var("snowplow__enable_youtube") %}
    'video'
  {% endif %} as media_type
{% endmacro %}
