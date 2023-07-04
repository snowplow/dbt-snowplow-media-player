{% macro source_url_col(youtube_url, media_current_src) %}
  {% if var("snowplow__enable_youtube") and var("snowplow__enable_whatwg_media") %}
    coalesce(
      {{ youtube_url }},
      {{ media_current_src }}
    )
  {% elif var("snowplow__enable_youtube") %}
    {{ youtube_url }}
  {% elif var("snowplow__enable_whatwg_media") %}
    {{ media_current_src }}
  {% endif %} as source_url
{% endmacro %}
