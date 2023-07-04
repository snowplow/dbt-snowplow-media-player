{% macro playback_quality_col(youtube_quality, video_width, video_height) %}
  {% if var("snowplow__enable_youtube") and var("snowplow__enable_whatwg_media") and var("snowplow__enable_whatwg_video") %}
    coalesce(
      {{ youtube_quality }},
      {{ video_width }}||'x'||{{ video_height }}
    )
  {% elif var("snowplow__enable_youtube") %}
    {{ youtube_quality }}
  {% elif var("snowplow__enable_whatwg_media") and var("snowplow__enable_whatwg_video") %}
    {{ video_width }}||'x'||{{ video_height }}
  {% else %}
    'N/A'
  {% endif %} as playback_quality
{% endmacro %}
