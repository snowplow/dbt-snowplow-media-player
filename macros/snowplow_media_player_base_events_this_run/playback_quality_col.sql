{% macro playback_quality_col(v2_quality, youtube_quality, video_width, video_height) %}
  coalesce(
    {% if var("snowplow__enable_media_player_v2") %}cast({{ v2_quality }} as text),{% endif %}
    {% if var("snowplow__enable_youtube") %}{{ youtube_quality }},{% endif %}
    {% if var("snowplow__enable_whatwg_media") and var("snowplow__enable_whatwg_video") %}
      {{ video_width }}||'x'||{{ video_height }},
    {% endif %}
    'N/A'
   ) as playback_quality
{% endmacro %}
