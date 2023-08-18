{% macro playback_quality_col(v2_quality, youtube_quality, video_width, video_height) %}
  coalesce(
    {% if var("snowplow__enable_media_player_v2") %}{{ property_col(
      v2_quality,
      col_prefix='contexts_com_snowplowanalytics_snowplow_media_player_2'
    ) }},{% endif %}
    {% if var("snowplow__enable_youtube") %}{{ property_col(
      youtube_quality,
      col_prefix='contexts_com_youtube_youtube_1'
    ) }},{% endif %}
    {% if var("snowplow__enable_whatwg_media") and var("snowplow__enable_whatwg_video") %}
      {{ property_col(
        video_width,
        col_prefix='contexts_org_whatwg_video_element_1'
      ) }}||'x'||{{ property_col(
        video_height,
        col_prefix='contexts_org_whatwg_video_element_1'
      ) }},
    {% endif %}
    'N/A'
   ) as playback_quality
{% endmacro %}
