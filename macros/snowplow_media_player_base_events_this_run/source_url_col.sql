{% macro source_url_col(youtube_url, media_current_src) %}
  coalesce(
    {% if var("snowplow__enable_youtube") %}
      {{ property_col(
        youtube_url,
        col_prefix='contexts_com_youtube_youtube_1'
      ) }},
    {% endif %}
    {% if var("snowplow__enable_whatwg_media") %}
      {{ property_col(
        media_current_src,
        col_prefix='contexts_org_whatwg_media_element_1'
      ) }},
    {% endif %}
    null
  ) as source_url
{% endmacro %}
