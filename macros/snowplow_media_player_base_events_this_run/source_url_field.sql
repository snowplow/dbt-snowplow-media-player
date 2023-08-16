{% macro source_url_field(youtube_url, media_current_src) %}
  coalesce(
    {% if var("snowplow__enable_youtube") -%}
      {{ field(
        youtube_url,
        col_prefix='contexts_com_youtube_youtube_1'
      ) }}
    {%- else -%}
      null
    {%- endif %},
    {% if var("snowplow__enable_whatwg_media") -%}
      {{ field(
        media_current_src,
        col_prefix='contexts_org_whatwg_media_element_1'
      ) }}
    {%- else -%}
      null
    {%- endif %}
  )
{% endmacro %}
