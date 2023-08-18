{% macro media_type_col(v2_media_type, media_media_type) %}
  coalesce(
    {% if var("snowplow__enable_media_player_v2") %}
      {{ property_col(
        v2_media_type,
        col_prefix='contexts_com_snowplowanalytics_snowplow_media_player_2'
      ) }},
    {% endif %}
    {% if var("snowplow__enable_whatwg_media") %}
      case when {{ property_col(
        media_media_type,
        col_prefix='contexts_org_whatwg_media_element_1'
      ) }} = 'audio' then 'audio' else 'video' end,
    {% elif var("snowplow__enable_youtube") %}
      'video',
    {% endif %}
    null
  ) as media_type
{% endmacro %}
