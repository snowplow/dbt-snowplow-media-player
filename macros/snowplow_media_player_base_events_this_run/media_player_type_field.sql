{% macro media_player_type_field(v2_player_type, youtube_player_id, media_player_id) %}
    coalesce(
      {% if var("snowplow__enable_media_player_v2") -%}
        {{ field(
          v2_player_type,
          col_prefix='contexts_com_snowplowanalytics_snowplow_media_player_2'
        ) }}
      {%- else -%}
        null
      {%- endif %},
      {% if var("snowplow__enable_youtube") and var("snowplow__enable_whatwg_media") -%}
        case
          when {{ field(
            youtube_player_id,
            col_prefix='contexts_com_youtube_youtube_1'
          ) }} is not null then 'com.youtube-youtube'
          when {{ field(
            media_player_id,
            col_prefix='contexts_org_whatwg_media_element_1'
          ) }} is not null then 'org.whatwg-media_element'
          else 'unknown'
        end
      {%- elif var("snowplow__enable_youtube") -%}
        'com.youtube-youtube'
      {% elif var("snowplow__enable_whatwg_media") -%}
        'org.whatwg-media_element'
      {%- else -%}
        null
      {% endif %}
    )
{% endmacro %}
