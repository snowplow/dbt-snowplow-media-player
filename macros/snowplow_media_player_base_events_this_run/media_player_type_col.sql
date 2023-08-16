{% macro media_player_type_col(v2_player_type, youtube_player_id, media_player_id) %}
    coalesce(
      {% if var("snowplow__enable_media_player_v2") %}{{ v2_player_type }},{% endif %}
      {% if var("snowplow__enable_youtube") and var("snowplow__enable_whatwg_media") %}
        case
          when {{ youtube_player_id }} is not null then 'com.youtube-youtube'
          when {{ media_player_id }} is not null then 'org.whatwg-media_element'
          else 'unknown'
        end,
      {% elif var("snowplow__enable_youtube") %}
        'com.youtube-youtube',
      {% elif var("snowplow__enable_whatwg_media") %}
        'org.whatwg-media_element',
      {% endif %}
      null
    ) as media_player_type
{% endmacro %}
