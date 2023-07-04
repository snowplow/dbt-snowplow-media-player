{% macro media_id_col(youtube_player_id, media_player_id) %}
    {% if var("snowplow__enable_youtube") and var("snowplow__enable_whatwg_media") %}
      coalesce({{ youtube_player_id }}, {{ media_player_id }})
    {% elif var("snowplow__enable_youtube") %}
      {{ youtube_player_id }}
    {% elif var("snowplow__enable_whatwg_media") %}
      {{ media_player_id }}
    {% endif %} as media_id
{% endmacro %}
