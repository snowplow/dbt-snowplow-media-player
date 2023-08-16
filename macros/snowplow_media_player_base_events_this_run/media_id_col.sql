{% macro media_id_col(v2_player_label, youtube_player_id, media_player_id) %}
    coalesce(
      {% if var("snowplow__enable_youtube") %}{{ youtube_player_id }},{% endif %}
      {% if var("snowplow__enable_whatwg_media") %}{{ media_player_id }},{% endif %}
      {% if var("snowplow__enable_media_player_v2") %}{{ dbt_utils.generate_surrogate_key([v2_player_label]) }},{% endif %}
      null
    ) as media_id
{% endmacro %}
