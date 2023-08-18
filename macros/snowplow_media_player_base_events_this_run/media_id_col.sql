{% macro media_id_col(v2_player_label, youtube_player_id, media_player_id) %}
    coalesce(
      {% if var("snowplow__enable_youtube") %}{{ property_col(
        youtube_player_id,
        col_prefix='contexts_com_youtube_youtube_1'
      ) }},{% endif %}
      {% if var("snowplow__enable_whatwg_media") %}{{ property_col(
        media_player_id,
        col_prefix='contexts_org_whatwg_media_element_1'
      ) }},{% endif %}
      {% if var("snowplow__enable_media_player_v2") %}{{ dbt_utils.generate_surrogate_key([
        property_col(
          v2_player_label,
          col_prefix='contexts_com_snowplowanalytics_snowplow_media_player_2'
        )
      ]) }},{% endif %}
      null
    ) as media_id
{% endmacro %}
