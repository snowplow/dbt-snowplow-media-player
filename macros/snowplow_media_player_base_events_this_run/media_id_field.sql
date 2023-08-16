{% macro media_id_field(v2_player_label, youtube_player_id, media_player_id) %}
    coalesce(
      {% if var("snowplow__enable_youtube") -%}
        {{ field(
          youtube_player_id,
          col_prefix='contexts_com_youtube_youtube_1'
        ) }}
      {%- else -%}
        null
      {%- endif %},
      {% if var("snowplow__enable_whatwg_media") -%}
        {{ field(
          media_player_id,
          col_prefix='contexts_org_whatwg_media_element_1'
        ) }}
      {%- else -%}
        null
      {%- endif %},
      {% if var("snowplow__enable_media_player_v2") -%}
        {{ dbt_utils.generate_surrogate_key([
          field(
            v2_player_label,
            col_prefix='contexts_com_snowplowanalytics_snowplow_media_player_2'
          )
        ]) }}
      {%- else -%}
        null
      {%- endif %}
    )
{% endmacro %}
