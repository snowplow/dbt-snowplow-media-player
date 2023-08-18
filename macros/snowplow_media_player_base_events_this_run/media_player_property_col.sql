{% macro media_player_property_col(v1_property, v2_property, default='null') %}
    coalesce(
      {% if var("snowplow__enable_media_player_v2") -%}
        {{ property_col(
          v2_property,
          col_prefix='contexts_com_snowplowanalytics_snowplow_media_player_2'
        ) }},
      {% endif %}
      {% if v1_property is not none and var("snowplow__enable_media_player_v1") -%}
        {{ property_col(
          v1_property,
          col_prefix='contexts_com_snowplowanalytics_snowplow_media_player_1'
        ) }},
      {% endif %}
      {{ default }}
    )
{% endmacro %}
