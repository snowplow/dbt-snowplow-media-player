{% macro media_session_property_col(property) %}
    coalesce(
      {% if var("snowplow__enable_media_session") %}{{ property_col(
        property,
        col_prefix='contexts_com_snowplowanalytics_snowplow_media_session_1'
      ) }},{% endif %}
      null
    )
{% endmacro %}
