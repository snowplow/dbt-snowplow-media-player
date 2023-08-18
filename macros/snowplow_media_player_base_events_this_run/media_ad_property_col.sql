{% macro media_ad_property_col(property) %}
    coalesce(
      {% if var("snowplow__enable_media_ad") %}{{ property_col(
        property,
        col_prefix='contexts_com_snowplowanalytics_snowplow_media_ad_1'
      ) }},{% endif %}
      null
    )
{% endmacro %}
