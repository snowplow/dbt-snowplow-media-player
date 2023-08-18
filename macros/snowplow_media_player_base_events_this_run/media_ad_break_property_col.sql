{% macro media_ad_break_property_col(property) %}
    coalesce(
      {% if var("snowplow__enable_media_ad_break") %}{{ property_col(
        property,
        col_prefix='contexts_com_snowplowanalytics_snowplow_media_ad_break_1'
      ) }},{% endif %}
      null
    )
{% endmacro %}
