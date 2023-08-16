{% macro media_ad_quartile_event_field(property) %}
    {%- if var("snowplow__enable_ad_quartile_event") -%}
      {{ field(
        property,
        col_prefix='unstruct_event_com_snowplowanalytics_snowplow_media_ad_quartile_event_1'
      ) }}
    {%- else -%}
      null
    {%- endif -%}
{% endmacro %}
