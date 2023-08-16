{% macro media_ad_break_field(property) %}
    {%- if var("snowplow__enable_media_ad_break") -%}
      {{ field(
        property,
        col_prefix='contexts_com_snowplowanalytics_snowplow_media_ad_break_1'
      ) }}
    {%- else -%}
      null
    {%- endif -%}
{% endmacro %}
