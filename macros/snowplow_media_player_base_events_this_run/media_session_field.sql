{% macro media_session_field(property) %}
    {% if var("snowplow__enable_media_session") -%}
      {{ field(
        property,
        col_prefix='contexts_com_snowplowanalytics_snowplow_media_session_1'
      ) }}
    {%- else -%}
      null
    {%- endif %}
{% endmacro %}
