{% macro web_or_mobile_field(web, mobile) %}
    coalesce(
      {% if var("snowplow__enable_web_events") -%}
        {{ field(web) }}
      {%- else -%}
        null
      {%- endif %},
      {% if var("snowplow__enable_mobile_events") -%}
        {{ field(mobile) }}
      {%- else -%}
        null
      {%- endif %}
    )
{% endmacro %}
