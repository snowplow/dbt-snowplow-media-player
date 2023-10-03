{#
Copyright (c) 2022-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{% macro web_or_mobile_field(web, mobile) %}
    coalesce(
      {% if var("snowplow__enable_web_events") -%}
        {{ field(web) }}
      {%- else -%}
        {% if web is string and target.type not in ['postgres', 'redshift'] -%}
          {{ web }}
        {% elif target.type not in ['postgres', 'redshift'] %}
          cast(null as {{ web.get('dtype', 'string') }})
        {%- else -%}
          null
        {% endif %}
      {%- endif %},
      {% if var("snowplow__enable_mobile_events") -%}
        {{ field(mobile) }}
      {%- else -%}
        {% if mobile is string and target.type not in ['postgres', 'redshift'] -%}
          {{ mobile }}
        {% elif target.type not in ['postgres', 'redshift'] %}
          cast(null as {{ mobile.get('dtype', 'string') }})
        {%- else -%}
          null
        {% endif %}
      {%- endif %}
    )
{% endmacro %}
