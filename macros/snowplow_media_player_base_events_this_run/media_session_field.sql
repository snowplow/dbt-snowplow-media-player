{#
Copyright (c) 2022-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{% macro media_session_field(property) %}
    {% if var("snowplow__enable_media_session") -%}
      {{ field(
        property,
        col_prefix='contexts_com_snowplowanalytics_snowplow_media_session_1'
      ) }}
    {%- else -%}
      {% if property is string and target.type not in ['postgres', 'redshift'] -%}
          {{ property }}
        {% elif target.type not in ['postgres', 'redshift'] %}
          cast(null as {{ property.get('dtype', 'string') }})
        {%- else -%}
          null
        {% endif %}
    {%- endif %}
{% endmacro %}
