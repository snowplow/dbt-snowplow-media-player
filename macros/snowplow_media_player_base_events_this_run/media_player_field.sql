{#
Copyright (c) 2022-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{% macro media_player_field(v1, v2, default='null') %}
    coalesce(
      {% if var("snowplow__enable_media_player_v2") -%}
        {{ field(
          v2,
          col_prefix='contexts_com_snowplowanalytics_snowplow_media_player_2'
        ) }}
      {%- else -%}
        {% if v2 is string and target.type not in ['postgres', 'redshift'] -%}
          {{ v2 }}
        {% elif target.type not in ['postgres', 'redshift'] %}
          cast(null as {{ v2.get('dtype', 'string') }})
        {%- else -%}
          null
        {% endif %}
      {%- endif %},
      {%- if v1 is not none and var("snowplow__enable_media_player_v1") -%}
        {{ field(
          v1,
          col_prefix='contexts_com_snowplowanalytics_snowplow_media_player_1'
        ) }}
      {%- else -%}
        {% if v1 is string and target.type not in ['postgres', 'redshift'] -%}
          {{ v1 }}
        {% elif target.type not in ['postgres', 'redshift'] %}
          cast(null as {{ v1.get('dtype', 'string') }})
        {%- else -%}
          null
        {% endif %}
      {%- endif %},
      {{ default }}
    )
{% endmacro %}
