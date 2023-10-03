{#
Copyright (c) 2022-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{% macro media_type_field(v2_media_type, media_media_type) %}
  coalesce(
    {% if var("snowplow__enable_media_player_v2") -%}
      {{ field(
        v2_media_type,
        col_prefix='contexts_com_snowplowanalytics_snowplow_media_player_2'
      ) }}
    {%- else -%}
      {% if v2_media_type is string and target.type not in ['postgres', 'redshift'] -%}
          {{ v2_media_type }}
        {% elif target.type not in ['postgres', 'redshift'] %}
          cast(null as {{ v2_media_type.get('dtype', 'string') }})
        {%- else -%}
          null
        {% endif %}
    {%- endif %},
    {% if var("snowplow__enable_whatwg_media") -%}
      case when {{ field(
        media_media_type,
        col_prefix='contexts_org_whatwg_media_element_1'
      ) }} = 'audio' then 'audio' else 'video' end
    {%- elif var("snowplow__enable_youtube") -%}
      'video'
    {%- else -%}
      {% if media_media_type is string and target.type not in ['postgres', 'redshift'] -%}
          {{ media_media_type }}
        {% elif target.type not in ['postgres', 'redshift'] %}
          cast(null as {{ media_media_type.get('dtype', 'string') }})
        {%- else -%}
          null
        {% endif %}
    {%- endif %}
  )
{% endmacro %}
