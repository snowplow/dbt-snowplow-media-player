{#
Copyright (c) 2022-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{% macro media_player_type_field(v2_player_type, youtube_player_id, media_player_id) %}
    coalesce(
      {% if var("snowplow__enable_media_player_v2") -%}
        {{ field(
          v2_player_type,
          col_prefix='contexts_com_snowplowanalytics_snowplow_media_player_2'
        ) }}
      {%- else -%}
        {% if v2_player_type is string and target.type not in ['postgres', 'redshift'] -%}
          {{ v2_player_type }}
        {% elif target.type not in ['postgres', 'redshift'] %}
          cast(null as {{ v2_player_type.get('dtype', 'string') }})
        {%- else -%}
          null
        {% endif %}
      {%- endif %},
      {% if var("snowplow__enable_youtube") and var("snowplow__enable_whatwg_media") -%}
        case
          when {{ field(
            youtube_player_id,
            col_prefix='contexts_com_youtube_youtube_1'
          ) }} is not null then 'com.youtube-youtube'
          when {{ field(
            media_player_id,
            col_prefix='contexts_org_whatwg_media_element_1'
          ) }} is not null then 'org.whatwg-media_element'
          else 'unknown'
        end
      {%- elif var("snowplow__enable_youtube") -%}
        'com.youtube-youtube'
      {% elif var("snowplow__enable_whatwg_media") -%}
        'org.whatwg-media_element'
      {%- else -%}
        {% if youtube_player_id is string and target.type not in ['postgres', 'redshift'] -%}
          {{ youtube_player_id }}
        {% elif target.type not in ['postgres', 'redshift'] %}
          cast(null as {{ youtube_player_id.get('dtype', 'string') }})
        {%- else -%}
          null
        {% endif %}
      {% endif %}
    )
{% endmacro %}
