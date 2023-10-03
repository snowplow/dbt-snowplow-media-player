{#
Copyright (c) 2022-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{% macro playback_quality_field(v2_quality, youtube_quality, video_width, video_height) %}
  coalesce(
    {% if var("snowplow__enable_media_player_v2") -%}
      {{ field(
        v2_quality,
        col_prefix='contexts_com_snowplowanalytics_snowplow_media_player_2'
      ) }}
    {%- else -%}
      {% if v2_quality is string and target.type not in ['postgres', 'redshift'] -%}
          {{ v2_quality }}
        {% elif target.type not in ['postgres', 'redshift'] %}
          cast(null as {{ v2_quality.get('dtype', 'string') }})
        {%- else -%}
          null
        {% endif %}
    {%- endif %},
    {% if var("snowplow__enable_youtube") -%}
      {{ field(
        youtube_quality,
        col_prefix='contexts_com_youtube_youtube_1'
      ) }}
    {%- else -%}
      {% if youtube_quality is string and target.type not in ['postgres', 'redshift'] -%}
          {{ youtube_quality }}
        {% elif target.type not in ['postgres', 'redshift'] %}
          cast(null as {{ youtube_quality.get('dtype', 'string') }})
        {%- else -%}
          null
        {% endif %}
    {%- endif %},
    {% if var("snowplow__enable_whatwg_media") and var("snowplow__enable_whatwg_video") -%}
      {{ field(
        video_width,
        col_prefix='contexts_org_whatwg_video_element_1'
      ) }}||'x'||{{ field(
        video_height,
        col_prefix='contexts_org_whatwg_video_element_1'
      ) }}
    {%- else -%}
      {% if video_width is string and target.type not in ['postgres', 'redshift'] -%}
          {{ video_width }}
        {% elif target.type not in ['postgres', 'redshift'] %}
          cast(null as {{ video_width.get('dtype', 'string') }})
        {%- else -%}
          null
        {% endif %}
    {% endif %},
    'N/A'
   )
{% endmacro %}
