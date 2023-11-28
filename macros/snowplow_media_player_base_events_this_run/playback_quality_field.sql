{#
Copyright (c) 2022-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{% macro playback_quality_field() -%}
  coalesce(
    media_player_v2__quality,
    youtube__playback_quality,
    {% if var("snowplow__enable_whatwg_media") and var("snowplow__enable_whatwg_video") -%}
      html5_video_element__video_width || 'x' || html5_video_element__video_width,
    {% endif -%}
    'N/A'
  )
{%- endmacro %}
