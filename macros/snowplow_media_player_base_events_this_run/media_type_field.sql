{#
Copyright (c) 2022-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{% macro media_type_field() -%}
  coalesce(
    media_player_v2__media_type
    {% if var("snowplow__enable_whatwg_media") -%}
      , case when html5_media_element__media_type = 'audio' then 'audio' else 'video' end
    {%- elif var("snowplow__enable_youtube") -%}
      , 'video'
    {%- endif %}
    , cast(null as {{ type_string() }})
  )
{%- endmacro %}
