{#
Copyright (c) 2022-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{% macro media_event_type_field() -%}
  coalesce(
    -- for v1 media schemas, use the type property in media_player_event
    media_player_event__type,
    -- for v2 media schemas, the type is the event name, remove underscores to match v1 event types
    case
        when right(event_name, 6) = '_event'
        then replace(
            left(event_name, length(event_name) - 6),
            '_',
            ''
        )
        else cast(null as {{ type_string() }})
    end
  )
{%- endmacro %}
