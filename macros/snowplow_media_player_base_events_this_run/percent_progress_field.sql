{#
Copyright (c) 2022-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{% macro percent_progress_field() %}

  {%- set v2_percent_progress -%}
    round(media_player_v2__current_time / media_player_v2__duration * 100)
  {%- endset -%}

  coalesce(
    {% if var("snowplow__enable_media_player_v1") -%}
      case
        when media_player_event__type = 'ended' then 100
        else media_player_v1__percent_progress
      end,
    {%- endif %}

    {% if var("snowplow__enable_media_player_v2") -%}
      case
        when event_name = 'end_event' then 100
        when event_name = 'percent_progress_event' and coalesce(media_player_v2__duration, 0) > 0
          then (
            case
              {% for element in get_percentage_boundaries(var("snowplow__percent_progress_boundaries"))|sort|reverse %}
                when {{ v2_percent_progress }} >= {{ element }} then {{ element }}
              {% endfor %}
            end
          )
      end,
    {%- endif %}
    cast(null as {{ type_numeric() }})
  )

{%- endmacro %}
