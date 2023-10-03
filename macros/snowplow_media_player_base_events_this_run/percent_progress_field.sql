{#
Copyright (c) 2022-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{% macro percent_progress_field(v1_percent_progress, v1_event_type, event_name, v2_current_time, v2_duration) %}
    {%- set v2_percent_progres -%}
      round({{ field(
        v2_current_time,
        col_prefix='contexts_com_snowplowanalytics_snowplow_media_player_2'
      ) }} / {{ field(
        v2_duration,
        col_prefix='contexts_com_snowplowanalytics_snowplow_media_player_2'
      ) }} * 100)
    {%- endset -%}
    coalesce(
      {% if var("snowplow__enable_media_player_v1") -%}
        case
          when {{ field(
            v1_event_type,
            col_prefix="unstruct_event_com_snowplowanalytics_snowplow_media_player_event_1",
            field='type'
          ) }} = 'ended'
          then 100
          else {{ field(
            v1_percent_progress,
            col_prefix='contexts_com_snowplowanalytics_snowplow_media_player_1'
          ) }}
        end
      {%- else -%}
        {% if v1_percent_progress is string and target.type not in ['postgres', 'redshift'] -%}
          {{ v1_percent_progress }}
        {% elif target.type not in ['postgres', 'redshift'] %}
          cast(null as {{ v1_percent_progress.get('dtype', 'string') }})
        {%- else -%}
          null
        {% endif %}
      {%- endif %},
      {% if var("snowplow__enable_media_player_v2") -%}
        case
            when {{ event_name }} = 'end_event'
            then 100
            when {{ event_name }} = 'percent_progress_event'
            and coalesce({{ field(
              v2_duration,
              col_prefix='contexts_com_snowplowanalytics_snowplow_media_player_2'
            ) }}, 0) > 0
            then (
              case
                {% for element in get_percentage_boundaries(var("snowplow__percent_progress_boundaries"))|sort|reverse %}
                when {{ v2_percent_progres }} >= {{ element }}
                then {{ element }}
                {% endfor %}
                else null
              end
            )

            else null
        end
      {%- else -%}
        {% if v2_duration is string and target.type not in ['postgres', 'redshift'] -%}
          {{ v2_duration }}
        {% elif target.type not in ['postgres', 'redshift'] %}
          cast(null as {{ v2_duration.get('dtype', 'string') }})
        {%- else -%}
          null
        {% endif %}
      {%- endif %}
    )
{% endmacro %}
