{% macro percent_progress_col(v1_percent_progress, v1_event_type, event_name, v2_current_time, v2_duration) %}
    coalesce(
      {% if var("snowplow__enable_media_player_v1") %}
        case
          when {{ v1_event_type }} = 'ended'
          then 100
          else {{ v1_percent_progress }}
        end,
      {% endif %}
      {% if var("snowplow__enable_media_player_v2") %}
        case
            when {{ event_name }} = 'end_event'
            then 100
            when {{ event_name }} = 'percent_progress_event'
            and coalesce({{ v2_duration }}, 0) > 0
            then (
              select max(p.pp)
              from (
                {% for element in get_percentage_boundaries(var("snowplow__percent_progress_boundaries")) %}
                  select {{ element }} as pp
                  {% if not loop.last %} union all {% endif %}
                {% endfor %}
              ) p
              where p.pp <= round({{ v2_current_time }} / {{ v2_duration }} * 100)
            )

            else null
        end,
      {% endif %}
      null
    )
{% endmacro %}
