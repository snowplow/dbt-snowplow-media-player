{% macro percent_progress_col(v1_percent_progress, v1_event_type, event_name, v2_current_time, v2_duration) %}
    coalesce(
      {% if var("snowplow__enable_media_player_v1") %}
        case
          when {{ property_col(
            v1_event_type,
            col_prefix="unstruct_event_com_snowplowanalytics_snowplow_media_player_event_1",
            field='type'
          ) }} = 'ended'
          then 100
          else {{ property_col(
            v1_percent_progress,
            col_prefix='contexts_com_snowplowanalytics_snowplow_media_player_1'
          ) }}
        end,
      {% endif %}
      {% if var("snowplow__enable_media_player_v2") %}
        case
            when {{ event_name }} = 'end_event'
            then 100
            when {{ event_name }} = 'percent_progress_event'
            and coalesce({{ property_col(
              v2_duration,
              col_prefix='contexts_com_snowplowanalytics_snowplow_media_player_2'
            ) }}, 0) > 0
            then (
              select max(p.pp)
              from (
                {% for element in get_percentage_boundaries(var("snowplow__percent_progress_boundaries")) %}
                  select {{ element }} as pp
                  {% if not loop.last %} union all {% endif %}
                {% endfor %}
              ) p
              where p.pp <= round({{ property_col(
                v2_current_time,
                col_prefix='contexts_com_snowplowanalytics_snowplow_media_player_2'
              ) }} / {{ property_col(
                v2_duration,
                col_prefix='contexts_com_snowplowanalytics_snowplow_media_player_2'
              ) }} * 100)
            )

            else null
        end,
      {% endif %}
      null
    ) as percent_progress
{% endmacro %}
