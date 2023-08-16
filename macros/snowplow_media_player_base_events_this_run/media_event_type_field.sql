{% macro media_event_type_field(media_player_event_type, event_name) %}
  coalesce(
    {% if var("snowplow__enable_media_player_v1") -%}
      -- for v1 media schemas, use the type property in media_player_event
      {{ field(
        media_player_event_type,
        col_prefix="unstruct_event_com_snowplowanalytics_snowplow_media_player_event_1",
        field='type'
      ) }}
    {%- else -%}
      null
    {%- endif %},
    -- for v2 media schemas, the type is the event name, remove underscores to match v1 event types
    case 
        when right({{ event_name }}, 6) = '_event'
        then replace(
            left({{ event_name }}, length({{ event_name }}) - 6),
            '_',
            ''
        )
        else null
    end
  )
{% endmacro %}
