{% macro media_event_type_col(media_player_event_type, event_name) %}
  coalesce(
    {% if var("snowplow__enable_media_player_v1") %}{{ media_player_event_type }},{% endif %}
    case 
        when right({{ event_name }}, 6) = '_event'
        then replace(
            left({{ event_name }}, length({{ event_name }}) - 6),
            '_',
            ''
        )
        else null
    end
  ) as event_type
{% endmacro %}
