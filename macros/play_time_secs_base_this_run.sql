
{% macro play_time_secs_base_this_run() %}
  {{ return(adapter.dispatch('play_time_secs_base_this_run', 'snowplow_media_player')()) }}
{% endmacro %}

{% macro default__play_time_secs_base_this_run() %}

  coalesce(s.media_session_time_played, d.play_time_secs)

{% endmacro %}
