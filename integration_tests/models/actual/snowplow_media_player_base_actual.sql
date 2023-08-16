{% set columns = adapter.get_columns_in_relation(ref('snowplow_media_player_base')) %}

select
    {% for col in columns if col.name.lower() not in [
        'play_time_secs',
        'play_time_muted_secs',
        'avg_playback_rate',
        'retention_rate',
        'content_watched_secs',
        'content_watched_percent'
    ] %}{{ col.name }}, {% endfor %}

    round(cast(play_time_secs as {{ type_numeric() }}), 3) as play_time_secs,
    round(cast(play_time_muted_secs as {{ type_numeric() }}), 3) as play_time_muted_secs,
    round(cast(avg_playback_rate as {{ type_numeric() }}), 3) as avg_playback_rate,
    round(cast(retention_rate as {{ type_numeric() }}), 3) as retention_rate,
    round(cast(content_watched_secs as {{ type_numeric() }}), 3) as content_watched_secs,
    round(cast(content_watched_percent as {{ type_numeric() }}), 3) as content_watched_percent

from {{ ref('snowplow_media_player_base') }}
