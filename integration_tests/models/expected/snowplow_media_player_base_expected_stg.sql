{#
Copyright (c) 2022-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{% set columns = adapter.get_columns_in_relation(ref('snowplow_media_player_base')) %}

select
    {% for col in columns if col.name.lower() not in [
        'play_time_secs',
        'play_time_muted_secs',
        'avg_playback_rate',
        'retention_rate',
        'content_watched_secs',
        'paused_time_secs',
        'buffering_time_secs',
        'ads_time_secs',
        'content_watched_percent'
    ] and not col.name.lower().endswith('_date') %}{{ col.name }}, {% endfor %}

    round(cast(play_time_secs as {{ type_numeric() }}), 3) as play_time_secs,
    round(cast(play_time_muted_secs as {{ type_numeric() }}), 3) as play_time_muted_secs,
    round(cast(avg_playback_rate as {{ type_numeric() }}), 3) as avg_playback_rate,
    round(cast(retention_rate as {{ type_numeric() }}), 3) as retention_rate,
    round(cast(content_watched_secs as {{ type_numeric() }}), 3) as content_watched_secs,
    round(cast(content_watched_percent as {{ type_numeric() }}), 3) as content_watched_percent,
    round(cast(paused_time_secs as {{ type_numeric() }}), 3) as paused_time_secs,
    round(cast(buffering_time_secs as {{ type_numeric() }}), 3) as buffering_time_secs,
    round(cast(ads_time_secs as {{ type_numeric() }}), 3) as ads_time_secs

from {{ ref('snowplow_media_player_base_expected') }}
