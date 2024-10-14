{#
Copyright (c) 2022-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{% set columns = adapter.get_columns_in_relation(ref('snowplow_media_player_media_stats')) %}

select
    {% for col in columns if col.name.lower() not in [
        'play_time_mins',
        'avg_play_time_mins',
        'avg_content_watched_mins',
        'avg_playback_rate',
        'avg_percent_played',
        'play_rate',
        'completion_rate_by_plays',
        'avg_retention_rate'
    ] and not col.name.lower().endswith('_date') %}{{ col.name }}, {% endfor %}

    round(cast(play_time_mins as {{ type_numeric() }}), 3) as play_time_mins,
    round(cast(avg_play_time_mins as {{ type_numeric() }}), 3) as avg_play_time_mins,
    round(cast(avg_content_watched_mins as {{ type_numeric() }}), 3) as avg_content_watched_mins,
    round(cast(avg_playback_rate as {{ type_numeric() }}), 3) as avg_playback_rate,
    round(cast(avg_percent_played as {{ type_numeric() }}), 3) as avg_percent_played,
    round(cast(play_rate as {{ type_numeric() }}), 3) as play_rate,
    round(cast(completion_rate_by_plays as {{ type_numeric() }}), 3) as completion_rate_by_plays,
    round(cast(avg_retention_rate as {{ type_numeric() }}), 3) as avg_retention_rate

from {{ ref('snowplow_media_player_media_stats_expected') }}
