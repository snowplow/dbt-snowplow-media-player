
select
    *
from {{ ref('snowplow_media_player_media_stats') }}
