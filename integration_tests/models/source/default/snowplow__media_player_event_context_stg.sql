
select
  *

from {{ ref('snowplow_media_player_event_context') }}

