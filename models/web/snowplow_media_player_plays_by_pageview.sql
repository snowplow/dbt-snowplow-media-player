{{
  config(
    materialized='view',
    tags=["derived"]
  )
}}

select *

from {{ ref("snowplow_media_player_base") }}

where is_played
