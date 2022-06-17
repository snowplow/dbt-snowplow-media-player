{{
  config(
    materialized='view',
    tags=["derived"],
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
  )
}}

select *

from {{ ref("snowplow_media_player_base") }}

where is_played
