{{
  config(
    materialized= var("snowplow__incremental_materialization", 'snowplow_incremental'),
    upsert_date_key='start_tstamp',
    unique_key = 'play_id',
    sort = 'start_tstamp',
    dist = 'play_id',
    tags=["derived"]
  )
}}

select *

from {{ ref('snowplow_media_player_base_this_run') }}

where {{ snowplow_utils.is_run_with_new_events('snowplow_web') }} --returns false if run doesn't contain new events.
