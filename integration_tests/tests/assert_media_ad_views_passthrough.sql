-- tests/assert_media_ad_views_passthrough.sql
with base_data as (
  select * from {{ ref('snowplow_media_player_base_events_this_run') }}
  where media_ad__ad_id is not null 
    and media_identifier is not null
),

expected as (
  select
    {{ dbt_utils.generate_surrogate_key(['platform', 'media_identifier', 'media_ad__ad_id']) }} as media_ad_id,
    -- simple passthrough
    v_collector,
    -- max aggregation
    max(v_tracker || app_id) as tracker_app_id
  from base_data
  group by 1, 2
),

actual as (
  select 
    media_ad_id,
    v_collector,
    tracker_app_id
  from {{ ref('snowplow_media_player_media_ad_views_this_run') }}
),

comparison as (
  select 
    coalesce(e.media_ad_id, a.media_ad_id) as media_ad_id,
    case 
      when e.v_collector != a.v_collector then 1
      when e.tracker_app_id != a.tracker_app_id then 1
      when e.media_ad_id is null then 1
      when a.media_ad_id is null then 1
      else 0
    end as has_mismatch
  from expected e
  full outer join actual a using (media_ad_id)
)

select
  'Data mismatch found' as test_name,
  sum(has_mismatch) as failures
from comparison
having sum(has_mismatch) > 0