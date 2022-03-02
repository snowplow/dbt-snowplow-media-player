{{
  config(
    materialized='incremental',
    unique_key = 'event_id',
    sort = 'derived_tstamp',
    dist = 'event_id'
  )
}}

with prep as (

  select
    e.event_id,
    pvc.id as page_view_id,
    e.domain_sessionid,
    e.domain_userid,
    e.page_referrer,
    e.page_url,
    e.geo_region_name,
    e.br_name,
    e.dvce_type,
    e.os_name,
    e.os_timezone,
    e.derived_tstamp

  from {{ source('atomic', 'events') }} as e

  left join {{ source('atomic', 'com_snowplowanalytics_snowplow_web_page_1') }} as pvc
  on pvc.root_id = e.event_id and pvc.root_tstamp = e.collector_tstamp

  where e.collector_tstamp >= {{ var("snowplow__mp_start_date") }}
  and e.event_name = 'media_player_event' and pvc.id is not null

  {% if is_incremental() %}
  and e.collector_tstamp > (select dateadd(hour, -{{ var("snowplow__mp_lookback_hours") }}, max(derived_tstamp)) from {{ this }})
  {% endif %}

)

select
    event_id,
    page_view_id,
    domain_sessionid,
    domain_userid,
    page_referrer,
    page_url,
    geo_region_name,
    br_name,
    dvce_type,
    os_name,
    os_timezone,
    derived_tstamp

from prep
