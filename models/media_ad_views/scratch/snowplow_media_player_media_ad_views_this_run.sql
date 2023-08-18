{{
  config(
    materialized='table',
    tags=["this_run"],
    sort='last_event',
    dist='media_ad_id',
    partition_by = snowplow_utils.get_value_by_target_type(bigquery_val={
      "field": "viewed_at",
      "data_type": "timestamp"
    }, databricks_val='viewed_at_date'),
    cluster_by=snowplow_utils.get_value_by_target_type(bigquery_val=["media_ad_id"]),
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
  )
}}

with

events_this_run as (

    select * from {{ ref('snowplow_media_player_base_events_this_run') }}
    where ad_id is not null and media_id is not null

)

, prep as (

  select
    {{ dbt_utils.generate_surrogate_key(['ev.platform', 'ev.media_id', 'ev.ad_id']) }} as media_ad_id,

    ev.platform,
    ev.media_id,
    max(ev.media_label) as media_label,
    ev.domain_userid,
    ev.user_id,
    ev.play_id,

    ev.ad_break_id,
    max(ev.ad_break_name) as ad_break_name,
    max(ev.ad_break_type) as ad_break_type,

    ev.ad_id,
    max(ev.ad_name) as name,
    max(ev.ad_creative_id) as creative_id,
    max(ev.ad_duration) as duration_secs,
    avg(ev.ad_pod_position) as pod_position,
    sum(case when ev.ad_skippable then 1 else 0 end) > 0 as skippable,

    max(case when ev.event_type in ('adclick') then 1 else 0 end) > 0 as clicked,
    max(case when ev.event_type in ('adskip') then 1 else 0 end) > 0 as skipped,
    max(case when ev.event_type in ('adcomplete') or (ev.event_type in ('adquartile') and ev.ad_percent_progress >= 25) then 1 else 0 end) > 0 as _25_percent_reached,
    max(case when ev.event_type in ('adcomplete') or (ev.event_type in ('adquartile') and ev.ad_percent_progress >= 50) then 1 else 0 end) > 0 as _50_percent_reached,
    max(case when ev.event_type in ('adcomplete') or (ev.event_type in ('adquartile') and ev.ad_percent_progress >= 75) then 1 else 0 end) > 0 as _75_percent_reached,
    max(case when ev.event_type in ('adcomplete') then 1 else 0 end) > 0 as _100_percent_reached,

    min(case when ev.event_type in ('adstart') then ev.start_tstamp end) as viewed_at,
    max(ev.start_tstamp) as last_event

    {% if target.type in ['databricks', 'spark'] -%}
      , date(ev.viewed_at) as viewed_at_date
    {%- endif %}

  from events_this_run as ev

  group by 1, 2, 3, 5, 6, 7, 8, 11

)

select * from prep
