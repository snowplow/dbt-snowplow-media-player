{#
Copyright (c) 2022-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{{
  config(
    materialized= 'incremental',
    unique_key = 'media_ad_id',
    sort = 'last_view',
    dist = 'media_ad_id',
    tags=["derived"],
    partition_by = snowplow_utils.get_value_by_target_type(bigquery_val={
      "field": "first_view",
      "data_type": "timestamp"
    }, databricks_val='first_view_date'),
    cluster_by=snowplow_utils.get_value_by_target_type(bigquery_val=["media_ad_id"]),
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt')),
    tblproperties={
      'delta.autoOptimize.optimizeWrite' : 'true',
      'delta.autoOptimize.autoCompact' : 'true'
    },
    enabled=var('snowplow__enable_media_ad', false)
  )
}}

with

new_media_ad_views as (

  select *

  from {{ ref("snowplow_media_player_media_ad_views") }} a

  {% if is_incremental() %}
    where -- enough time has passed since the page_view's start_tstamp to be able to process it as a whole (please bear in mind the late arriving data)
    cast({{ dateadd('hour', var("snowplow__max_media_pv_window", 10), 'a.viewed_at') }} as {{ type_timestamp() }}) < {{ snowplow_utils.current_timestamp_in_utc() }}
    -- and it has not been processed yet
    and (
      not exists(select 1 from {{ this }}) or -- no records in the table
      a.viewed_at > ( select max(last_view) from {{ this }} )
    )
  {% endif %}

)

, new_data as (

  select
    a.media_ad_id,

    a.platform,
    a.app_id,
    a.media_identifier,
    max(a.media_label) as media_label,

    a.ad_id as ad_id,
    max(a.name) as name,
    max(a.creative_id) as creative_id,
    max(a.duration_secs) as duration_secs,
    sum(case when a.skippable then 1 else 0 end) > 0 as skippable,
    avg(a.pod_position) as pod_position,

    count(*) as views,
    sum(case when a.clicked then 1 else 0 end) as clicked,
    sum(case when a.skipped then 1 else 0 end) as skipped,
    sum(case when a.percent_reached_25 then 1 else 0 end) as percent_reached_25,
    sum(case when a.percent_reached_50 then 1 else 0 end) as percent_reached_50,
    sum(case when a.percent_reached_75 then 1 else 0 end) as percent_reached_75,
    sum(case when a.percent_reached_100 then 1 else 0 end) as percent_reached_100,

    {% if is_incremental() %}
      0 as views_unique,
      0 as clicked_unique,
      0 as skipped_unique,
      0 as percent_reached_25_unique,
      0 as percent_reached_50_unique,
      0 as percent_reached_75_unique,
      0 as percent_reached_100_unique,
    {% else %}
      count(distinct a.user_identifier) as views_unique,
      count(distinct case when a.clicked then a.user_identifier end) as clicked_unique,
      count(distinct case when a.skipped then a.user_identifier end) as skipped_unique,
      count(distinct case when a.percent_reached_25 then user_identifier end) as percent_reached_25_unique,
      count(distinct case when a.percent_reached_50 then user_identifier end) as percent_reached_50_unique,
      count(distinct case when a.percent_reached_75 then user_identifier end) as percent_reached_75_unique,
      count(distinct case when a.percent_reached_100 then user_identifier end) as percent_reached_100_unique,
    {% endif %}

    min(viewed_at) as first_view,
    max(viewed_at) as last_view

  from new_media_ad_views a

  group by 1, 2, 3, 4, 6

)

{% if is_incremental() %}

, unique_counts_that_exist_in_new_data as (

  select
    a.media_ad_id,
    count(distinct a.user_identifier) as views_unique,
    count(distinct case when a.clicked then a.user_identifier end) as clicked_unique,
    count(distinct case when a.skipped then a.user_identifier end) as skipped_unique,
    count(distinct case when a.percent_reached_25 then user_identifier end) as percent_reached_25_unique,
    count(distinct case when a.percent_reached_50 then user_identifier end) as percent_reached_50_unique,
    count(distinct case when a.percent_reached_75 then user_identifier end) as percent_reached_75_unique,
    count(distinct case when a.percent_reached_100 then user_identifier end) as percent_reached_100_unique

  from {{ ref("snowplow_media_player_media_ad_views") }} a

  where
    -- enough time has passed since the page_view's start_tstamp to be able to process it as a whole (please bear in mind the late arriving data)
    cast({{ dateadd('hour', var("snowplow__max_media_pv_window", 10), 'a.viewed_at') }} as {{ type_timestamp() }}) < {{ snowplow_utils.current_timestamp_in_utc() }}

    -- exists in the new data
    and exists(select 1 from new_media_ad_views as b where b.media_ad_id = a.media_ad_id)

  group by 1

)

, all_data as (

  select * from new_data
  union all
  select * {% if target.type in ['databricks'] %}except(first_view_date){% endif %}
  from {{ this }}

)

, all_data_grouped as (

  select
    a.media_ad_id,

    a.platform,
    a.app_id,
    a.media_identifier,
    max(a.media_label) as media_label,

    a.ad_id as ad_id,
    max(a.name) as name,
    max(a.creative_id) as creative_id,
    max(a.duration_secs) as duration_secs,
    sum(case when a.skippable then 1 else 0 end) > 0 as skippable,
    sum(a.pod_position * a.views) / sum(a.views) as pod_position,

    sum(a.views) as views,
    sum(a.clicked) as clicked,
    sum(a.skipped) as skipped,
    sum(a.percent_reached_25) as percent_reached_25,
    sum(a.percent_reached_50) as percent_reached_50,
    sum(a.percent_reached_75) as percent_reached_75,
    sum(a.percent_reached_100) as percent_reached_100,

    sum(a.views_unique) as views_unique,
    sum(a.clicked_unique) as clicked_unique,
    sum(a.skipped_unique) as skipped_unique,
    sum(a.percent_reached_25_unique) as percent_reached_25_unique,
    sum(a.percent_reached_50_unique) as percent_reached_50_unique,
    sum(a.percent_reached_75_unique) as percent_reached_75_unique,
    sum(a.percent_reached_100_unique) as percent_reached_100_unique,

    min(a.first_view) as first_view,
    max(a.last_view) as last_view

  from all_data a

  group by 1, 2, 3, 4, 6

)

, prep as (

  select
    a.media_ad_id,

    a.platform,
    a.app_id, 
    a.media_identifier,
    a.media_label,

    a.ad_id,
    a.name,
    a.creative_id,
    a.duration_secs,
    a.skippable,
    a.pod_position,

    a.views,
    a.clicked,
    a.skipped,
    a.percent_reached_25 as percent_reached_25,
    a.percent_reached_50 as percent_reached_50,
    a.percent_reached_75 as percent_reached_75,
    a.percent_reached_100,

    coalesce(b.views_unique, a.views_unique) as views_unique,
    coalesce(b.clicked_unique, a.clicked_unique) as clicked_unique,
    coalesce(b.skipped_unique, a.skipped_unique) as skipped_unique,
    coalesce(b.percent_reached_25_unique, a.percent_reached_25_unique) as percent_reached_25_unique,
    coalesce(b.percent_reached_50_unique, a.percent_reached_50_unique) as percent_reached_50_unique,
    coalesce(b.percent_reached_75_unique, a.percent_reached_75_unique) as percent_reached_75_unique,
    coalesce(b.percent_reached_100_unique, a.percent_reached_100_unique) as percent_reached_100_unique,

    a.first_view,
    a.last_view

  from all_data_grouped a

  left join unique_counts_that_exist_in_new_data b
    on a.media_ad_id = b.media_ad_id
)

{% else %}

, prep as (

  select * from new_data

)

{% endif %}

select *
  {% if target.type in ['databricks'] -%}
  , date(prep.first_view) as first_view_date
  {%- endif %}

  from prep
