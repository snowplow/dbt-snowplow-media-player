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
    }
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
      exists(select 1 from {{ this }}) = false or -- no records in the table
      a.viewed_at > ( select max(last_view) from {{ this }} )
    )
  {% endif %}

)

, new_data as (

  select
    a.media_ad_id,

    a.platform,
    a.media_id,
    max(a.media_label) as media_label,

    a.ad_id,
    max(a.name) as name,
    max(a.creative_id) as creative_id,
    max(a.duration_secs) as duration_secs,
    bool_or(a.skippable) as skippable,
    avg(a.pod_position) as pod_position,

    count(*) as views,
    sum(case when a.clicked then 1 else 0 end) as clicked,
    sum(case when a.skipped then 1 else 0 end) as skipped,
    sum(case when a._25_percent_reached then 1 else 0 end) as _25_percent_reached,
    sum(case when a._50_percent_reached then 1 else 0 end) as _50_percent_reached,
    sum(case when a._75_percent_reached then 1 else 0 end) as _75_percent_reached,
    sum(case when a._100_percent_reached then 1 else 0 end) as _100_percent_reached,

    {% if is_incremental() %}
    0 as views_unique,
    0 as clicked_unique,
    0 as skipped_unique,
    0 as _25_percent_reached_unique,
    0 as _50_percent_reached_unique,
    0 as _75_percent_reached_unique,
    0 as _100_percent_reached_unique,
    {% else %}
    count(distinct a.domain_userid) as views_unique,
    count(distinct case when a.clicked then a.domain_userid end) as clicked_unique,
    count(distinct case when a.skipped then a.domain_userid end) as skipped_unique,
    count(distinct case when a._25_percent_reached then domain_userid end) as _25_percent_reached_unique,
    count(distinct case when a._50_percent_reached then domain_userid end) as _50_percent_reached_unique,
    count(distinct case when a._75_percent_reached then domain_userid end) as _75_percent_reached_unique,
    count(distinct case when a._100_percent_reached then domain_userid end) as _100_percent_reached_unique,
    {% endif %}

    min(viewed_at) as first_view,
    max(viewed_at) as last_view

  from new_media_ad_views a

  group by 1, 2, 3, 5

)

{% if is_incremental() %}

, unique_counts_that_exist_in_new_data as (

  select
    a.media_ad_id,
    count(distinct a.domain_userid) as views_unique,
    count(distinct case when a.clicked then a.domain_userid end) as clicked_unique,
    count(distinct case when a.skipped then a.domain_userid end) as skipped_unique,
    count(distinct case when a._25_percent_reached then domain_userid end) as _25_percent_reached_unique,
    count(distinct case when a._50_percent_reached then domain_userid end) as _50_percent_reached_unique,
    count(distinct case when a._75_percent_reached then domain_userid end) as _75_percent_reached_unique,
    count(distinct case when a._100_percent_reached then domain_userid end) as _100_percent_reached_unique

  from {{ ref("snowplow_media_player_media_ad_views") }} a

  where
    -- enough time has passed since the page_view's start_tstamp to be able to process it as a whole (please bear in mind the late arriving data)
    cast({{ dateadd('hour', var("snowplow__max_media_pv_window", 10), 'a.viewed_at') }} as {{ type_timestamp() }}) < {{ snowplow_utils.current_timestamp_in_utc() }}

    -- exists in the new data
    and a.media_ad_id in (select distinct b.media_ad_id from new_media_ad_views b)

  group by 1

)

, all_data as (

  select * from new_data
  union all
  select * from {{ this }}

)

, all_data_grouped as (

  select
    a.media_ad_id,

    a.platform,
    a.media_id,
    max(a.media_label) as media_label,

    a.ad_id,
    max(a.name) as name,
    max(a.creative_id) as creative_id,
    max(a.duration_secs) as duration_secs,
    bool_or(a.skippable) as skippable,
    sum(a.pod_position * a.views) / sum(a.views) as pod_position,

    sum(a.views) as views,
    sum(a.clicked) as clicked,
    sum(a.skipped) as skipped,
    sum(a._25_percent_reached) as _25_percent_reached,
    sum(a._50_percent_reached) as _50_percent_reached,
    sum(a._75_percent_reached) as _75_percent_reached,
    sum(a._100_percent_reached) as _100_percent_reached,

    min(a.first_view) as first_view,
    max(a.last_view) as last_view

  from all_data a

  group by 1, 2, 3, 5

)

, prep as (

  select
    a.media_ad_id,

    a.platform,
    a.media_id,
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
    a._25_percent_reached,
    a._50_percent_reached,
    a._75_percent_reached,
    a._100_percent_reached,

    b.views_unique,
    b.clicked_unique,
    b.skipped_unique,
    b._25_percent_reached_unique,
    b._50_percent_reached_unique,
    b._75_percent_reached_unique,
    b._100_percent_reached_unique,

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
  {% if target.type in ['databricks', 'spark'] -%}
  , date(first_view) as first_view_date
  {%- endif %}

  from prep
