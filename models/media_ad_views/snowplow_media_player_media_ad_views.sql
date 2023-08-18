{{
  config(
    materialized= "incremental",
    upsert_date_key='last_event',
    sort = 'last_event',
    dist = 'media_ad_id',
    tags=["derived"],
    partition_by = snowplow_utils.get_value_by_target_type(bigquery_val={
      "field": "viewed_at",
      "data_type": "timestamp"
    }, databricks_val='viewed_at_date'),
    cluster_by=snowplow_utils.get_value_by_target_type(bigquery_val=["media_ad_id"]),
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt')),
    tblproperties={
      'delta.autoOptimize.optimizeWrite' : 'true',
      'delta.autoOptimize.autoCompact' : 'true'
    },
    snowplow_optimize=true
  )
}}

select *

from {{ ref('snowplow_media_player_media_ad_views_this_run') }}

--returns false if run doesn't contain new events.
where {{ snowplow_utils.is_run_with_new_events('snowplow_media_player') }}
