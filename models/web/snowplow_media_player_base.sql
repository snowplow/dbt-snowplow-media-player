{{
  config(
    materialized= var("snowplow__incremental_materialization", 'snowplow_incremental'),
    upsert_date_key='start_tstamp',
    unique_key = 'play_id',
    sort = 'start_tstamp',
    dist = 'play_id',
    tags=["derived"],
    partition_by = snowplow_utils.get_partition_by(bigquery_partition_by={
      "field": "start_tstamp",
      "data_type": "timestamp"
    }, databricks_partition_by='start_tstamp_date'),
    cluster_by=snowplow_utils.get_cluster_by(bigquery_cols=["media_id"]),
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
  )
}}

select *

from {{ ref('snowplow_media_player_base_this_run') }}

where {{ snowplow_utils.is_run_with_new_events('snowplow_web') }} --returns false if run doesn't contain new events.
