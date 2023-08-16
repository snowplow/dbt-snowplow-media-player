{{
  config(
    materialized='incremental',
    unique_key='session_id',
    upsert_date_key='start_tstamp',
    sort='start_tstamp',
    dist='session_id',
    partition_by = snowplow_utils.get_value_by_target_type(bigquery_val={
      "field": "start_tstamp",
      "data_type": "timestamp"
    }, databricks_val='start_tstamp_date'),
    cluster_by=snowplow_utils.get_value_by_target_type(bigquery_val=["session_id"], snowflake_val=["to_date(start_tstamp)"]),
    full_refresh=snowplow_media_player.allow_refresh(),
    tags=["manifest"],
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt')),
    tblproperties={
      'delta.autoOptimize.optimizeWrite' : 'true',
      'delta.autoOptimize.autoCompact' : 'true'
    },
    snowplow_optimize = true
  )
}}

-- Known edge cases:
-- 1: Rare case with multiple domain_userid per session.

{% set lower_limit, upper_limit, _ = snowplow_utils.return_base_new_event_limits(ref('snowplow_media_player_base_new_event_limits')) %}
{% set session_lookback_limit = snowplow_utils.get_session_lookback_limit(lower_limit) %}
{% set is_run_with_new_events = snowplow_utils.is_run_with_new_events('snowplow_media_player') %}

with

{% if var('snowplow__enable_mobile_events', false) -%}
    {{ snowplow_utils.get_sde_or_context(var('snowplow__atomic_schema', 'atomic'), var('snowplow__context_mobile_session'), lower_limit, upper_limit, 'mob_session') }},
{%- endif %}

new_events_session_ids as (
  select
    {% if var('snowplow__enable_mobile_events', false) %}
      coalesce(ms.mob_session_session_id, e.domain_sessionid) as session_id,
      max(coalesce(ms.mob_session_user_id, e.domain_userid)) as domain_userid,
    {% else %}
      e.domain_sessionid as session_id,
      max(e.domain_userid) as domain_userid, -- Edge case 1: Arbitary selection to avoid window function like first_value.
    {% endif %}
    min(e.collector_tstamp) as start_tstamp,
    max(e.collector_tstamp) as end_tstamp

  from {{ var('snowplow__events') }} e
  {% if var('snowplow__enable_mobile_events', false) -%}
      left join {{ var('snowplow__context_mobile_session') }} ms on e.event_id = ms.mob_session__id and e.collector_tstamp = ms.mob_session__tstamp
  {%- endif %}
  where
    {% if var('snowplow__enable_mobile_events', false) %}
      coalesce(ms.mob_session_session_id, e.domain_sessionid) is not null
      and not exists (select 1 from {{ ref('snowplow_media_player_base_quarantined_sessions') }} as a where a.session_id = coalesce(ms.mob_session_session_id, e.domain_sessionid)) -- don't continue processing v.long sessions
    {% else %}
      e.domain_sessionid is not null
      and not exists (select 1 from {{ ref('snowplow_media_player_base_quarantined_sessions') }} as a where a.session_id = e.domain_sessionid) -- don't continue processing v.long sessions
    {% endif %}
    and e.dvce_sent_tstamp <= {{ snowplow_utils.timestamp_add('day', var("snowplow__days_late_allowed", 3), 'dvce_created_tstamp') }} -- don't process data that's too late
    and e.collector_tstamp >= {{ lower_limit }}
    and e.collector_tstamp <= {{ upper_limit }}
    and {{ snowplow_utils.app_id_filter(var("snowplow__app_id",[])) }}
    and {{ event_name_filter(var("snowplow__media_event_names", ["media_player_event"]))}}
    and {{ is_run_with_new_events }} --don't reprocess sessions that have already been processed.
    {% if var('snowplow__derived_tstamp_partitioned', true) and target.type == 'bigquery' | as_bool() %} -- BQ only
      and e.derived_tstamp >= {{ lower_limit }}
      and e.derived_tstamp <= {{ upper_limit }}
    {% endif %}

  group by 1
  )

{% if is_incremental() %}

, previous_sessions as (
  select *

  from {{ this }}

  where start_tstamp >= {{ session_lookback_limit }}
  and {{ is_run_with_new_events }} --don't reprocess sessions that have already been processed.
)

, session_lifecycle as (
  select
    ns.session_id,
    coalesce(self.domain_userid, ns.domain_userid) as domain_userid, -- Edge case 1: Take previous value to keep domain_userid consistent. Not deterministic but performant
    least(ns.start_tstamp, coalesce(self.start_tstamp, ns.start_tstamp)) as start_tstamp,
    greatest(ns.end_tstamp, coalesce(self.end_tstamp, ns.end_tstamp)) as end_tstamp -- BQ 1 NULL will return null hence coalesce

  from new_events_session_ids ns
  left join previous_sessions as self
    on ns.session_id = self.session_id

  where
    self.session_id is null -- process all new sessions
    or self.end_tstamp < {{ snowplow_utils.timestamp_add('day', var("snowplow__max_session_days", 3), 'self.start_tstamp') }} --stop updating sessions exceeding 3 days
  )

{% else %}

, session_lifecycle as (

  select * from new_events_session_ids

)

{% endif %}

select
  sl.session_id,
  sl.domain_userid,
  sl.start_tstamp,
  least({{ snowplow_utils.timestamp_add('day', var("snowplow__max_session_days", 3), 'sl.start_tstamp') }}, sl.end_tstamp) as end_tstamp -- limit session length to max_session_days
  {% if target.type in ['databricks', 'spark'] -%}
  , DATE(sl.start_tstamp) as start_tstamp_date
  {%- endif %}

from session_lifecycle sl
