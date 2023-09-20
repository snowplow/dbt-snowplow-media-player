{{
  config(
    materialized='incremental',
    full_refresh=snowplow_media_player.allow_refresh(),
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt')),
    tblproperties={
      'delta.autoOptimize.optimizeWrite' : 'true',
      'delta.autoOptimize.autoCompact' : 'true'
    }
  )
}}

/*
Boilerplate to generate table.
Table updated as part of post-hook on sessions_this_run
Any sessions exceeding max_session_days are quarantined
Once quarantined, any subsequent events from the session will not be processed.
This significantly reduces table scans
*/

with prep as (
  select
    cast(null as {% if target.type == 'redshift' %} varchar(400) {% else %} {{snowplow_utils.type_max_string()}} {% endif %}) as session_id
)

select *

from prep
where false
