{{ 
  config(
    materialized='table',
    unique_key = 'play_id',
    sort = 'start_tstamp',
    dist = 'play_id'
  )
}}

with prep as (

  select
    {{ dbt_utils.hash('i.page_view_id||i.media_id') }} play_id,
    i.page_view_id,
    i.media_id,
    max(i.domain_sessionid) as domain_sessionid,
    i.domain_userid,
    i.duration,
    i.title,
    i.media_type,
    i.media_player_type,
    i.page_referrer,
    i.content_url,
    i.geo_region_name,
    i.br_name,
    i.dvce_type,
    i.os_name,
    i.os_timezone,
    coalesce(sum(p.weight_rate * i.duration), 0)::int as play_time_sec_estimated,
    min(start_tstamp) start_tstamp,
    sum(case when i.event_type in ('seek', 'seeked') then 1 else 0 end) as seeks,
    max(case 
          when i.event_type = 'ended' then 100 
          when i.percent_progress = 0 then NULL 
          else i.percent_progress 
        end) as last_percent_progress,
    max(i.end_tstamp) end_tstamp,
    sum(i.play_time_sec_amended) as play_time_sec,
    sum(case when i.muted then i.play_time_sec_amended else 0 end) as play_time_sec_muted,
    coalesce(sum(i.playback_rate * i.play_time_sec) / nullif(sum(i.play_time_sec), 0), 0) as avg_playback_rate,
    coalesce(sum(i.play_time_sec_amended)/ nullif(max(i.duration), 0), 0) as retention_rate
  
  from {{ ref("interactions") }} i

  left join {{ ref("pivot_base") }} p
  on i.percent_progress = p.percent_progress

  group by
    {{ dbt_utils.hash('page_view_id||media_id') }},
    page_view_id,
    media_id,
    domain_userid,
    duration,
    title,
    media_type,
    media_player_type,
    page_referrer,
    content_url,
    geo_region_name,
    br_name,
    dvce_type,
    os_name,
    os_timezone

  having sum(case when i.event_type in ('play', 'playing') then 1 else 0 end) > 0

)

, dedupe as (

    select 
      *,
      row_number() over (partition by page_view_id order by start_tstamp) as duplicate_count

    from prep

)

, pivoting as (

    select
      {{ dbt_utils.hash('page_view_id||media_id') }} play_id,
      {{ dbt_utils.pivot(
        column='percent_progress',
        values=dbt_utils.get_column_values( table=ref('pivot_base'), column='percent_progress', default=[]) | sort,
        alias=True,
        agg='count',
        cmp='=',
        prefix='_',
        suffix='_percent_passed',
        else_value='NULL',
        quote_identifiers=FALSE
        ) }}

    from {{ ref("interactions") }} 

    group by {{ dbt_utils.hash('page_view_id||media_id') }}

)

select
  d.play_id,
  d.page_view_id,
  d.media_id,
  d.domain_sessionid,
  d.domain_userid,
  d.duration:: int,
  d.title,
  d.media_type,
  d.media_player_type,
  d.page_referrer,
  d.content_url,
  d.geo_region_name,
  d.br_name,
  d.dvce_type,
  d.os_name,
  d.os_timezone,
  d.start_tstamp,
  d.end_tstamp,
  d.play_time_sec,
  d.play_time_sec_muted,
  d.play_time_sec_estimated,
  d.retention_rate,
  case when d.play_time_sec >= {{ var('snowplow__valid_play_sec') }} then true else false end is_valid_play,
  case when d.retention_rate >= {{ var('snowplow__complete_play_rate') }} then true else false end is_complete_play,
  d.seeks,
  coalesce(d.last_percent_progress, 0) as last_percent_progress,
{% for key, value in var('snowplow__percent_progress_boundaries').items() %}
  p._{{ key }}_percent_passed::boolean
  {% if not loop.last %}
    ,
  {% endif %}
{% endfor %}

from dedupe d

left join pivoting p
on p.play_id = d.play_id

where d.duplicate_count = 1
