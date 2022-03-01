{{
  config(
    unique_key = 'play_id',
    sort = 'start_tstamp',
    dist = 'play_id'
  )
}}

with prep as (

  select
    i.play_id,
    i.page_view_id,
    i.media_id,
    max(i.domain_sessionid) as domain_sessionid,
    i.domain_userid,
    max(round(i.duration)) as duration,
    i.media_type,
    i.media_player_type,
    i.page_referrer,
    i.page_url,
    max(i.source_url) as source_url,
    i.geo_region_name,
    i.br_name,
    i.dvce_type,
    i.os_name,
    i.os_timezone,
    cast(coalesce(sum(p.weight_rate * i.duration / 100), 0) as {{ dbt_utils.type_int() }}) as play_time_sec_estimated,
    min(start_tstamp) start_tstamp,
    sum(case when i.event_type in ('seek', 'seeked') then 1 else 0 end) as seeks,
    max(i.percent_progress) as max_percent_progress,
    max(i.end_tstamp) end_tstamp,
    sum(i.play_time_sec_amended) as play_time_sec,
    sum(case when i.is_muted then i.play_time_sec_amended else 0 end) as play_time_sec_muted,
    sum(i.playback_rate * i.play_time_sec) / nullif(sum(i.play_time_sec), 0) as avg_playback_rate,
    sum(i.play_time_sec_amended)/ nullif(max(i.duration), 0) as retention_rate,
    {{ dbt_utils.pivot(
        column='i.percent_progress',
        values=dbt_utils.get_column_values( table=ref('pivot_base'), column='percent_progress', default=[]) | sort,
        alias=True,
        agg='bool_or',
        cmp='=',
        prefix='_',
        suffix='_percent_passed',
        else_value='0',
        quote_identifiers=FALSE
        ) }}

  from {{ ref("interactions") }} i

  left join {{ ref("pivot_base") }} p
  on i.percent_progress = p.percent_progress

  group by 1,2,3,5,7,8,9,10,12,13,14,15,16

)

, dedupe as (

  select
    *,
    row_number() over (partition by page_view_id order by start_tstamp) as duplicate_count

  from prep

)

select
  d.play_id,
  d.page_view_id,
  d.media_id,
  d.domain_sessionid,
  d.domain_userid,
  cast(d.duration as {{ dbt_utils.type_int() }}) as duration,
  d.media_type,
  d.media_player_type,
  d.page_referrer,
  d.page_url,
  d.source_url,
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
  case when d.play_time_sec > 0 then true else false end is_played,
  case when d.play_time_sec >= {{ var('snowplow__valid_play_sec') }} then true else false end is_valid_play,
  d.retention_rate,
  d.avg_playback_rate,
  case when d.retention_rate >= {{ var('snowplow__complete_play_rate') }} then true else false end is_complete_play,
  d.seeks,
  coalesce(d.max_percent_progress, 0) as max_percent_progress,
{% for element in var('snowplow__percent_progress_boundaries') %}
  d._{{ element }}_percent_passed as _{{ element }}_percent_passed
  {% if not loop.last %}
    ,
  {% endif %}
{% endfor %}

{% if 100 not in var("snowplow__percent_progress_boundaries") %}
  , d._100_percent_passed as _100_percent_passed
{% endif %}

from dedupe d

where d.duplicate_count = 1
