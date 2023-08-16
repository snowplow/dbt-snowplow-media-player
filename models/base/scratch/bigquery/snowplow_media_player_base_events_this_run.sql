{{
  config(
    tags=["this_run"]
  )
}}

{%- set lower_limit, upper_limit = snowplow_utils.return_limits_from_model(ref('snowplow_media_player_base_sessions_this_run'),
                                                                            'start_tstamp',
                                                                            'end_tstamp') %}

-- check for exceptions
{% if var("snowplow__enable_whatwg_media") is false and var("snowplow__enable_whatwg_video") %}
  {{ exceptions.raise_compiler_error("variable: snowplow__enable_whatwg_video is enabled but variable: snowplow__enable_whatwg_media is not, both need to be enabled for modelling html5 video tracking data.") }}
{% elif not var("snowplow__enable_youtube") and not var("snowplow__enable_whatwg_media") %}
  {{ exceptions.raise_compiler_error("No media context enabled. Please enable as many of the following variables as required: snowplow__enable_youtube, snowplow__enable_whatwg_media, snowplow__enable_whatwg_video") }}
{% endif %}

with prep as (

  select
    *,
    dense_rank() over (partition by ev.session_id order by ev.start_tstamp) AS event_in_session_index,

  from (

    select

      a.event_id,
      a.contexts_com_snowplowanalytics_snowplow_web_page_1_0_0[safe_offset(0)].id as page_view_id,
      b.session_id,
      a.domain_userid,
      a.page_referrer,
      a.page_url,
      a.geo_region_name,
      a.br_name,
      a.dvce_type,
      a.os_name,
      a.os_timezone,
      a.derived_tstamp as start_tstamp,
      a.collector_tstamp,

      -- unpacking the media player event
      {{ snowplow_utils.get_optional_fields(
          enabled= true,
          fields=[{'field': 'label', 'dtype': 'string'}],
          col_prefix='unstruct_event_com_snowplowanalytics_snowplow_media_player_event_1',
          relation=source('atomic', 'events'),
          relation_alias='a',
          include_field_alias=false)}} as media_label,
      {{ snowplow_utils.get_optional_fields(
          enabled= true,
          fields=[{'field': 'type', 'dtype': 'string'}],
          col_prefix='unstruct_event_com_snowplowanalytics_snowplow_media_player_event_1',
          relation=source('atomic', 'events'),
          relation_alias='a',
          include_field_alias=false)}} as event_type,

      -- unpacking the media player object
      round(cast({{ snowplow_utils.get_optional_fields(
                      enabled= true,
                      fields=[{'field': 'duration', 'dtype': 'string'}],
                      col_prefix='contexts_com_snowplowanalytics_snowplow_media_player_1',
                      relation=source('atomic', 'events'),
                      relation_alias='a',
                      include_field_alias=false)}} as float64)) as duration_secs,
      {{ snowplow_utils.get_optional_fields(
          enabled= true,
          fields=[{'field': 'current_time', 'dtype': 'string'}],
          col_prefix='contexts_com_snowplowanalytics_snowplow_media_player_1',
          relation=source('atomic', 'events'),
          relation_alias='a',
          include_field_alias=false)}} as player_current_time,
      coalesce(cast({{ snowplow_utils.get_optional_fields(
                        enabled= true,
                        fields=[{'field': 'playback_rate', 'dtype': 'string'}],
                        col_prefix='contexts_com_snowplowanalytics_snowplow_media_player_1',
                        relation=source('atomic', 'events'),
                        relation_alias='a',
                        include_field_alias=false)}} as float64), 1) as playback_rate,
      case when {{ snowplow_utils.get_optional_fields(
                    enabled= true,
                    fields=[{'field': 'type', 'dtype': 'string'}],
                    col_prefix='unstruct_event_com_snowplowanalytics_snowplow_media_player_event_1',
                    relation=source('atomic', 'events'),
                    relation_alias='a',
                    include_field_alias=false)}} = 'ended'
          then 100
          else safe_cast({{ snowplow_utils.get_optional_fields(
                              enabled= true,
                              fields=[{'field': 'percent_progress', 'dtype': 'int'}],
                              col_prefix='contexts_com_snowplowanalytics_snowplow_media_player_1',
                              relation=source('atomic', 'events'),
                              relation_alias='a',
                              include_field_alias=false)}} as int64) end as percent_progress,
      cast({{ snowplow_utils.get_optional_fields(
                enabled= true,
                fields=[{'field': 'muted', 'dtype': 'string'}],
                col_prefix='contexts_com_snowplowanalytics_snowplow_media_player_1',
                relation=source('atomic', 'events'),
                relation_alias='a',
                include_field_alias=false)}} as boolean) as is_muted,
      {{ snowplow_utils.get_optional_fields(
          enabled= true,
          fields=[{'field': 'is_live', 'dtype': 'string'}],
          col_prefix='contexts_com_snowplowanalytics_snowplow_media_player_1',
          relation=source('atomic', 'events'),
          relation_alias='a',
          include_field_alias=false)}} as is_live,
      {{ snowplow_utils.get_optional_fields(
          enabled= true,
          fields=[{'field': 'loop', 'dtype': 'string'}],
          col_prefix='contexts_com_snowplowanalytics_snowplow_media_player_1',
          relation=source('atomic', 'events'),
          relation_alias='a',
          include_field_alias=false)}} as loop,
      {{ snowplow_utils.get_optional_fields(
          enabled= true,
          fields=[{'field': 'volume', 'dtype': 'string'}],
          col_prefix='contexts_com_snowplowanalytics_snowplow_media_player_1',
          relation=source('atomic', 'events'),
          relation_alias='a',
          include_field_alias=false)}} as volume,

      -- combined media properties
      {{ media_id_col(
        youtube_player_id=snowplow_utils.get_optional_fields(
          enabled= true,
          fields=[{'field': 'player_id', 'dtype': 'string'}],
          col_prefix='contexts_com_youtube_youtube_1',
          relation=source('atomic', 'events'),
          relation_alias='a',
          include_field_alias=false),
        media_player_id=snowplow_utils.get_optional_fields(
          enabled= true,
          fields=[{'field': 'html_id', 'dtype': 'string'}],
          col_prefix='contexts_org_whatwg_media_element_1_',
          relation=source('atomic', 'events'),
          relation_alias='a',
          include_field_alias=false)
      ) }},
      {{ media_player_type_col(
        youtube_player_id=snowplow_utils.get_optional_fields(
          enabled= true,
          fields=[{'field': 'player_id', 'dtype': 'string'}],
          col_prefix='contexts_com_youtube_youtube_1',
          relation=source('atomic', 'events'),
          relation_alias='a',
          include_field_alias=false),
        media_player_id=snowplow_utils.get_optional_fields(
          enabled= true,
          fields=[{'field': 'html_id', 'dtype': 'string'}],
          col_prefix='contexts_org_whatwg_media_element_1_',
          relation=source('atomic', 'events'),
          relation_alias='a',
          include_field_alias=false)
      ) }},
      {{ source_url_col(
        youtube_url=snowplow_utils.get_optional_fields(
          enabled= true,
          fields=[{'field': 'url', 'dtype': 'string'}],
          col_prefix='contexts_com_youtube_youtube_1',
          relation=source('atomic', 'events'),
          relation_alias='a',
          include_field_alias=false),
        media_current_src=snowplow_utils.get_optional_fields(
          enabled= true,
          fields=[{'field': 'current_src', 'dtype': 'string'}],
          col_prefix='contexts_org_whatwg_media_element_1_',
          relation=source('atomic', 'events'),
          relation_alias='a',
          include_field_alias=false)
      ) }},
      {{ media_type_col(
        media_media_type=snowplow_utils.get_optional_fields(
          enabled= true,
          fields=[{'field': 'media_type', 'dtype': 'string'}],
          col_prefix='contexts_org_whatwg_media_element_1_',
          relation=source('atomic', 'events'),
          relation_alias='a',
          include_field_alias=false)
      )}},
      {{ playback_quality_col(
        youtube_quality=snowplow_utils.get_optional_fields(
          enabled= true,
          fields=[{'field': 'playback_quality', 'dtype': 'string'}],
          col_prefix='contexts_com_youtube_youtube_1',
          relation=source('atomic', 'events'),
          relation_alias='a',
          include_field_alias=false),
        video_width=snowplow_utils.get_optional_fields(
          enabled= true,
          fields=[{'field': 'video_width', 'dtype': 'string'}],
          col_prefix='contexts_org_whatwg_video_element_1',
          relation=source('atomic', 'events'),
          relation_alias='a',
          include_field_alias=false),
        video_height=snowplow_utils.get_optional_fields(
          enabled= true,
          fields=[{'field': 'video_height', 'dtype': 'string'}],
          col_prefix='contexts_org_whatwg_video_element_1',
          relation=source('atomic', 'events'),
          relation_alias='a',
          include_field_alias=false)
      )}}

      from {{ var('snowplow__events') }} as a
      inner join {{ ref('snowplow_media_player_base_sessions_this_run') }} as b
      on a.domain_sessionid = b.session_id

      where a.collector_tstamp <= {{ snowplow_utils.timestamp_add('day', var("snowplow__max_session_days", 3), 'b.start_tstamp') }}
      and a.dvce_sent_tstamp <= {{ snowplow_utils.timestamp_add('day', var("snowplow__days_late_allowed", 3), 'a.dvce_created_tstamp') }}
      and a.collector_tstamp >= {{ lower_limit }}
      and a.collector_tstamp <= {{ upper_limit }}
      {% if var('snowplow__derived_tstamp_partitioned', true) and target.type == 'bigquery' | as_bool() %}
        and a.derived_tstamp >= {{ snowplow_utils.timestamp_add('hour', -1, lower_limit) }}
        and a.derived_tstamp <= {{ upper_limit }}
      {% endif %}
      and {{ snowplow_utils.app_id_filter(var("snowplow__app_id",[])) }}
      and {{ snowplow_media_player.event_name_filter(var("snowplow__media_event_names", "['media_player_event']")) }}

      qualify row_number() over (partition by a.event_id order by a.collector_tstamp) = 1
  ) ev
)

select
  {{ dbt_utils.generate_surrogate_key(['p.page_view_id', 'p.media_id' ]) }} play_id,
  p.*,
  coalesce(cast(piv.weight_rate * p.duration_secs / 100 as {{ type_int() }}), 0) as play_time_secs,
  coalesce(cast(case when p.is_muted = true then piv.weight_rate * p.duration_secs / 100 else 0 end as {{ type_int() }}), 0) as play_time_muted_secs

  from prep p

  left join {{ ref("snowplow_media_player_pivot_base") }} piv
  on p.percent_progress = piv.percent_progress
