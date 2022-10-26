{{
  config(
    materialized='table',
    tags=["this_run"],
  )
}}

with prep as (

 select
    e.event_id,
    e.page_view_id,
    e.domain_sessionid,
    e.domain_userid,
    e.page_referrer,
    e.page_url,
    {{ snowplow_utils.get_optional_fields(
                enabled= true,
                fields=[{'field': 'label', 'dtype': 'string'}],
                col_prefix='unstruct_event_com_snowplowanalytics_snowplow_media_player_event_1',
                relation=ref('snowplow_web_base_events_this_run'),
                relation_alias='e',
                include_field_alias=false)}} as media_label,
    round(cast({{ snowplow_utils.get_optional_fields(
                 enabled= true,
                 fields=[{'field': 'duration', 'dtype': 'int'}],
                 col_prefix='contexts_com_snowplowanalytics_snowplow_media_player_1',
                 relation=ref('snowplow_web_base_events_this_run'),
                 relation_alias='e',
                 include_field_alias=false)}} as float64)) as duration,
    e.geo_region_name,
    e.br_name,
    e.dvce_type,
    e.os_name,
    e.os_timezone,
    {{ snowplow_utils.get_optional_fields(
                enabled= true,
                fields=[{'field': 'type', 'dtype': 'string'}],
                col_prefix='unstruct_event_com_snowplowanalytics_snowplow_media_player_event_1',
                relation=ref('snowplow_web_base_events_this_run'),
                relation_alias='e',
                   include_field_alias=false)}} as event_type,
    e.derived_tstamp as start_tstamp,
    {{ snowplow_utils.get_optional_fields(
                enabled= true,
                fields=[{'field': 'current_time', 'dtype': 'string'}],
                col_prefix='contexts_com_snowplowanalytics_snowplow_media_player_1',
                relation=ref('snowplow_web_base_events_this_run'),
                relation_alias='e',
                include_field_alias=false)}} as player_current_time,
    coalesce(cast({{ snowplow_utils.get_optional_fields(
                        enabled= true,
                        fields=[{'field': 'playback_rate', 'dtype': 'string'}],
                        col_prefix='contexts_com_snowplowanalytics_snowplow_media_player_1',
                        relation=ref('snowplow_web_base_events_this_run'),
                        relation_alias='e',
                        include_field_alias=false)}} as float64), 1) as playback_rate,
    case when {{ snowplow_utils.get_optional_fields(
                        enabled= true,
                        fields=[{'field': 'type', 'dtype': 'string'}],
                        col_prefix='unstruct_event_com_snowplowanalytics_snowplow_media_player_event_1',
                        relation=ref('snowplow_web_base_events_this_run'),
                        relation_alias='e',
                        include_field_alias=false)}} = 'ended'
        then 100
        else safe_cast({{ snowplow_utils.get_optional_fields(
                        enabled= true,
                        fields=[{'field': 'percent_progress', 'dtype': 'int'}],
                        col_prefix='contexts_com_snowplowanalytics_snowplow_media_player_1',
                        relation=ref('snowplow_web_base_events_this_run'),
                        relation_alias='e',
                        include_field_alias=false)}} as int64) end as percent_progress,
    cast({{ snowplow_utils.get_optional_fields(
                enabled= true,
                fields=[{'field': 'muted', 'dtype': 'string'}],
                col_prefix='contexts_com_snowplowanalytics_snowplow_media_player_1',
                relation=ref('snowplow_web_base_events_this_run'),
                relation_alias='e',
                include_field_alias=false)}} as boolean) as is_muted,
    {{ snowplow_utils.get_optional_fields(
                enabled= true,
                fields=[{'field': 'is_live', 'dtype': 'string'}],
                col_prefix='contexts_com_snowplowanalytics_snowplow_media_player_1',
                relation=ref('snowplow_web_base_events_this_run'),
                relation_alias='e',
                include_field_alias=false)}} as is_live,
    {{ snowplow_utils.get_optional_fields(
                enabled= true,
                fields=[{'field': 'loop', 'dtype': 'string'}],
                col_prefix='contexts_com_snowplowanalytics_snowplow_media_player_1',
                relation=ref('snowplow_web_base_events_this_run'),
                relation_alias='e',
                include_field_alias=false)}} as loop,
    {{ snowplow_utils.get_optional_fields(
                enabled= true,
                fields=[{'field': 'volume', 'dtype': 'string'}],
                col_prefix='contexts_com_snowplowanalytics_snowplow_media_player_1',
                relation=ref('snowplow_web_base_events_this_run'),
                relation_alias='e',
                include_field_alias=false)}} as volume,
    {% if var("snowplow__enable_whatwg_media") is false and var("snowplow__enable_whatwg_video") %}
      {{ exceptions.raise_compiler_error("variable: snowplow__enable_whatwg_video is enabled but variable: snowplow__enable_whatwg_media is not, both need to be enabled for modelling html5 video tracking data.") }}

    {% elif var("snowplow__enable_youtube") %}
      {% if var("snowplow__enable_whatwg_media") %}
        coalesce({{ snowplow_utils.get_optional_fields(
                            enabled= true,
                            fields=[{'field': 'player_id', 'dtype': 'string'}],
                            col_prefix='contexts_com_youtube_youtube_1',
                            relation=ref('snowplow_web_base_events_this_run'),
                            relation_alias='e',
                            include_field_alias=false)}}, {{ snowplow_utils.get_optional_fields(
                                                                                enabled= true,
                                                                                fields=[{'field': 'html_id', 'dtype': 'string'}],
                                                                                col_prefix='contexts_org_whatwg_media_element_1_',
                                                                                relation=ref('snowplow_web_base_events_this_run'),
                                                                                relation_alias='e',
                                                                                include_field_alias=false)}}) as media_id,
        case when {{ snowplow_utils.get_optional_fields(
                              enabled= true,
                              fields=[{'field': 'player_id', 'dtype': 'string'}],
                              col_prefix='contexts_com_youtube_youtube_1',
                              relation=ref('snowplow_web_base_events_this_run'),
                              relation_alias='e',
                              include_field_alias=false)}} is not null
            then 'com.youtube-youtube'
            when {{ snowplow_utils.get_optional_fields(
                              enabled= true,
                              fields=[{'field': 'html_id', 'dtype': 'string'}],
                              col_prefix='contexts_org_whatwg_media_element_1_',
                              relation=ref('snowplow_web_base_events_this_run'),
                              relation_alias='e',
                              include_field_alias=false)}} is not null
            then 'org.whatwg-media_element'
            else 'unknown' end as media_player_type,
        coalesce({{ snowplow_utils.get_optional_fields(
                            enabled= true,
                            fields=[{'field': 'url', 'dtype': 'string'}],
                            col_prefix='contexts_com_youtube_youtube_1',
                            relation=ref('snowplow_web_base_events_this_run'),
                            relation_alias='e',
                            include_field_alias=false)}}, {{ snowplow_utils.get_optional_fields(
                                                                      enabled= true,
                                                                      fields=[{'field': 'current_src', 'dtype': 'string'}],
                                                                      col_prefix='contexts_org_whatwg_media_element_1_',
                                                                      relation=ref('snowplow_web_base_events_this_run'),
                                                                      relation_alias='e',
                                                                      include_field_alias=false)}}) as source_url,
        case when {{ snowplow_utils.get_optional_fields(
                              enabled= true,
                              fields=[{'field': 'media_type', 'dtype': 'string'}],
                              col_prefix='contexts_org_whatwg_media_element_1_',
                              relation=ref('snowplow_web_base_events_this_run'),
                              relation_alias='e',
                              include_field_alias=false)}} = 'audio' then 'audio' else 'video' end as media_type,
        {% if var("snowplow__enable_whatwg_video") %}
          coalesce({{ snowplow_utils.get_optional_fields(
                              enabled= true,
                              fields=[{'field': 'playback_quality', 'dtype': 'string'}],
                              col_prefix='contexts_com_youtube_youtube_1',
                              relation=ref('snowplow_web_base_events_this_run'),
                              relation_alias='e',
                              include_field_alias=false)}}, {{ snowplow_utils.get_optional_fields(
                                                                                enabled= true,
                                                                                fields=[{'field': 'video_width', 'dtype': 'string'}],
                                                                                col_prefix='contexts_org_whatwg_video_element_1',
                                                                                relation=ref('snowplow_web_base_events_this_run'),
                                                                                relation_alias='e',
                                                                                include_field_alias=false)}}||'x'||{{ snowplow_utils.get_optional_fields(
                                                                                                                                        enabled= true,
                                                                                                                                        fields=[{'field': 'video_height', 'dtype': 'string'}],
                                                                                                                                        col_prefix='contexts_org_whatwg_video_element_1',
                                                                                                                                        relation=ref('snowplow_web_base_events_this_run'),
                                                                                                                                        relation_alias='e',
                                                                                                                                        include_field_alias=false)}}) as playback_quality
        {% else %}
          {{ snowplow_utils.get_optional_fields(
                              enabled= true,
                              fields=[{'field': 'playback_quality', 'dtype': 'string'}],
                              col_prefix='contexts_com_youtube_youtube_1',
                              relation=ref('snowplow_web_base_events_this_run'),
                              relation_alias='e')}},
        {% endif %}
      {% else %}
        {{ snowplow_utils.get_optional_fields(
                   enabled= true,
                   fields=[{'field': 'player_id', 'dtype': 'string'}],
                   col_prefix='contexts_com_youtube_youtube_1',
                   relation=ref('snowplow_web_base_events_this_run'),
                   relation_alias='e',
                   include_field_alias=false)}} as media_id,
        'com.youtube-youtube' as media_player_type,
        {{ snowplow_utils.get_optional_fields(
                            enabled= true,
                            fields=[{'field': 'url', 'dtype': 'string'}],
                            col_prefix='contexts_com_youtube_youtube_1',
                            relation=ref('snowplow_web_base_events_this_run'),
                            relation_alias='e',
                            include_field_alias=false)}} as source_url,
        'video' as media_type,
        {{ snowplow_utils.get_optional_fields(
                              enabled= true,
                              fields=[{'field': 'playback_quality', 'dtype': 'string'}],
                              col_prefix='contexts_com_youtube_youtube_1',
                              relation=ref('snowplow_web_base_events_this_run'),
                              relation_alias='e')}}
      {% endif %}

    {% elif var("snowplow__enable_whatwg_media") %}
      {{ snowplow_utils.get_optional_fields(
                  enabled= true,
                  fields=[{'field': 'html_id', 'dtype': 'string'}],
                  col_prefix='contexts_org_whatwg_media_element_1_',
                  relation=ref('snowplow_web_base_events_this_run'),
                  relation_alias='e',
                  include_field_alias=false)}} as media_id,
      'org.whatwg-media_element' as media_player_type,
      {{ snowplow_utils.get_optional_fields(
                  enabled= true,
                  fields=[{'field': 'current_src', 'dtype': 'string'}],
                  col_prefix='contexts_org_whatwg_media_element_1_',
                  relation=ref('snowplow_web_base_events_this_run'),
                  relation_alias='e',
                  include_field_alias=false)}} as source_url,
      case when {{ snowplow_utils.get_optional_fields(
                              enabled= true,
                              fields=[{'field': 'media_type', 'dtype': 'string'}],
                              col_prefix='contexts_org_whatwg_media_element_1_',
                              relation=ref('snowplow_web_base_events_this_run'),
                              relation_alias='e',
                              include_field_alias=false)}} = 'audio' then 'audio' else 'video' end as media_type,
      {% if var("snowplow__enable_whatwg_video") %}
        {{ snowplow_utils.get_optional_fields(
                    enabled= true,
                    fields=[{'field': 'video_width', 'dtype': 'string'}],
                    col_prefix='contexts_org_whatwg_video_element_1',
                    relation=ref('snowplow_web_base_events_this_run'),
                    relation_alias='e',
                    include_field_alias=false)}}||'x'||{{ snowplow_utils.get_optional_fields(
                                                                            enabled= true,
                                                                            fields=[{'field': 'video_height', 'dtype': 'string'}],
                                                                            col_prefix='contexts_org_whatwg_video_element_1',
                                                                            relation=ref('snowplow_web_base_events_this_run'),
                                                                            relation_alias='e',
                                                                            include_field_alias=false)}} as playback_quality
      {% else %}
        'N/A' as playback_quality
      {% endif %}

    {% else %}
      {{ exceptions.raise_compiler_error("No media context enabled. Please enable as many of the following variables as required: snowplow__enable_youtube, snowplow__enable_whatwg_media, snowplow__enable_whatwg_video") }}
    {% endif %}

    from {{ ref("snowplow_web_base_events_this_run") }} as e

    where event_name = 'media_player_event'
)

 select
  {{ dbt_utils.generate_surrogate_key(['p.page_view_id', 'p.media_id' ]) }} play_id,
  p.*,
  coalesce(cast(piv.weight_rate * p.duration / 100 as {{ type_int() }}), 0) as play_time_sec,
  coalesce(cast(case when p.is_muted = true then piv.weight_rate * p.duration / 100 else 0 end as {{ type_int() }}), 0) as play_time_sec_muted

  from prep p

  left join {{ ref("snowplow_media_player_pivot_base") }} piv
  on p.percent_progress = piv.percent_progress
