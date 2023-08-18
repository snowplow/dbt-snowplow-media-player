{{ config(
  materialized='table',
  )
}}

-- page view context is given as json string in csv. Extract array from json
with prep as (
  select
    * except(
      contexts_com_snowplowanalytics_snowplow_web_page_1_0_0,
      contexts_com_snowplowanalytics_mobile_screen_1_0_0,
      contexts_com_snowplowanalytics_snowplow_client_session_1_0_2,
      contexts_org_whatwg_video_element_1_0_0,
      contexts_org_whatwg_media_element_1_0_0,
      contexts_com_youtube_youtube_1_0_0,
      contexts_com_snowplowanalytics_snowplow_media_player_1_0_0,
      contexts_com_snowplowanalytics_snowplow_media_player_2_0_0,
      contexts_com_snowplowanalytics_snowplow_media_session_1_0_0,
      contexts_com_snowplowanalytics_snowplow_media_ad_1_0_0,
      contexts_com_snowplowanalytics_snowplow_media_ad_break_1_0_0,
      unstruct_event_com_snowplowanalytics_snowplow_media_player_event_1_0_0,
      unstruct_event_com_snowplowanalytics_snowplow_media_ad_quartile_event_1_0_0
    ),
    JSON_EXTRACT_ARRAY(contexts_com_snowplowanalytics_snowplow_web_page_1_0_0) AS contexts_com_snowplowanalytics_snowplow_web_page_1_0_0,
    JSON_EXTRACT_ARRAY(contexts_com_snowplowanalytics_mobile_screen_1_0_0) AS contexts_com_snowplowanalytics_mobile_screen_1_0_0,
    JSON_EXTRACT_ARRAY(contexts_com_snowplowanalytics_snowplow_client_session_1_0_2) AS contexts_com_snowplowanalytics_snowplow_client_session_1_0_2,
    JSON_EXTRACT_ARRAY(contexts_org_whatwg_video_element_1_0_0) AS contexts_org_whatwg_video_element_1_0_0,
    JSON_EXTRACT_ARRAY(contexts_org_whatwg_media_element_1_0_0) AS contexts_org_whatwg_media_element_1_0_0,
    JSON_EXTRACT_ARRAY(contexts_com_youtube_youtube_1_0_0) AS contexts_com_youtube_youtube_1_0_0,
    JSON_EXTRACT_ARRAY(contexts_com_snowplowanalytics_snowplow_media_player_1_0_0) AS contexts_com_snowplowanalytics_snowplow_media_player_1_0_0,
    JSON_EXTRACT_ARRAY(contexts_com_snowplowanalytics_snowplow_media_player_2_0_0) AS contexts_com_snowplowanalytics_snowplow_media_player_2_0_0,
    JSON_EXTRACT_ARRAY(contexts_com_snowplowanalytics_snowplow_media_session_1_0_0) AS contexts_com_snowplowanalytics_snowplow_media_session_1_0_0,
    JSON_EXTRACT_ARRAY(contexts_com_snowplowanalytics_snowplow_media_ad_1_0_0) AS contexts_com_snowplowanalytics_snowplow_media_ad_1_0_0,
    JSON_EXTRACT_ARRAY(contexts_com_snowplowanalytics_snowplow_media_ad_break_1_0_0) AS contexts_com_snowplowanalytics_snowplow_media_ad_break_1_0_0,
    PARSE_JSON(unstruct_event_com_snowplowanalytics_snowplow_media_player_event_1_0_0) AS unstruct_event_com_snowplowanalytics_snowplow_media_player_event_1_0_0,
    PARSE_JSON(unstruct_event_com_snowplowanalytics_snowplow_media_ad_quartile_event_1_0_0) AS unstruct_event_com_snowplowanalytics_snowplow_media_ad_quartile_event_1_0_0

  from {{ ref('snowplow_media_player_events') }}
)

-- recreate repeated record field i.e. array of structs as is originally in BQ events table
select
  * except(
    contexts_com_snowplowanalytics_snowplow_web_page_1_0_0,
    contexts_com_snowplowanalytics_mobile_screen_1_0_0,
    contexts_com_snowplowanalytics_snowplow_client_session_1_0_2,
    contexts_org_whatwg_video_element_1_0_0,
    contexts_org_whatwg_media_element_1_0_0,
    contexts_com_youtube_youtube_1_0_0,
    contexts_com_snowplowanalytics_snowplow_media_player_1_0_0,
    contexts_com_snowplowanalytics_snowplow_media_player_2_0_0,
    contexts_com_snowplowanalytics_snowplow_media_session_1_0_0,
    contexts_com_snowplowanalytics_snowplow_media_ad_1_0_0,
    contexts_com_snowplowanalytics_snowplow_media_ad_break_1_0_0,
    unstruct_event_com_snowplowanalytics_snowplow_media_player_event_1_0_0,
    unstruct_event_com_snowplowanalytics_snowplow_media_ad_quartile_event_1_0_0
  ),
  array(
    select as struct JSON_EXTRACT_scalar(json_array,'$.id') as id
    from unnest(contexts_com_snowplowanalytics_snowplow_web_page_1_0_0) as json_array
  ) as contexts_com_snowplowanalytics_snowplow_web_page_1_0_0,
  array(
    select as struct JSON_EXTRACT_scalar(json_array,'$.name') as name,
                    JSON_EXTRACT_scalar(json_array,'$.type') as type,
                    JSON_EXTRACT_scalar(json_array,'$.id') as id,
                    JSON_EXTRACT_scalar(json_array,'$.view_controller') as view_controller,
                    JSON_EXTRACT_scalar(json_array,'$.top_view_controller') as top_view_controller,
                    JSON_EXTRACT_scalar(json_array,'$.activity') as activity,
                    JSON_EXTRACT_scalar(json_array,'$.fragment') as fragment
    from unnest(contexts_com_snowplowanalytics_mobile_screen_1_0_0) as json_array
  ) as contexts_com_snowplowanalytics_mobile_screen_1_0_0,
  array(
    select as struct JSON_EXTRACT_scalar(json_array,'$.user_id') as user_id,
                    JSON_EXTRACT_scalar(json_array,'$.session_id') as session_id,
                    JSON_EXTRACT_scalar(json_array,'$.session_index') as session_index,
                    JSON_EXTRACT_scalar(json_array,'$.event_index') as event_index,
                    JSON_EXTRACT_scalar(json_array,'$.previous_session_id') as previous_session_id,
                    JSON_EXTRACT_scalar(json_array,'$.storage_mechanism') as storage_mechanism,
                    JSON_EXTRACT_scalar(json_array,'$.first_event_id') as first_event_id,
                    JSON_EXTRACT_scalar(json_array,'$.first_event_timestamp') as first_event_timestamp
    from unnest(contexts_com_snowplowanalytics_snowplow_client_session_1_0_2) as json_array
  ) as contexts_com_snowplowanalytics_snowplow_client_session_1_0_2,
  array(
    select as struct JSON_EXTRACT_scalar(json_array,'$.video_height') as video_height,
                    JSON_EXTRACT_scalar(json_array,'$.video_width') as video_width,
                    JSON_EXTRACT_scalar(json_array,'$.auto_picture_in_picture') as auto_picture_in_picture,
                    JSON_EXTRACT_scalar(json_array,'$.disable_picture_in_picture') as disable_picture_in_picture,
                    JSON_EXTRACT_scalar(json_array,'$.poster') as poster
    from unnest(contexts_org_whatwg_video_element_1_0_0) as json_array
  ) as contexts_org_whatwg_video_element_1_0_0,
  array(
    select as struct JSON_EXTRACT_scalar(json_array,'$.auto_play') as auto_play,
                    JSON_EXTRACT_scalar(json_array,'$.current_src') as current_src,
                    JSON_EXTRACT_scalar(json_array,'$.default_muted') as default_muted,
                    JSON_EXTRACT_scalar(json_array,'$.default_playback_rate') as default_playback_rate,
                    JSON_EXTRACT_scalar(json_array,'$.html_id') as html_id,
                    JSON_EXTRACT_scalar(json_array,'$.media_type') as media_type,
                    JSON_EXTRACT_scalar(json_array,'$.network_state') as network_state,
                    JSON_EXTRACT_scalar(json_array,'$.preload') as preload,
                    JSON_EXTRACT_scalar(json_array,'$.ready_state') as ready_state,
                    JSON_EXTRACT_scalar(json_array,'$.seeking') as seeking,
                    JSON_EXTRACT_scalar(json_array,'$.cross_origin') as cross_origin,
                    JSON_EXTRACT_scalar(json_array,'$.disable_remote_playback') as disable_remote_playback,
                    JSON_EXTRACT_scalar(json_array,'$.error') as error,
                    JSON_EXTRACT_scalar(json_array,'$.file_extension') as file_extension,
                    JSON_EXTRACT_scalar(json_array,'$.fullscreen') as fullscreen,
                    JSON_EXTRACT_scalar(json_array,'$.picture_in_picture') as picture_in_picture,
                    JSON_EXTRACT_scalar(json_array,'$.played') as played,
                    JSON_EXTRACT_scalar(json_array,'$.src') as src,
                    JSON_EXTRACT_scalar(json_array,'$.text_tracks') as text_tracks
    from unnest(contexts_org_whatwg_media_element_1_0_0) as json_array
  ) as contexts_org_whatwg_media_element_1_0_0,
  array(
    select as struct JSON_EXTRACT_scalar(json_array,'$.auto_play') as auto_play,
                    JSON_EXTRACT_scalar(json_array,'$.available_playback_rates') as available_playback_rates,
                    JSON_EXTRACT_scalar(json_array,'$.buffering') as buffering,
                    JSON_EXTRACT_scalar(json_array,'$.controls') as controls,
                    JSON_EXTRACT_scalar(json_array,'$.cued') as cued,
                    JSON_EXTRACT_scalar(json_array,'$.loaded') as loaded,
                    JSON_EXTRACT_scalar(json_array,'$.playback_quality') as playback_quality,
                    JSON_EXTRACT_scalar(json_array,'$.player_id') as player_id,
                    JSON_EXTRACT_scalar(json_array,'$.unstarted') as unstarted,
                    JSON_EXTRACT_scalar(json_array,'$.url') as url,
                    JSON_EXTRACT_scalar(json_array,'$.error') as error,
                    JSON_EXTRACT_scalar(json_array,'$.fov') as fov,
                    JSON_EXTRACT_scalar(json_array,'$.origin') as origin,
                    JSON_EXTRACT_scalar(json_array,'$.pitch') as pitch,
                    JSON_EXTRACT_scalar(json_array,'$.playlist') as playlist,
                    JSON_EXTRACT_scalar(json_array,'$.playlist_index') as playlist_index,
                    JSON_EXTRACT_scalar(json_array,'$.roll') as roll,
                    JSON_EXTRACT_scalar(json_array,'$.yaw') as yaw
    from unnest(contexts_com_youtube_youtube_1_0_0) as json_array
    ) as contexts_com_youtube_youtube_1_0_0,
  array(
    select as struct cast(JSON_EXTRACT_scalar(json_array,'$.current_time') as float64) as current_time,
                    cast(JSON_EXTRACT_scalar(json_array,'$.ended') as boolean) as ended,
                    cast(JSON_EXTRACT_scalar(json_array,'$.loop') as boolean) as loop,
                    cast(JSON_EXTRACT_scalar(json_array,'$.muted') as boolean) as muted,
                    cast(JSON_EXTRACT_scalar(json_array,'$.paused') as boolean) as paused,
                    cast(JSON_EXTRACT_scalar(json_array,'$.playback_rate') as float64) as playback_rate,
                    JSON_EXTRACT_scalar(json_array,'$.volume') as volume,
                    cast(JSON_EXTRACT_scalar(json_array,'$.duration') as float64) as duration,
                    JSON_EXTRACT_scalar(json_array,'$.is_live') as is_live,
                    cast(JSON_EXTRACT_scalar(json_array,'$.percent_progress') as integer) as percent_progress
    from unnest(contexts_com_snowplowanalytics_snowplow_media_player_1_0_0) as json_array
  ) as contexts_com_snowplowanalytics_snowplow_media_player_1_0_0,
  array(
    select as struct cast(JSON_EXTRACT_scalar(json_array,'$.current_time') as float64) as current_time,
                    cast(JSON_EXTRACT_scalar(json_array,'$.duration') as float64) as duration,
                    cast(JSON_EXTRACT_scalar(json_array,'$.ended') as boolean) as ended,
                    cast(JSON_EXTRACT_scalar(json_array,'$.fullscreen') as boolean) as fullscreen,
                    cast(JSON_EXTRACT_scalar(json_array,'$.livestream') as boolean) as livestream,
                    JSON_EXTRACT_scalar(json_array,'$.label') as label,
                    cast(JSON_EXTRACT_scalar(json_array,'$.loop') as boolean) as loop,
                    JSON_EXTRACT_scalar(json_array,'$.media_type') as media_type,
                    cast(JSON_EXTRACT_scalar(json_array,'$.muted') as boolean) as muted,
                    cast(JSON_EXTRACT_scalar(json_array,'$.paused') as boolean) as paused,
                    cast(JSON_EXTRACT_scalar(json_array,'$.picture_in_picture') as boolean) as picture_in_picture,
                    cast(JSON_EXTRACT_scalar(json_array,'$.playback_rate') as float64) as playback_rate,
                    JSON_EXTRACT_scalar(json_array,'$.player_type') as player_type,
                    JSON_EXTRACT_scalar(json_array,'$.quality') as quality,
                    JSON_EXTRACT_scalar(json_array,'$.volume') as volume

    from unnest(contexts_com_snowplowanalytics_snowplow_media_player_2_0_0) as json_array
  ) as contexts_com_snowplowanalytics_snowplow_media_player_2_0_0,
  array(
    select as struct JSON_EXTRACT_scalar(json_array,'$.media_session_id') as media_session_id,
                    JSON_EXTRACT_scalar(json_array,'$.started_at') as started_at,
                    JSON_EXTRACT_scalar(json_array,'$.ping_interval') as ping_interval,
                    cast(JSON_EXTRACT_scalar(json_array,'$.time_played') as float64) as time_played,
                    cast(JSON_EXTRACT_scalar(json_array,'$.time_played_muted') as float64) as time_played_muted,
                    cast(JSON_EXTRACT_scalar(json_array,'$.time_paused') as float64) as time_paused,
                    cast(JSON_EXTRACT_scalar(json_array,'$.content_watched') as float64) as content_watched,
                    cast(JSON_EXTRACT_scalar(json_array,'$.time_buffering') as float64) as time_buffering,
                    cast(JSON_EXTRACT_scalar(json_array,'$.time_spent_ads') as float64) as time_spent_ads,
                    cast(JSON_EXTRACT_scalar(json_array,'$.ads') as integer) as ads,
                    cast(JSON_EXTRACT_scalar(json_array,'$.ads_clicked') as integer) as ads_clicked,
                    cast(JSON_EXTRACT_scalar(json_array,'$.ads_skipped') as integer) as ads_skipped,
                    cast(JSON_EXTRACT_scalar(json_array,'$.ad_breaks') as integer) as ad_breaks,
                    cast(JSON_EXTRACT_scalar(json_array,'$.avg_playback_rate') as float64) as avg_playback_rate

    from unnest(contexts_com_snowplowanalytics_snowplow_media_session_1_0_0) as json_array
  ) as contexts_com_snowplowanalytics_snowplow_media_session_1_0_0,
  array(
    select as struct JSON_EXTRACT_scalar(json_array,'$.name') as name,
                    JSON_EXTRACT_scalar(json_array,'$.ad_id') as ad_id,
                    JSON_EXTRACT_scalar(json_array,'$.creative_id') as creative_id,
                    cast(JSON_EXTRACT_scalar(json_array,'$.pod_position') as integer) as pod_position,
                    cast(JSON_EXTRACT_scalar(json_array,'$.duration') as float64) as duration,
                    cast(JSON_EXTRACT_scalar(json_array,'$.skippable') as boolean) as skippable

    from unnest(contexts_com_snowplowanalytics_snowplow_media_ad_1_0_0) as json_array
  ) as contexts_com_snowplowanalytics_snowplow_media_ad_1_0_0,
  array(
    select as struct JSON_EXTRACT_scalar(json_array,'$.name') as name,
                    JSON_EXTRACT_scalar(json_array,'$.break_id') as break_id,
                    JSON_EXTRACT_scalar(json_array,'$.start_time') as start_time,
                    JSON_EXTRACT_scalar(json_array,'$.break_type') as break_type,
                    JSON_EXTRACT_scalar(json_array,'$.pod_size') as pod_size

    from unnest(contexts_com_snowplowanalytics_snowplow_media_ad_break_1_0_0) as json_array
  ) as contexts_com_snowplowanalytics_snowplow_media_ad_break_1_0_0,

  struct(
    JSON_EXTRACT_scalar(unstruct_event_com_snowplowanalytics_snowplow_media_player_event_1_0_0,'$.type') as type,
    JSON_EXTRACT_scalar(unstruct_event_com_snowplowanalytics_snowplow_media_player_event_1_0_0,'$.label') as label
  ) as unstruct_event_com_snowplowanalytics_snowplow_media_player_event_1_0_0,

  struct(
    cast(JSON_EXTRACT_scalar(unstruct_event_com_snowplowanalytics_snowplow_media_ad_quartile_event_1_0_0,'$.percent_progress') as integer) as percent_progress
  ) as unstruct_event_com_snowplowanalytics_snowplow_media_ad_quartile_event_1_0_0

from prep
