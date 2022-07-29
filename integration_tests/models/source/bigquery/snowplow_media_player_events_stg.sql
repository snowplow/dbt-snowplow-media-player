{{ config(
  materialized='table',
  )
}}

-- page view context is given as json string in csv. Extract array from json
with prep as (
select
  *
  except(contexts_com_snowplowanalytics_snowplow_web_page_1_0_0,
        contexts_org_whatwg_video_element_1_0_0,
        contexts_org_whatwg_media_element_1_0_0,
        contexts_com_youtube_youtube_1_0_0,
        contexts_com_snowplowanalytics_snowplow_media_player_1_0_0,
        unstruct_event_com_snowplowanalytics_snowplow_media_player_event_1_0_0),
  JSON_EXTRACT_ARRAY(contexts_com_snowplowanalytics_snowplow_web_page_1_0_0) AS contexts_com_snowplowanalytics_snowplow_web_page_1_0_0,
  JSON_EXTRACT_ARRAY(contexts_org_whatwg_video_element_1_0_0) AS contexts_org_whatwg_video_element_1_0_0,
  JSON_EXTRACT_ARRAY(contexts_org_whatwg_media_element_1_0_0) AS contexts_org_whatwg_media_element_1_0_0,
  JSON_EXTRACT_ARRAY(contexts_com_youtube_youtube_1_0_0) AS contexts_com_youtube_youtube_1_0_0,
  JSON_EXTRACT_ARRAY(contexts_com_snowplowanalytics_snowplow_media_player_1_0_0) AS contexts_com_snowplowanalytics_snowplow_media_player_1_0_0,
  JSON_EXTRACT_ARRAY(unstruct_event_com_snowplowanalytics_snowplow_media_player_event_1_0_0) AS unstruct_event_com_snowplowanalytics_snowplow_media_player_event_1_0_0

from {{ ref('snowplow_media_player_events') }}
)

-- recreate repeated record field i.e. array of structs as is originally in BQ events table
select
  *
  except(contexts_com_snowplowanalytics_snowplow_web_page_1_0_0,
      contexts_org_whatwg_video_element_1_0_0,
      contexts_org_whatwg_media_element_1_0_0,
      contexts_com_youtube_youtube_1_0_0,
      contexts_com_snowplowanalytics_snowplow_media_player_1_0_0,
      unstruct_event_com_snowplowanalytics_snowplow_media_player_event_1_0_0),
  array(
    select as struct JSON_EXTRACT_scalar(json_array,'$.id') as id
    from unnest(contexts_com_snowplowanalytics_snowplow_web_page_1_0_0) as json_array
    ) as contexts_com_snowplowanalytics_snowplow_web_page_1_0_0,
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
    select as struct JSON_EXTRACT_scalar(json_array,'$.current_time') as current_time,
                    JSON_EXTRACT_scalar(json_array,'$.ended') as ended,
                    JSON_EXTRACT_scalar(json_array,'$.loop') as loop,
                    JSON_EXTRACT_scalar(json_array,'$.muted') as muted,
                    JSON_EXTRACT_scalar(json_array,'$.paused') as paused,
                    JSON_EXTRACT_scalar(json_array,'$.playback_rate') as playback_rate,
                    JSON_EXTRACT_scalar(json_array,'$.volume') as volume,
                    cast(JSON_EXTRACT_scalar(json_array,'$.duration') as float64) as duration,
                    JSON_EXTRACT_scalar(json_array,'$.is_live') as is_live,
                    JSON_EXTRACT_scalar(json_array,'$.percent_progress') as percent_progress
    from unnest(contexts_com_snowplowanalytics_snowplow_media_player_1_0_0) as json_array
    ) as contexts_com_snowplowanalytics_snowplow_media_player_1_0_0,
  array(
    select as struct JSON_EXTRACT_scalar(json_array,'$.type') as type,
                    JSON_EXTRACT_scalar(json_array,'$.label') as label
    from unnest(unstruct_event_com_snowplowanalytics_snowplow_media_player_event_1_0_0) as json_array
    ) as unstruct_event_com_snowplowanalytics_snowplow_media_player_event_1_0_0

from prep
