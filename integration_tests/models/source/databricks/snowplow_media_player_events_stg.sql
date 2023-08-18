{{
  config(
    materialized='table',
  )
}}

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
    from_json(contexts_com_snowplowanalytics_snowplow_web_page_1_0_0, 'array<struct<id:string>>') as contexts_com_snowplowanalytics_snowplow_web_page_1,
    from_json(contexts_com_snowplowanalytics_mobile_screen_1_0_0, 'array<struct<name:string, type:string, id:string, view_controller:string, top_view_controller:string, activity:string, fragment:string>>') as contexts_com_snowplowanalytics_mobile_screen_1,
    from_json(contexts_com_snowplowanalytics_snowplow_client_session_1_0_2, 'array<struct<user_id:string, session_id:string, session_index:string, event_index:string, previous_session_id:string, storage_mechanism:string, first_event_id:string, first_event_timestamp:string>>') as contexts_com_snowplowanalytics_snowplow_client_session_1,
    from_json(contexts_org_whatwg_video_element_1_0_0, 'array<struct<video_height:string, video_width:string, auto_picture_in_picture:string, disable_picture_in_picture:string, poster:string >>') as contexts_org_whatwg_video_element_1,
    from_json(contexts_org_whatwg_media_element_1_0_0, 'array<struct<auto_play:string, current_src:string, default_muted:string, default_playback_rate:string ,html_id:string,media_type:string,network_state:string,preload:string,ready_state:string,seeking:string,cross_origin:string,disable_remote_playback:string,error:string,file_extension:string,fullscreen:string,picture_in_picture:string,played:string,src:string,text_tracks:string>>') as contexts_org_whatwg_media_element_1,
    from_json(contexts_com_youtube_youtube_1_0_0, 'array<struct<auto_play:string, available_playback_rates:string, buffering:string, controls:string, cued:string, loaded:string, playback_quality:string, player_id:string, unstarted:string, url:string, error:string, fov:string, origin:string, pitch:string, playlist:string, playlist_index:string, roll:string, yaw:string>>') as contexts_com_youtube_youtube_1,
    from_json(contexts_com_snowplowanalytics_snowplow_media_player_1_0_0, 'array<struct<current_time:string, ended:string, loop:string, muted:string, paused:string, playback_rate:string, volume:string, duration:string, is_live:string, percent_progress:string>>') as contexts_com_snowplowanalytics_snowplow_media_player_1,
    from_json(contexts_com_snowplowanalytics_snowplow_media_player_2_0_0, 'array<struct<current_time:string, duration:string, ended: string, fullscreen: string, livestream: string, label:string, loop:string, media_type:string, muted:string, paused:string, picture_in_picture:string, playback_rate:string, player_type:string, quality:string, volume:string>>') as contexts_com_snowplowanalytics_snowplow_media_player_2,
    from_json(contexts_com_snowplowanalytics_snowplow_media_session_1_0_0, 'array<struct<media_session_id:string, started_at:string, ping_interval:string, time_played:string, time_played_muted:string, time_paused:string, content_watched:string, time_buffering:string, time_spent_ads:string, ads:string, ads_clicked:string, ads_skipped:string, ad_breaks:string, avg_playback_rate:string>>') as contexts_com_snowplowanalytics_snowplow_media_session_1,
    from_json(contexts_com_snowplowanalytics_snowplow_media_ad_1_0_0, 'array<struct<name:string, ad_id:string, creative_id:string, pod_position:string, duration:string, skippable:string>>') as contexts_com_snowplowanalytics_snowplow_media_ad_1,
    from_json(contexts_com_snowplowanalytics_snowplow_media_ad_break_1_0_0, 'array<struct<name:string, break_id:string, start_time:string, break_type:string, pod_size:string>>') as contexts_com_snowplowanalytics_snowplow_media_ad_break_1,
    from_json(unstruct_event_com_snowplowanalytics_snowplow_media_player_event_1_0_0, 'struct<type:string, label:string>') as unstruct_event_com_snowplowanalytics_snowplow_media_player_event_1,
    from_json(unstruct_event_com_snowplowanalytics_snowplow_media_ad_quartile_event_1_0_0, 'struct<percent_progress:string>') as unstruct_event_com_snowplowanalytics_snowplow_media_ad_quartile_event_1

  from {{ ref('snowplow_media_player_events') }}

)

  select
    *

  from prep
