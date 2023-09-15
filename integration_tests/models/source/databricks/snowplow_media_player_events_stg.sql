{#
Copyright (c) 2022-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

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
      unstruct_event_com_snowplowanalytics_snowplow_media_ad_quartile_event_1_0_0,
      sf_contexts_com_snowplowanalytics_snowplow_client_session_1_0_2,
      sf_contexts_org_whatwg_video_element_1_0_0,
      sf_contexts_org_whatwg_media_element_1_0_0,
      sf_contexts_com_youtube_youtube_1_0_0,
      sf_contexts_com_snowplowanalytics_snowplow_media_player_1_0_0,
      sf_contexts_com_snowplowanalytics_snowplow_media_player_2_0_0,
      sf_contexts_com_snowplowanalytics_snowplow_media_session_1_0_0,
      sf_contexts_com_snowplowanalytics_snowplow_media_ad_1_0_0,
      sf_contexts_com_snowplowanalytics_snowplow_media_ad_break_1_0_0,
      sf_unstruct_event_com_snowplowanalytics_snowplow_media_ad_quartile_event_1_0_0
    ),
    from_json(contexts_com_snowplowanalytics_snowplow_web_page_1_0_0, 'array<struct<id:string>>') as contexts_com_snowplowanalytics_snowplow_web_page_1,
    from_json(contexts_com_snowplowanalytics_mobile_screen_1_0_0, 'array<struct<name:string, type:string, id:string, view_controller:string, top_view_controller:string, activity:string, fragment:string>>') as contexts_com_snowplowanalytics_mobile_screen_1,
    from_json(contexts_com_snowplowanalytics_snowplow_client_session_1_0_2, 'array<struct<user_id:string, session_id:string, session_index:integer, event_index:integer, previous_session_id:string, storage_mechanism:string, first_event_id:string, first_event_timestamp:string>>') as contexts_com_snowplowanalytics_snowplow_client_session_1,
    from_json(contexts_org_whatwg_video_element_1_0_0, 'array<struct<video_height:integer, video_width:integer, auto_picture_in_picture:boolean, disable_picture_in_picture:boolean, poster:string >>') as contexts_org_whatwg_video_element_1,
    from_json(contexts_org_whatwg_media_element_1_0_0, 'array<struct<auto_play:boolean, current_src:string, default_muted:boolean, default_playback_rate:double ,html_id:string,media_type:string,network_state:string,preload:string,ready_state:string,seeking:boolean,cross_origin:string,disable_remote_playback:boolean,error:string,file_extension:string,fullscreen:boolean,picture_in_picture:boolean,src:string>>') as contexts_org_whatwg_media_element_1,
    from_json(contexts_com_youtube_youtube_1_0_0, 'array<struct<auto_play:boolean, buffering:boolean, controls:boolean, cued:boolean, loaded:integer, playback_quality:string, player_id:string, unstarted:boolean, url:string, error:string, fov:double, origin:string, pitch:double, playlist_index:double, roll:double, yaw:double>>') as contexts_com_youtube_youtube_1,
    from_json(contexts_com_snowplowanalytics_snowplow_media_player_1_0_0, 'array<struct<current_time:double, ended:boolean, loop:boolean, muted:boolean, paused:boolean, playback_rate:double, volume:integer, duration:double, is_live:boolean, percent_progress:integer>>') as contexts_com_snowplowanalytics_snowplow_media_player_1,
    from_json(contexts_com_snowplowanalytics_snowplow_media_player_2_0_0, 'array<struct<current_time:double, duration:double, ended: boolean, fullscreen:boolean, livestream:boolean, label:string, loop:boolean, media_type:string, muted:boolean, paused:boolean, picture_in_picture:boolean, playback_rate:double, player_type:string, quality:string, volume:integer>>') as contexts_com_snowplowanalytics_snowplow_media_player_2,
    from_json(contexts_com_snowplowanalytics_snowplow_media_session_1_0_0, 'array<struct<media_session_id:string, started_at:string, ping_interval:integer, time_played:double, time_played_muted:double, time_paused:double, content_watched:double, time_buffering:double, time_spent_ads:double, ads:integer, ads_clicked:integer, ads_skipped:integer, ad_breaks:integer, avg_playback_rate:double>>') as contexts_com_snowplowanalytics_snowplow_media_session_1,
    from_json(contexts_com_snowplowanalytics_snowplow_media_ad_1_0_0, 'array<struct<name:string, ad_id:string, creative_id:string, pod_position:integer, duration:double, skippable:boolean>>') as contexts_com_snowplowanalytics_snowplow_media_ad_1,
    from_json(contexts_com_snowplowanalytics_snowplow_media_ad_break_1_0_0, 'array<struct<name:string, break_id:string, start_time:string, break_type:string, pod_size:integer>>') as contexts_com_snowplowanalytics_snowplow_media_ad_break_1,
    from_json(unstruct_event_com_snowplowanalytics_snowplow_media_player_event_1_0_0, 'struct<type:string, label:string>') as unstruct_event_com_snowplowanalytics_snowplow_media_player_event_1,
    from_json(unstruct_event_com_snowplowanalytics_snowplow_media_ad_quartile_event_1_0_0, 'struct<percent_progress:integer>') as unstruct_event_com_snowplowanalytics_snowplow_media_ad_quartile_event_1

  from {{ ref('snowplow_media_player_events') }}

)

  select
    *

  from prep
