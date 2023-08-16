with prep as (
  select
    * exclude (
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
    parse_json(ev.contexts_com_snowplowanalytics_snowplow_web_page_1_0_0) as contexts_com_snowplowanalytics_snowplow_web_page_1,
    parse_json(ev.contexts_com_snowplowanalytics_mobile_screen_1_0_0) as contexts_com_snowplowanalytics_mobile_screen_1,
    parse_json(ev.sf_contexts_com_snowplowanalytics_snowplow_client_session_1_0_2) as contexts_com_snowplowanalytics_snowplow_client_session_1,
    parse_json(ev.sf_contexts_org_whatwg_video_element_1_0_0) as contexts_org_whatwg_video_element_1,
    parse_json(ev.sf_contexts_org_whatwg_media_element_1_0_0) as contexts_org_whatwg_media_element_1,
    parse_json(ev.sf_contexts_com_youtube_youtube_1_0_0) as contexts_com_youtube_youtube_1,
    parse_json(ev.sf_contexts_com_snowplowanalytics_snowplow_media_player_1_0_0) as contexts_com_snowplowanalytics_snowplow_media_player_1,
    parse_json(ev.sf_contexts_com_snowplowanalytics_snowplow_media_player_2_0_0) as contexts_com_snowplowanalytics_snowplow_media_player_2,
    parse_json(ev.sf_contexts_com_snowplowanalytics_snowplow_media_session_1_0_0) as contexts_com_snowplowanalytics_snowplow_media_session_1,
    parse_json(ev.sf_contexts_com_snowplowanalytics_snowplow_media_ad_1_0_0) as contexts_com_snowplowanalytics_snowplow_media_ad_1,
    parse_json(ev.sf_contexts_com_snowplowanalytics_snowplow_media_ad_break_1_0_0) as contexts_com_snowplowanalytics_snowplow_media_ad_break_1,
    parse_json(ev.unstruct_event_com_snowplowanalytics_snowplow_media_player_event_1_0_0) as unstruct_event_com_snowplowanalytics_snowplow_media_player_event_1,
    parse_json(ev.sf_unstruct_event_com_snowplowanalytics_snowplow_media_ad_quartile_event_1_0_0) as unstruct_event_com_snowplowanalytics_snowplow_media_ad_quartile_event_1

  from {{ ref('snowplow_media_player_events') }} as ev
)

select
  *

from prep
