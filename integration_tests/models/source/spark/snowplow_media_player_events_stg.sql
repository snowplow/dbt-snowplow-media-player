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
    app_id,
    platform,
    etl_tstamp,
    collector_tstamp,
    dvce_created_tstamp,
    event,
    event_id,
    txn_id,
    name_tracker,
    v_tracker,
    v_collector,
    v_etl,
    user_id,
    user_ipaddress,
    user_fingerprint,
    domain_userid,
    domain_sessionidx,
    network_userid,
    geo_country,
    geo_region,
    geo_city,
    geo_zipcode,
    geo_latitude,
    geo_longitude,
    geo_region_name,
    ip_isp,
    ip_organization,
    ip_domain,
    ip_netspeed,
    page_url,
    page_title,
    page_referrer,
    page_urlscheme,
    page_urlhost,
    page_urlport,
    page_urlpath,
    page_urlquery,
    page_urlfragment,
    refr_urlscheme,
    refr_urlhost,
    refr_urlport,
    refr_urlpath,
    refr_urlquery,
    refr_urlfragment,
    refr_medium,
    refr_source,
    refr_term,
    mkt_medium,
    mkt_source,
    mkt_term,
    mkt_content,
    mkt_campaign,se_category,se_action,se_label,se_property,se_value,tr_orderid,tr_affiliation,tr_total,
    tr_tax,tr_shipping,tr_city,tr_state,tr_country,ti_orderid,ti_sku,ti_name,ti_category,ti_price,ti_quantity,
    pp_xoffset_min,pp_xoffset_max,pp_yoffset_min,pp_yoffset_max,useragent,br_name,br_family,br_version,br_type,
    br_renderengine,br_lang,br_features_pdf,br_features_flash,br_features_java,br_features_director,br_features_quicktime,
    br_features_realplayer,br_features_windowsmedia,br_features_gears,br_features_silverlight,br_cookies,br_colordepth,br_viewwidth,
    br_viewheight,os_name,os_family,os_manufacturer,os_timezone,dvce_type,dvce_ismobile,dvce_screenwidth,dvce_screenheight,doc_charset,
    doc_width,doc_height,tr_currency,tr_total_base,tr_tax_base,tr_shipping_base,ti_currency,ti_price_base,base_currency,geo_timezone,mkt_clickid,
    mkt_network,etl_tags,dvce_sent_tstamp,refr_domain_userid,refr_dvce_tstamp,domain_sessionid,derived_tstamp,event_vendor,event_name,event_format,
    event_version,event_fingerprint,true_tstamp,load_tstamp,

    from_json(contexts_com_snowplowanalytics_snowplow_web_page_1_0_0, 'array<struct<id:string>>') as contexts_com_snowplowanalytics_snowplow_web_page_1_0_0,
    from_json(contexts_com_snowplowanalytics_mobile_screen_1_0_0, 'array<struct<name:string, type:string, id:string, view_controller:string, top_view_controller:string, activity:string, fragment:string>>') as contexts_com_snowplowanalytics_mobile_screen_1_0_0,
    from_json(contexts_com_snowplowanalytics_snowplow_client_session_1_0_2, 'array<struct<user_id:string, session_id:string, session_index:integer, event_index:integer, previous_session_id:string, storage_mechanism:string, first_event_id:string, first_event_timestamp:string>>') as contexts_com_snowplowanalytics_snowplow_client_session_1_0_2,
    from_json(contexts_org_whatwg_video_element_1_0_0, 'array<struct<video_height:integer, video_width:integer, auto_picture_in_picture:boolean, disable_picture_in_picture:boolean, poster:string >>') as contexts_org_whatwg_video_element_1_0_0,
    from_json(contexts_org_whatwg_media_element_1_0_0, 'array<struct<auto_play:boolean, current_src:string, default_muted:boolean, default_playback_rate:double ,html_id:string,media_type:string,network_state:string,preload:string,ready_state:string,seeking:boolean,cross_origin:string,disable_remote_playback:boolean,error:string,file_extension:string,fullscreen:boolean,picture_in_picture:boolean,src:string>>') as contexts_org_whatwg_media_element_1_0_0,
    from_json(contexts_com_youtube_youtube_1_0_0, 'array<struct<auto_play:boolean, buffering:boolean, controls:boolean, cued:boolean, loaded:integer, playback_quality:string, player_id:string, unstarted:boolean, url:string, error:string, fov:double, origin:string, pitch:double, playlist_index:double, roll:double, yaw:double>>') as contexts_com_youtube_youtube_1_0_0,
    from_json(contexts_com_snowplowanalytics_snowplow_media_player_1_0_0, 'array<struct<current_time:double, ended:boolean, loop:boolean, muted:boolean, paused:boolean, playback_rate:double, volume:integer, duration:double, is_live:boolean, percent_progress:integer>>') as contexts_com_snowplowanalytics_snowplow_media_player_1_0_0,
    from_json(contexts_com_snowplowanalytics_snowplow_media_player_2_0_0, 'array<struct<current_time:double, duration:double, ended: boolean, fullscreen:boolean, livestream:boolean, label:string, loop:boolean, media_type:string, muted:boolean, paused:boolean, picture_in_picture:boolean, playback_rate:double, player_type:string, quality:string, volume:integer>>') as contexts_com_snowplowanalytics_snowplow_media_player_2_0_0,
    from_json(contexts_com_snowplowanalytics_snowplow_media_session_1_0_0, 'array<struct<media_session_id:string, started_at:string, ping_interval:integer, time_played:double, time_played_muted:double, time_paused:double, content_watched:double, time_buffering:double, time_spent_ads:double, ads:integer, ads_clicked:integer, ads_skipped:integer, ad_breaks:integer, avg_playback_rate:double>>') as contexts_com_snowplowanalytics_snowplow_media_session_1_0_0,
    from_json(contexts_com_snowplowanalytics_snowplow_media_ad_1_0_0, 'array<struct<name:string, ad_id:string, creative_id:string, pod_position:integer, duration:double, skippable:boolean>>') as contexts_com_snowplowanalytics_snowplow_media_ad_1_0_0,
    from_json(contexts_com_snowplowanalytics_snowplow_media_ad_break_1_0_0, 'array<struct<name:string, break_id:string, start_time:string, break_type:string, pod_size:integer>>') as contexts_com_snowplowanalytics_snowplow_media_ad_break_1_0_0,
    from_json(unstruct_event_com_snowplowanalytics_snowplow_media_player_event_1_0_0, 'struct<type:string, label:string>') as unstruct_event_com_snowplowanalytics_snowplow_media_player_event_1_0_0,
    from_json(unstruct_event_com_snowplowanalytics_snowplow_media_ad_quartile_event_1_0_0, 'struct<percent_progress:integer>') as unstruct_event_com_snowplowanalytics_snowplow_media_ad_quartile_event_1_0_0


  from {{ ref('snowplow_media_player_events') }}

  )

  select
    app_id,
    platform,
    etl_tstamp,
    collector_tstamp,
    dvce_created_tstamp,
    event,
    event_id,
    txn_id,
    name_tracker,
    v_tracker,
    v_collector,
    v_etl,
    user_id,
    user_ipaddress,
    user_fingerprint,
    domain_userid,
    domain_sessionidx,
    network_userid,
    geo_country,
    geo_region,
    geo_city,
    geo_zipcode,
    geo_latitude,
    geo_longitude,
    geo_region_name,
    ip_isp,
    ip_organization,
    ip_domain,
    ip_netspeed,
    page_url,
    page_title,
    page_referrer,
    page_urlscheme,
    page_urlhost,
    page_urlport,
    page_urlpath,
    page_urlquery,
    page_urlfragment,
    refr_urlscheme,
    refr_urlhost,
    refr_urlport,
    refr_urlpath,
    refr_urlquery,
    refr_urlfragment,
    refr_medium,
    refr_source,
    refr_term,
    mkt_medium,
    mkt_source,
    mkt_term,
    mkt_content,
    mkt_campaign,se_category,se_action,se_label,se_property,se_value,tr_orderid,tr_affiliation,tr_total,
    tr_tax,tr_shipping,tr_city,tr_state,tr_country,ti_orderid,ti_sku,ti_name,ti_category,ti_price,ti_quantity,
    pp_xoffset_min,pp_xoffset_max,pp_yoffset_min,pp_yoffset_max,useragent,br_name,br_family,br_version,br_type,
    br_renderengine,br_lang,br_features_pdf,br_features_flash,br_features_java,br_features_director,br_features_quicktime,
    br_features_realplayer,br_features_windowsmedia,br_features_gears,br_features_silverlight,br_cookies,br_colordepth,br_viewwidth,
    br_viewheight,os_name,os_family,os_manufacturer,os_timezone,dvce_type,dvce_ismobile,dvce_screenwidth,dvce_screenheight,doc_charset,
    doc_width,doc_height,tr_currency,tr_total_base,tr_tax_base,tr_shipping_base,ti_currency,ti_price_base,base_currency,geo_timezone,mkt_clickid,
    mkt_network,etl_tags,dvce_sent_tstamp,refr_domain_userid,refr_dvce_tstamp,domain_sessionid,derived_tstamp,event_vendor,event_name,event_format,
    event_version,event_fingerprint,true_tstamp,load_tstamp,


    array(
          struct(
              cast(contexts_com_snowplowanalytics_snowplow_web_page_1_0_0[0].id as STRING) AS id
          )
      ) as contexts_com_snowplowanalytics_snowplow_web_page_1,
      
    array(
        struct(
            cast(contexts_com_snowplowanalytics_mobile_screen_1_0_0[0].name as STRING) AS name,
            cast(contexts_com_snowplowanalytics_mobile_screen_1_0_0[0].type as STRING) AS type,
            cast(contexts_com_snowplowanalytics_mobile_screen_1_0_0[0].id as STRING) AS id,
            cast(contexts_com_snowplowanalytics_mobile_screen_1_0_0[0].view_controller as STRING) AS view_controller,
            cast(contexts_com_snowplowanalytics_mobile_screen_1_0_0[0].top_view_controller as STRING) AS top_view_controller,
            cast(contexts_com_snowplowanalytics_mobile_screen_1_0_0[0].activity as STRING) AS activity,
            cast(contexts_com_snowplowanalytics_mobile_screen_1_0_0[0].fragment as STRING) AS fragment
        )
    ) as contexts_com_snowplowanalytics_mobile_screen_1,

    array(
        struct(
            cast(contexts_com_snowplowanalytics_snowplow_client_session_1_0_2[0].user_id as STRING) AS user_id,
            cast(contexts_com_snowplowanalytics_snowplow_client_session_1_0_2[0].session_id as STRING) AS session_id,
            cast(contexts_com_snowplowanalytics_snowplow_client_session_1_0_2[0].session_index as INT) AS session_index,
            cast(contexts_com_snowplowanalytics_snowplow_client_session_1_0_2[0].event_index as INT) AS event_index,
            cast(contexts_com_snowplowanalytics_snowplow_client_session_1_0_2[0].previous_session_id as STRING) AS previous_session_id,
            cast(contexts_com_snowplowanalytics_snowplow_client_session_1_0_2[0].storage_mechanism as STRING) AS storage_mechanism,
            cast(contexts_com_snowplowanalytics_snowplow_client_session_1_0_2[0].first_event_id as STRING) AS first_event_id,
            cast(contexts_com_snowplowanalytics_snowplow_client_session_1_0_2[0].first_event_timestamp as TIMESTAMP) AS first_event_timestamp
        )
    ) as contexts_com_snowplowanalytics_snowplow_client_session_1,

    array(
        struct(
            cast(contexts_org_whatwg_video_element_1_0_0[0].video_height as INT) AS video_height,
            cast(contexts_org_whatwg_video_element_1_0_0[0].video_width as INT) AS video_width,
            cast(contexts_org_whatwg_video_element_1_0_0[0].auto_picture_in_picture as BOOLEAN) AS auto_picture_in_picture,
            cast(contexts_org_whatwg_video_element_1_0_0[0].disable_picture_in_picture as BOOLEAN) AS disable_picture_in_picture,
            cast(contexts_org_whatwg_video_element_1_0_0[0].poster as STRING) AS poster
        )
    ) as contexts_org_whatwg_video_element_1,

    array(
        struct(
            cast(contexts_org_whatwg_media_element_1_0_0[0].auto_play as BOOLEAN) AS auto_play,
            cast(contexts_org_whatwg_media_element_1_0_0[0].current_src as STRING) AS current_src,
            cast(contexts_org_whatwg_media_element_1_0_0[0].default_muted as BOOLEAN) AS default_muted,
            cast(contexts_org_whatwg_media_element_1_0_0[0].default_playback_rate as DOUBLE) AS default_playback_rate,
            cast(contexts_org_whatwg_media_element_1_0_0[0].html_id as STRING) AS html_id,
            cast(contexts_org_whatwg_media_element_1_0_0[0].media_type as STRING) AS media_type,
            cast(contexts_org_whatwg_media_element_1_0_0[0].network_state as STRING) AS network_state,
            cast(contexts_org_whatwg_media_element_1_0_0[0].preload as STRING) AS preload,
            cast(contexts_org_whatwg_media_element_1_0_0[0].ready_state as STRING) AS ready_state,
            cast(contexts_org_whatwg_media_element_1_0_0[0].seeking as BOOLEAN) AS seeking,
            cast(contexts_org_whatwg_media_element_1_0_0[0].cross_origin as STRING) AS cross_origin,
            cast(contexts_org_whatwg_media_element_1_0_0[0].disable_remote_playback as BOOLEAN) AS disable_remote_playback,
            cast(contexts_org_whatwg_media_element_1_0_0[0].error as STRING) AS error,
            cast(contexts_org_whatwg_media_element_1_0_0[0].file_extension as STRING) AS file_extension,
            cast(contexts_org_whatwg_media_element_1_0_0[0].fullscreen as BOOLEAN) AS fullscreen,
            cast(contexts_org_whatwg_media_element_1_0_0[0].picture_in_picture as BOOLEAN) AS picture_in_picture,
            cast(contexts_org_whatwg_media_element_1_0_0[0].src as STRING) AS src
        )
    ) as contexts_org_whatwg_media_element_1,

    array(
        struct(
            cast(contexts_com_youtube_youtube_1_0_0[0].auto_play as BOOLEAN) AS auto_play,
            cast(contexts_com_youtube_youtube_1_0_0[0].buffering as BOOLEAN) AS buffering,
            cast(contexts_com_youtube_youtube_1_0_0[0].controls as BOOLEAN) AS controls,
            cast(contexts_com_youtube_youtube_1_0_0[0].cued as BOOLEAN) AS cued,
            cast(contexts_com_youtube_youtube_1_0_0[0].loaded as INT) AS loaded,
            cast(contexts_com_youtube_youtube_1_0_0[0].playback_quality as STRING) AS playback_quality,
            cast(contexts_com_youtube_youtube_1_0_0[0].player_id as STRING) AS player_id,
            cast(contexts_com_youtube_youtube_1_0_0[0].unstarted as BOOLEAN) AS unstarted,
            cast(contexts_com_youtube_youtube_1_0_0[0].url as STRING) AS url,
            cast(contexts_com_youtube_youtube_1_0_0[0].error as STRING) AS error,
            cast(contexts_com_youtube_youtube_1_0_0[0].fov as DOUBLE) AS fov,
            cast(contexts_com_youtube_youtube_1_0_0[0].origin as STRING) AS origin,
            cast(contexts_com_youtube_youtube_1_0_0[0].pitch as DOUBLE) AS pitch,
            cast(contexts_com_youtube_youtube_1_0_0[0].playlist_index as DOUBLE) AS playlist_index,
            cast(contexts_com_youtube_youtube_1_0_0[0].roll as DOUBLE) AS roll,
            cast(contexts_com_youtube_youtube_1_0_0[0].yaw as DOUBLE) AS yaw
        )
    ) as contexts_com_youtube_youtube_1,

    array(
        struct(
            cast(contexts_com_snowplowanalytics_snowplow_media_player_1_0_0[0].current_time as DOUBLE) AS current_time,
            cast(contexts_com_snowplowanalytics_snowplow_media_player_1_0_0[0].ended as BOOLEAN) AS ended,
            cast(contexts_com_snowplowanalytics_snowplow_media_player_1_0_0[0].loop as BOOLEAN) AS loop,
            cast(contexts_com_snowplowanalytics_snowplow_media_player_1_0_0[0].muted as BOOLEAN) AS muted,
            cast(contexts_com_snowplowanalytics_snowplow_media_player_1_0_0[0].paused as BOOLEAN) AS paused,
            cast(contexts_com_snowplowanalytics_snowplow_media_player_1_0_0[0].playback_rate as DOUBLE) AS playback_rate,
            cast(contexts_com_snowplowanalytics_snowplow_media_player_1_0_0[0].volume as INT) AS volume,
            cast(contexts_com_snowplowanalytics_snowplow_media_player_1_0_0[0].duration as DOUBLE) AS duration,
            cast(contexts_com_snowplowanalytics_snowplow_media_player_1_0_0[0].is_live as BOOLEAN) AS is_live,
            cast(contexts_com_snowplowanalytics_snowplow_media_player_1_0_0[0].percent_progress as INT) AS percent_progress
        )
    ) as contexts_com_snowplowanalytics_snowplow_media_player_1,


    array(
    struct(
        cast(contexts_com_snowplowanalytics_snowplow_media_player_2_0_0[0].current_time as DOUBLE) AS current_time,
        cast(contexts_com_snowplowanalytics_snowplow_media_player_2_0_0[0].duration as DOUBLE) AS duration,
        cast(contexts_com_snowplowanalytics_snowplow_media_player_2_0_0[0].ended as BOOLEAN) AS ended,
        cast(contexts_com_snowplowanalytics_snowplow_media_player_2_0_0[0].fullscreen as BOOLEAN) AS fullscreen,
        cast(contexts_com_snowplowanalytics_snowplow_media_player_2_0_0[0].livestream as BOOLEAN) AS livestream,
        cast(contexts_com_snowplowanalytics_snowplow_media_player_2_0_0[0].label as STRING) AS label,
        cast(contexts_com_snowplowanalytics_snowplow_media_player_2_0_0[0].loop as BOOLEAN) AS loop,
        cast(contexts_com_snowplowanalytics_snowplow_media_player_2_0_0[0].media_type as STRING) AS media_type,
        cast(contexts_com_snowplowanalytics_snowplow_media_player_2_0_0[0].muted as BOOLEAN) AS muted,
        cast(contexts_com_snowplowanalytics_snowplow_media_player_2_0_0[0].paused as BOOLEAN) AS paused,
        cast(contexts_com_snowplowanalytics_snowplow_media_player_2_0_0[0].picture_in_picture as BOOLEAN) AS picture_in_picture,
        cast(contexts_com_snowplowanalytics_snowplow_media_player_2_0_0[0].playback_rate as DOUBLE) AS playback_rate,
        cast(contexts_com_snowplowanalytics_snowplow_media_player_2_0_0[0].player_type as STRING) AS player_type,
        cast(contexts_com_snowplowanalytics_snowplow_media_player_2_0_0[0].quality as STRING) AS quality,
        cast(contexts_com_snowplowanalytics_snowplow_media_player_2_0_0[0].volume as INT) AS volume
    )
) as contexts_com_snowplowanalytics_snowplow_media_player_2,

    array(
        struct(
            cast(contexts_com_snowplowanalytics_snowplow_media_session_1_0_0[0].media_session_id as STRING) AS media_session_id,
            cast(contexts_com_snowplowanalytics_snowplow_media_session_1_0_0[0].started_at as STRING) AS started_at,
            cast(contexts_com_snowplowanalytics_snowplow_media_session_1_0_0[0].ping_interval as INT) AS ping_interval,
            cast(contexts_com_snowplowanalytics_snowplow_media_session_1_0_0[0].time_played as DOUBLE) AS time_played,
            cast(contexts_com_snowplowanalytics_snowplow_media_session_1_0_0[0].time_played_muted as DOUBLE) AS time_played_muted,
            cast(contexts_com_snowplowanalytics_snowplow_media_session_1_0_0[0].time_paused as DOUBLE) AS time_paused,
            cast(contexts_com_snowplowanalytics_snowplow_media_session_1_0_0[0].content_watched as DOUBLE) AS content_watched,
            cast(contexts_com_snowplowanalytics_snowplow_media_session_1_0_0[0].time_buffering as DOUBLE) AS time_buffering,
            cast(contexts_com_snowplowanalytics_snowplow_media_session_1_0_0[0].time_spent_ads as DOUBLE) AS time_spent_ads,
            cast(contexts_com_snowplowanalytics_snowplow_media_session_1_0_0[0].ads as INT) AS ads,
            cast(contexts_com_snowplowanalytics_snowplow_media_session_1_0_0[0].ads_clicked as INT) AS ads_clicked,
            cast(contexts_com_snowplowanalytics_snowplow_media_session_1_0_0[0].ads_skipped as INT) AS ads_skipped,
            cast(contexts_com_snowplowanalytics_snowplow_media_session_1_0_0[0].ad_breaks as INT) AS ad_breaks,
            cast(contexts_com_snowplowanalytics_snowplow_media_session_1_0_0[0].avg_playback_rate as DOUBLE) AS avg_playback_rate
        )
    ) as contexts_com_snowplowanalytics_snowplow_media_session_1,

    array(
        struct(
            cast(contexts_com_snowplowanalytics_snowplow_media_ad_1_0_0[0].name as STRING) AS name,
            cast(contexts_com_snowplowanalytics_snowplow_media_ad_1_0_0[0].ad_id as STRING) AS ad_id,
            cast(contexts_com_snowplowanalytics_snowplow_media_ad_1_0_0[0].creative_id as STRING) AS creative_id,
            cast(contexts_com_snowplowanalytics_snowplow_media_ad_1_0_0[0].pod_position as INT) AS pod_position,
            cast(contexts_com_snowplowanalytics_snowplow_media_ad_1_0_0[0].duration as DOUBLE) AS duration,
            cast(contexts_com_snowplowanalytics_snowplow_media_ad_1_0_0[0].skippable as BOOLEAN) AS skippable
        )
    ) as contexts_com_snowplowanalytics_snowplow_media_ad_1,

    array(
        struct(
            cast(contexts_com_snowplowanalytics_snowplow_media_ad_break_1_0_0[0].name as STRING) AS name,
            cast(contexts_com_snowplowanalytics_snowplow_media_ad_break_1_0_0[0].break_id as STRING) AS break_id,
            cast(contexts_com_snowplowanalytics_snowplow_media_ad_break_1_0_0[0].start_time as STRING) AS start_time,
            cast(contexts_com_snowplowanalytics_snowplow_media_ad_break_1_0_0[0].break_type as STRING) AS break_type,
            cast(contexts_com_snowplowanalytics_snowplow_media_ad_break_1_0_0[0].pod_size as INT) AS pod_size
        )
    ) as contexts_com_snowplowanalytics_snowplow_media_ad_break_1,

    struct(
        cast(unstruct_event_com_snowplowanalytics_snowplow_media_player_event_1_0_0.type as STRING) AS type,
        cast(unstruct_event_com_snowplowanalytics_snowplow_media_player_event_1_0_0.label as STRING) AS label
    ) as unstruct_event_com_snowplowanalytics_snowplow_media_player_event_1,

    struct(
        cast(unstruct_event_com_snowplowanalytics_snowplow_media_ad_quartile_event_1_0_0.percent_progress as INT) AS percent_progress
    ) as unstruct_event_com_snowplowanalytics_snowplow_media_ad_quartile_event_1

  from prep
