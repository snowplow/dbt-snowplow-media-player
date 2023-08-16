{{
    config(
        sort='collector_tstamp',
        dist='event_id',
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

with

{% if var("snowplow__enable_mobile_events") %}
-- unpacking the screen context entity
{{ snowplow_utils.get_sde_or_context(var('snowplow__atomic_schema', 'atomic'), var('snowplow__context_screen'), lower_limit, upper_limit) }},
-- unpacking the client session context entity
{{ snowplow_utils.get_sde_or_context(var('snowplow__atomic_schema', 'atomic'), var('snowplow__context_mobile_session'), lower_limit, upper_limit) }},
{% endif %}

/* Dedupe logic: Per dupe event_id keep earliest row ordered by collector_tstamp.
   If multiple earliest rows, take arbitrary one using row_number(). */

events_this_run AS (
    select
        a.app_id,
        a.platform,
        a.etl_tstamp,
        a.collector_tstamp,
        a.dvce_created_tstamp,
        a.event,
        a.event_id,
        a.txn_id,
        a.name_tracker,
        a.v_tracker,
        a.v_collector,
        a.v_etl,
        a.user_id,
        a.user_ipaddress,
        a.user_fingerprint,
        b.domain_userid, -- take domain_userid from manifest. This ensures only 1 domain_userid per session.
        a.network_userid,
        a.geo_country,
        a.geo_region,
        a.geo_city,
        a.geo_zipcode,
        a.geo_latitude,
        a.geo_longitude,
        a.geo_region_name,
        a.ip_isp,
        a.ip_organization,
        a.ip_domain,
        a.ip_netspeed,
        a.page_url,
        a.page_title,
        a.page_referrer,
        a.page_urlscheme,
        a.page_urlhost,
        a.page_urlport,
        a.page_urlpath,
        a.page_urlquery,
        a.page_urlfragment,
        a.refr_urlscheme,
        a.refr_urlhost,
        a.refr_urlport,
        a.refr_urlpath,
        a.refr_urlquery,
        a.refr_urlfragment,
        a.refr_medium,
        a.refr_source,
        a.refr_term,
        a.mkt_medium,
        a.mkt_source,
        a.mkt_term,
        a.mkt_content,
        a.mkt_campaign,
        a.se_category,
        a.se_action,
        a.se_label,
        a.se_property,
        a.se_value,
        a.tr_orderid,
        a.tr_affiliation,
        a.tr_total,
        a.tr_tax,
        a.tr_shipping,
        a.tr_city,
        a.tr_state,
        a.tr_country,
        a.ti_orderid,
        a.ti_sku,
        a.ti_name,
        a.ti_category,
        a.ti_price,
        a.ti_quantity,
        a.pp_xoffset_min,
        a.pp_xoffset_max,
        a.pp_yoffset_min,
        a.pp_yoffset_max,
        a.useragent,
        a.br_name,
        a.br_family,
        a.br_version,
        a.br_type,
        a.br_renderengine,
        a.br_lang,
        a.br_features_pdf,
        a.br_features_flash,
        a.br_features_java,
        a.br_features_director,
        a.br_features_quicktime,
        a.br_features_realplayer,
        a.br_features_windowsmedia,
        a.br_features_gears,
        a.br_features_silverlight,
        a.br_cookies,
        a.br_colordepth,
        a.br_viewwidth,
        a.br_viewheight,
        a.os_name,
        a.os_family,
        a.os_manufacturer,
        a.os_timezone,
        a.dvce_type,
        a.dvce_ismobile,
        a.dvce_screenwidth,
        a.dvce_screenheight,
        a.doc_charset,
        a.doc_width,
        a.doc_height,
        a.tr_currency,
        a.tr_total_base,
        a.tr_tax_base,
        a.tr_shipping_base,
        a.ti_currency,
        a.ti_price_base,
        a.base_currency,
        a.geo_timezone,
        a.mkt_clickid,
        a.mkt_network,
        a.etl_tags,
        a.dvce_sent_tstamp,
        a.refr_domain_userid,
        a.refr_dvce_tstamp,
        a.domain_sessionid,
        a.derived_tstamp,
        a.event_vendor,
        a.event_name,
        a.event_format,
        a.event_version,
        a.event_fingerprint,
        a.true_tstamp,
        {% if var('snowplow__enable_load_tstamp', true) %}
            a.load_tstamp,
        {% endif %}
        row_number() over (partition by a.event_id order by a.collector_tstamp) as event_id_dedupe_index,
        count(*) over (partition by a.event_id) as event_id_dedupe_count

    from {{ var('snowplow__events') }} as a
    {% if var('snowplow__enable_mobile_events', false) -%}
        left join {{ var('snowplow__context_mobile_session') }} cs on a.event_id = cs.client_session__id and a.collector_tstamp = cs.client_session__tstamp
    {%- endif %}
        inner join {{ ref('snowplow_media_player_base_sessions_this_run') }} as b
            on
        {% if var('snowplow__enable_mobile_events', false) %}
            coalesce(
                cs.session_id,
                a.domain_sessionid
            )
        {% else %}
            a.domain_sessionid
        {% endif %} = b.session_id

    where a.collector_tstamp <= {{ snowplow_utils.timestamp_add('day', var("snowplow__max_session_days", 3), 'b.start_tstamp') }}
        and a.dvce_sent_tstamp <= {{ snowplow_utils.timestamp_add('day', var("snowplow__days_late_allowed", 3), 'a.dvce_created_tstamp') }}
        and a.collector_tstamp >= {{ lower_limit }}
        and a.collector_tstamp <= {{ upper_limit }}
        and {{ snowplow_utils.app_id_filter(var("snowplow__app_id",[])) }}
        and {{ snowplow_media_player.event_name_filter(var("snowplow__media_event_names", "['media_player_event']")) }}

),

{% if var("snowplow__enable_media_player_v1") %}
-- unpacking the media player event
{{ snowplow_utils.get_sde_or_context(var('snowplow__atomic_schema', 'atomic'), var('snowplow__media_player_event_context'), lower_limit, upper_limit) }},
-- unpacking the media player context entity
{{ snowplow_utils.get_sde_or_context(var('snowplow__atomic_schema', 'atomic'), var('snowplow__media_player_context'), lower_limit, upper_limit) }},
{% endif %}
{% if var("snowplow__enable_media_player_v2") %}
-- unpacking the media player context entity v2
{{ snowplow_utils.get_sde_or_context(var('snowplow__atomic_schema', 'atomic'), var('snowplow__media_player_v2_context'), lower_limit, upper_limit) }},
{% endif %}
{% if var("snowplow__enable_media_session") %}
-- unpacking the media session context entity
{{ snowplow_utils.get_sde_or_context(var('snowplow__atomic_schema', 'atomic'), var('snowplow__media_session_context'), lower_limit, upper_limit) }},
{% endif %}
{% if var("snowplow__enable_media_ad") %}
-- unpacking the media ad context entity
{{ snowplow_utils.get_sde_or_context(var('snowplow__atomic_schema', 'atomic'), var('snowplow__media_ad_context'), lower_limit, upper_limit) }},
{% endif %}
{% if var("snowplow__enable_media_ad_break") %}
-- unpacking the media ad break context entity
{{ snowplow_utils.get_sde_or_context(var('snowplow__atomic_schema', 'atomic'), var('snowplow__media_ad_break_context'), lower_limit, upper_limit) }},
{% endif %}
-- unpacking the youtube context entity
{%- if var("snowplow__enable_youtube") -%}
    {{ snowplow_utils.get_sde_or_context(var('snowplow__atomic_schema', 'atomic'), var('snowplow__youtube_context'), lower_limit, upper_limit) }},
{%- endif %}
-- unpacking the whatwg media context entity
{% if var("snowplow__enable_whatwg_media") -%}
    {{ snowplow_utils.get_sde_or_context(var('snowplow__atomic_schema', 'atomic'), var('snowplow__html5_media_element_context'), lower_limit, upper_limit) }},
{%- endif %}
-- unpacking the whatwg video context entity
{% if var("snowplow__enable_whatwg_video") -%}
    {{ snowplow_utils.get_sde_or_context(var('snowplow__atomic_schema', 'atomic'), var('snowplow__html5_video_element_context'), lower_limit, upper_limit) }},
{%- endif %}
{% if var("snowplow__enable_web_events") %}
-- unpacking the web page context entity
{{ snowplow_utils.get_sde_or_context(var('snowplow__atomic_schema', 'atomic'), var('snowplow__context_web_page'), lower_limit, upper_limit) }},
{% endif %}
{% if var("snowplow__enable_ad_quartile_event") %}
-- unpacking the ad quartile event
{{ snowplow_utils.get_sde_or_context(var('snowplow__atomic_schema', 'atomic'), var('snowplow__media_ad_quartile_event'), lower_limit, upper_limit) }},
{% endif %}

prep as (
  select
    ev.event_id,
    {{ web_or_mobile_col(web_property='pv.id', mobile_property='sv.id') }} as page_view_id,
    ev.domain_sessionid, -- this is coalesced web or mobile session id
    ev.domain_userid, -- this is coalesced web or mobile user id
    ev.user_id,
    ev.platform,
    ev.page_referrer,
    ev.page_url,
    ev.geo_region_name,
    ev.br_name,
    ev.dvce_type,
    ev.os_name,
    ev.os_timezone,
    ev.derived_tstamp as start_tstamp,
    ev.collector_tstamp,

    -- unpacking the media player event
    {{ media_label_col(v1_event_label='mpe.label', v2_player_label='mp2.label') }},
    {{ media_event_type_col(media_player_event_type='mpe.type', event_name='ev.event_name') }},

    -- unpacking the media player object
    round({{ media_player_property_col(v1_property='mp.duration', v2_property='mp2.duration') }}) as duration,
    {{ media_player_property_col(v1_property='mp.current_time', v2_property='mp2.current_time') }} as current_time,
    {{ media_player_property_col(v1_property='mp.playback_rate', v2_property='mp2.playback_rate') }} as playback_rate,
    {{ percent_progress_col(
        v1_percent_progress='mp.percent_progress',
        v1_event_type='mpe.type',
        event_name='ev.event_name',
        v2_current_time='mp2.current_time',
        v2_duration='mp2.duration'
    ) }} as percent_progress,
    {{ media_player_property_col(v1_property='mp.muted', v2_property='mp2.muted') }} as is_muted,
    {{ media_player_property_col(v1_property='mp.is_live', v2_property='mp2.livestream') }} as is_live,
    {{ media_player_property_col(v1_property='mp.loop', v2_property='mp2.loop') }} as loop,
    {{ media_player_property_col(v1_property='mp.volume', v2_property='mp2.volume') }} as volume,
    {{ media_player_property_col(v1_property=none, v2_property='mp2.picture_in_picture') }} as picture_in_picture,
    {{ media_player_property_col(v1_property=none, v2_property='mp2.fullscreen') }} as fullscreen,

    -- media session properties
    {{ media_session_property_col(property='ms.media_session_id') }} as media_session_id,
    {{ media_session_property_col(property='ms.started_at') }} as media_session_started_at,
    {{ media_session_property_col(property='ms.time_played') }} as media_session_time_played,
    {{ media_session_property_col(property='ms.time_played_muted') }} as media_session_time_played_muted,
    {{ media_session_property_col(property='ms.time_paused') }} as media_session_time_paused,
    {{ media_session_property_col(property='ms.content_watched') }} as media_session_content_watched,
    {{ media_session_property_col(property='ms.time_buffering') }} as media_session_time_buffering,
    {{ media_session_property_col(property='ms.time_spent_ads') }} as media_session_time_spent_ads,
    {{ media_session_property_col(property='ms.ads') }} as media_session_ads,
    {{ media_session_property_col(property='ms.ads_clicked') }} as media_session_ads_clicked,
    {{ media_session_property_col(property='ms.ads_skipped') }} as media_session_ads_skipped,
    {{ media_session_property_col(property='ms.ad_breaks') }} as media_session_ad_breaks,
    {{ media_session_property_col(property='ms.avg_playback_rate') }} as media_session_avg_playback_rate,

    -- ad properties
    {{ media_ad_property_col(property='ma.name') }} as ad_name,
    {{ media_ad_property_col(property='ma.ad_id') }} as ad_id,
    {{ media_ad_property_col(property='ma.creative_id') }} as ad_creative_id,
    {{ media_ad_property_col(property='ma.pod_position') }} as ad_pod_position,
    {{ media_ad_property_col(property='ma.duration') }} as ad_duration,
    {{ media_ad_property_col(property='ma.skippable') }} as ad_skippable,

    -- ad break properties
    {{ media_ad_break_property_col(property='mb.name') }} as ad_break_name,
    {{ media_ad_break_property_col(property='mb.break_id') }} as ad_break_id,
    {{ media_ad_break_property_col(property='mb.start_time') }} as ad_break_start_time,
    {{ media_ad_break_property_col(property='mb.break_type') }} as ad_break_type,
    {{ media_ad_break_property_col(property='mb.pod_size') }} as ad_break_pod_size,

    -- ad quartile event
    {{ media_ad_quartile_event_property_col(property='aq.percent_progress') }} as ad_percent_progress,

    -- combined media properties
    {{ media_id_col(v2_player_label='mp2.label', youtube_player_id='yt.player_id', media_player_id='me.html_id') }},
    {{ media_player_type_col(v2_player_type='mp2.player_type', youtube_player_id='yt.player_id', media_player_id='me.html_id') }},
    {{ source_url_col(youtube_url='yt.url', media_current_src='me.current_src')}},
    {{ media_type_col(v2_media_type='mp2.media_type', media_media_type='me.media_type')}},
    {{ playback_quality_col(
        v2_quality='mp2.quality',
        youtube_quality='yt.playback_quality',
        video_width='ve.video_width',
        video_height='ve.video_height'
    )}},

    dense_rank() over (partition by domain_sessionid order by derived_tstamp) AS event_in_session_index

    from events_this_run ev


    -- youtube context entity
    {% if var("snowplow__enable_youtube") %}
        left join {{ var('snowplow__youtube_context') }} yt on ev.event_id = yt.youtube__id and ev.collector_tstamp = yt.youtube__tstamp
    {%- endif %}
    -- whatwg media context entity
    {% if var("snowplow__enable_whatwg_media") %}
    left join {{ var('snowplow__html5_media_element_context') }} me on ev.event_id = me.media_element__id and ev.collector_tstamp = me.media_element__tstamp
    {%- endif %}
    -- whatwg video context entity
    {% if var("snowplow__enable_whatwg_video") %}
    left join {{ var('snowplow__html5_video_element_context') }} ve on ev.event_id = ve.video_element__id and ev.collector_tstamp = ve.video_element__tstamp
    {%- endif %}
    {% if var("snowplow__enable_media_player_v1") %}
    -- media player event
    left join {{ var('snowplow__media_player_event_context') }} mpe on ev.event_id = mpe.media_player_event__id and ev.collector_tstamp = mpe.media_player_event__tstamp
    -- media player context entity
    left join {{ var('snowplow__media_player_context') }} mp on ev.event_id = mp.media_player__id and ev.collector_tstamp = mp.media_player__tstamp
    {% endif %}
    {% if var("snowplow__enable_media_player_v2") %}
    -- media player v2 context entity
    left join {{ var('snowplow__media_player_v2_context') }} mp2 on ev.event_id = mp2.media_player__id and ev.collector_tstamp = mp2.media_player__tstamp
    {% endif %}
    {% if var("snowplow__enable_media_session") %}
    -- media session context entity
    left join {{ var('snowplow__media_session_context') }} ms on ev.event_id = ms.session__id and ev.collector_tstamp = ms.session__tstamp
    {% endif %}
    {% if var("snowplow__enable_media_ad") %}
    -- media ad context entity
    left join {{ var('snowplow__media_ad_context') }} ma on ev.event_id = ma.ad__id and ev.collector_tstamp = ma.ad__tstamp
    {% endif %}
    {% if var("snowplow__enable_media_ad_break") %}
    -- media ad break context entity
    left join {{ var('snowplow__media_ad_break_context') }} mb on ev.event_id = mb.ad_break__id and ev.collector_tstamp = mb.ad_break__tstamp
    {% endif %}
    {% if var("snowplow__enable_web_events") %}
    -- web page context entity
    left join {{ var('snowplow__context_web_page') }} pv on ev.platform = 'web' and ev.event_id = pv.web_page__id and ev.collector_tstamp = pv.web_page__tstamp
    {% endif %}
    {% if var("snowplow__enable_mobile_events") %}
    -- screen context entity
    left join {{ var('snowplow__context_screen') }} sv on ev.platform = 'mob' and ev.event_id = sv.screen__id and ev.collector_tstamp = sv.screen__tstamp
    {% endif %}
    {% if var("snowplow__enable_ad_quartile_event") %}
    -- ad quartile event
    left join {{ var('snowplow__media_ad_quartile_event') }} aq on ev.event_id = aq.ad_quartile_event__id and ev.collector_tstamp = aq.ad_quartile_event__tstamp
    {% endif %}


where
    ev.event_id_dedupe_index = 1
)

 select
 coalesce(
    p.media_session_id,
    {{ dbt_utils.generate_surrogate_key(['p.page_view_id', 'p.media_id' ]) }}
  ) play_id,
  p.*,
  coalesce(cast(round(piv.weight_rate * p.duration / 100) as {{ type_int() }}), 0) as play_time_sec,
  coalesce(cast(case when p.is_muted then round(piv.weight_rate * p.duration / 100) end as {{ type_int() }}), 0) as play_time_sec_muted

  from prep p

  left join {{ ref("snowplow_media_player_pivot_base") }} piv
  on p.percent_progress = piv.percent_progress
