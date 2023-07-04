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

/* Dedupe logic: Per dupe event_id keep earliest row ordered by collector_tstamp.
   If multiple earliest rows, take arbitrary one using row_number(). */

with events_this_run AS (
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
        a.domain_sessionidx,
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
        inner join {{ ref('snowplow_media_player_base_sessions_this_run') }} as b
            on a.domain_sessionid = b.session_id

    where a.collector_tstamp <= {{ snowplow_utils.timestamp_add('day', var("snowplow__max_session_days", 3), 'b.start_tstamp') }}
        and a.dvce_sent_tstamp <= {{ snowplow_utils.timestamp_add('day', var("snowplow__days_late_allowed", 3), 'a.dvce_created_tstamp') }}
        and a.collector_tstamp >= {{ lower_limit }}
        and a.collector_tstamp <= {{ upper_limit }}
        and {{ snowplow_utils.app_id_filter(var("snowplow__app_id",[])) }}
        and {{ snowplow_media_player.event_name_filter(var("snowplow__media_event_names", "['media_player_event']")) }}

),

-- unpacking the media player event
{{ snowplow_utils.get_sde_or_context(var('snowplow__atomic_schema', 'atomic'), var('snowplow__media_player_event_context'), lower_limit, upper_limit) }},
-- unpacking the media player context entity
{{ snowplow_utils.get_sde_or_context(var('snowplow__atomic_schema', 'atomic'), var('snowplow__media_player_context'), lower_limit, upper_limit) }},
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
-- unpacking the web page context entity
{{ snowplow_utils.get_sde_or_context(var('snowplow__atomic_schema', 'atomic'), var('snowplow__context_web_page'), lower_limit, upper_limit) }},

prep as (
  select
    ev.event_id,
    pv.id as page_view_id,
    ev.domain_sessionid,
    ev.domain_userid,
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
    mpe.label as media_label,
    mpe.type as event_type,

    -- unpacking the media player object
    round(mp.duration) as duration,
    mp.current_time as player_current_time,
    coalesce(mp.playback_rate, 1) as playback_rate,
    case when mpe.type = 'ended' then 100 else mp.percent_progress end percent_progress,
    mp.muted as is_muted,
    mp.is_live,
    mp.loop,
    mp.volume,

    -- combined media properties
    {{ media_id_col(youtube_player_id='yt.player_id', media_player_id='me.html_id') }},
    {{ media_player_type_col(youtube_player_id='yt.player_id', media_player_id='me.html_id') }},
    {{ source_url_col(youtube_url='yt.url', media_current_src='me.current_src')}},
    {{ media_type_col(media_media_type='me.media_type')}},
    {{ playback_quality_col(
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
    -- media player event
    left join {{ var('snowplow__media_player_event_context') }} mpe on ev.event_id = mpe.media_player_event__id and ev.collector_tstamp = mpe.media_player_event__tstamp
    -- media player context entity
    left join {{ var('snowplow__media_player_context') }} mp on ev.event_id = mp.media_player__id and ev.collector_tstamp = mp.media_player__tstamp
    -- web page context entity
    left join {{ var('snowplow__context_web_page') }} pv on ev.event_id = pv.web_page__id and ev.collector_tstamp = pv.web_page__tstamp


where
    ev.event_id_dedupe_index = 1
)

 select
 {{ dbt_utils.generate_surrogate_key(['p.page_view_id', 'p.media_id' ]) }} play_id,
  p.*,
  coalesce(cast(round(piv.weight_rate * p.duration / 100) as {{ type_int() }}), 0) as play_time_sec,
  coalesce(cast(case when p.is_muted then round(piv.weight_rate * p.duration / 100) end as {{ type_int() }}), 0) as play_time_sec_muted

  from prep p

  left join {{ ref("snowplow_media_player_pivot_base") }} piv
  on p.percent_progress = piv.percent_progress
