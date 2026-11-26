{#
Copyright (c) 2022-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{% macro media_event_filter(event_names, v2_event_names=none) %}
  {{ return({
    'event_names': event_names,
    'event_vendor': 'com.snowplowanalytics.snowplow.media',
    'vendor_event_names': v2_event_names,
    'case_sensitive': v2_event_names is not none
  }) }}
{% endmacro %}

{% macro event_name_filter(event_names) %}
  {{ snowplow_utils.event_scan_filter(snowplow_media_player.media_event_filter(event_names)) }}
{% endmacro %}
