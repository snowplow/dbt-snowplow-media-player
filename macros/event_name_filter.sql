{#
Copyright (c) 2022-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{# Filters on event_name if provided #}
{% macro event_name_filter(event_names) %}

  (
  {%- if event_names|length -%}
    lower(event_name) in ('{{ event_names|map("lower")|join("','") }}') --filter on event_name if provided
  {%- else -%}
    true
  {%- endif %}
  or lower(event_vendor) = 'com.snowplowanalytics.snowplow.media'
  )

{% endmacro %}
