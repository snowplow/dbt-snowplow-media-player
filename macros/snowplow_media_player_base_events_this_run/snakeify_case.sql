{#
Copyright (c) 2022-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{# Take a string in camel/pascal case and make it snakecase #}
{% macro snakeify_case(text) %}
  {%- set re = modules.re %}
  {%- set camel_string1 = '([A-Z]+)([A-Z][a-z])' -%} {# Capitals followed by a lowercase  #}
  {%- set camel_string2 = '([a-z\d])([A-Z])' -%} {# lowercase followed by a capital #}
  {%- set replace_string = '\\1_\\2' %}
  {%- set output_text = re.sub(camel_string2, replace_string, re.sub(camel_string1, replace_string, text)).replace('-', '_').lower() -%}
  {{ output_text }}
{%- endmacro %}
