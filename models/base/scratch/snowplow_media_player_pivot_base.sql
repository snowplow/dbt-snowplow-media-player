{#
Copyright (c) 2022-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{{
  config(
    materialized='table',
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
  )
}}

with prep as (

  {% for element in get_percentage_boundaries(var("snowplow__percent_progress_boundaries")) %}

    select

      {{ element }} as percent_progress

    {% if not loop.last %}

      union all

    {% endif %}

  {% endfor %}

)

, weight_calc as (

  select
    percent_progress,
    percent_progress
    - lag(percent_progress, 1) over (order by percent_progress) as weight_rate,
    first_value(percent_progress)
      over (
        order by
          percent_progress
        rows between unbounded preceding and unbounded following
      ) as first_item

  from prep

  order by percent_progress

)

select
  percent_progress,
  coalesce(weight_rate, first_item) as weight_rate

from weight_calc
