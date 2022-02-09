{{
  config(
    materialized='table'
  )
}}

with prep as (
  
  {% for key, value in var("snowplow__percent_progress_boundaries").items() %}

    select
      {{ key }} as percent_progress,
      {{ value }} as weight_rate
    
    {% if not loop.last %}

    union all

    {% endif %}

  {% endfor %}
)

select *

from prep
