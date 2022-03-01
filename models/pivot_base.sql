with prep as (

{% for element in var("snowplow__percent_progress_boundaries") %}
  {% if element < 0 or element > 100 %}
    {{ exceptions.raise_compiler_error("`snowplow__percent_progress_boundary` is outside the accepted range 0-100. Got: " ~ element) }}
  {% endif %}

  {% if element % 1 != 0 %}
    {{ exceptions.raise_compiler_error("`snowplow__percent_progress_boundary` needs to be a whole number. Got: " ~ element) }}
  {% endif %}

  select
    {{ element }} as percent_progress
  {% if not loop.last %}
  union all
  {% endif %}
{% endfor %}

{% if 100 not in var("snowplow__percent_progress_boundaries") %}
  union all
  select 100 as percent_progress
{% endif %}

)

, weight_calc as (

  select
    percent_progress,
    percent_progress - lag(percent_progress, 1) over(order by percent_progress) as weight_rate,
    first_value(percent_progress) over(order by percent_progress rows between unbounded preceding and unbounded following) as first_item

  from prep

  order by percent_progress

)

select
  percent_progress,
  coalesce(weight_rate, first_item) as weight_rate

from weight_calc
