{% macro get_percentage_boundaries(tracked_boundaries) %}

   {% set percentage_boundaries = [] %}

   {% for element in var("snowplow__percent_progress_boundaries") %}
     {% if element < 0 or element > 100 %}
       {{ exceptions.raise_compiler_error("`snowplow__percent_progress_boundary` is outside the accepted range 0-100. Got: " ~ element) }}

     {% elif element % 1 != 0 %}
       {{ exceptions.raise_compiler_error("`snowplow__percent_progress_boundary` needs to be a whole number. Got: " ~ element) }}

     {% else %}
       {% do percentage_boundaries.append(element) %}
     {% endif %}
   {% endfor %}

   {% if 100 not in var("snowplow__percent_progress_boundaries") %}
     {% do percentage_boundaries.append(100) %}
   {% endif %}

   {{ return(percentage_boundaries) }}

 {% endmacro %}
