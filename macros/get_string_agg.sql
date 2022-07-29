{#
 Takes care of harmonising cross-db list_agg, string_agg type functions.
 #}

{%- macro get_string_agg(string_column, column_prefix) -%}
    {{ return(adapter.dispatch('get_string_agg', 'snowplow_media_player')(string_column, column_prefix)) }}
{%- endmacro -%}

{% macro default__get_string_agg(string_column, column_prefix) %}
   listagg({{column_prefix}}.{{string_column}}, ',') within group (order by {{column_prefix}}.{{string_column}})
{% endmacro %}

{% macro bigquery__get_string_agg(string_column, column_prefix) %}
    string_agg(cast({{column_prefix}}.{{string_column}} as string), ',' order by {{column_prefix}}.{{string_column}})
{% endmacro %}

{% macro databricks__get_string_agg(string_column, column_prefix) %}
    array_join(array_sort(collect_set(cast({{column_prefix}}.{{string_column}} as int))),",")
{% endmacro %}

{% macro postgres__get_string_agg(string_column, column_prefix) %}
    string_agg({{column_prefix}}.{{string_column}}::varchar(10), ',' order by {{column_prefix}}.{{string_column}})
{% endmacro %}

{% macro redshift__get_string_agg(string_column, column_prefix) %}
   listagg({{column_prefix}}.{{string_column}}, ',') within group (order by {{column_prefix}}.{{string_column}})
{% endmacro %}

{% macro spark__get_string_agg(string_column, column_prefix) %}
    array_join(array_sort(collect_set(cast({{column_prefix}}.{{string_column}} as int))),",")
{% endmacro %}
