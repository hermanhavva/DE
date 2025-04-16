-- macros/get_min_max_value.sql

{% macro get_min_max_date(model_name, column_name) %}
    select
        min({{ column_name }}) as min_value,
        max({{ column_name }}) as max_value
    from {{ ref(model_name) }}
{% endmacro %}
