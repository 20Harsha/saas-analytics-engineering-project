{% macro mask_phone(column_name) %}
    case
        when {{ column_name }} is null then null
        else concat('XXXXXX', right({{ column_name }}, 4))
    end
{% endmacro %}
