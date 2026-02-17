{% macro mask_email(column_name) %}
    case
        when {{ column_name }} is null then null
        else concat('***@', split_part({{ column_name }}, '@', 2))
    end
{% endmacro %}
