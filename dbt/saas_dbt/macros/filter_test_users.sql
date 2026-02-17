{% macro exclude_test_users(name_col) %}
    lower({{ name_col }}) not like '%test%'
{% endmacro %}