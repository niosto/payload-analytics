{% test accepted_range(model, column_name, min_value=none, max_value=none) %}
    select *
    from {{ model }}
    where 
    {% if min_value is not none and max_value is not none %}
        {{ column_name }} < {{ min_value }} or {{ column_name }} > {{ max_value }}
    {% elif min_value is not none %}
        {{ column_name }} < {{ min_value }}
    {% elif max_value is not none %}
        {{ column_name }} > {{ max_value }}
    {% else %}
        1 = 0 -- no test conditions provided
    {% endif %}
{% endtest %}
