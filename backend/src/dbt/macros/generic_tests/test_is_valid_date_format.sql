-- tests/generic/is_valid_date_format.sql
{% test is_valid_date_format(model, column_name) %}

select {{ column_name }}
from {{ model }}
where
    {{ column_name }} is not null
    and {{ column_name }}::text !~ '^\d{4}-\d{2}-\d{2}$'

{% endtest %}