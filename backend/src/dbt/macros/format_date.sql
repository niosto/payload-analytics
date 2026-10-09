{% macro format_date(column) %}

TO_DATE(
         {{ column }},
         CASE
           WHEN {{ column }} LIKE '____-__-__' THEN 'YYYY-MM-DD'
           WHEN {{ column }} LIKE '________' THEN 'YYYYMMDD'
           WHEN {{ column }} LIKE '____/__/__' THEN 'YYYY/MM/DD'
           WHEN {{ column }} LIKE '__-__-____' THEN 'MM-DD-YYYY'
           WHEN {{ column }} LIKE '__/__/____' THEN 'DD/MM/YYYY'
         END
       )
{% endmacro %}