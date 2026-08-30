{% macro normalize_phone(column, country_code_column) %}
  CASE
    WHEN {{ column }} NOT LIKE '%+%'
    THEN (
      SELECT CONCAT('+',phone_code, {{column}})
      FROM {{ ref('seed_phone_codes') }}
      WHERE country_code = {{ country_code_column }}
      LIMIT 1
    )
    ELSE REPLACE(REPLACE(REPLACE(REPLACE(TRIM({{ column }}), '(', ''), ')', ''), ' ', ''), '-', '')
  END
{% endmacro %}