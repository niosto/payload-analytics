{% macro clean_email(column) %}
    CASE
        WHEN {{ column }} IS NULL THEN NULL
        ELSE (
            WITH unaccented AS (
                SELECT
                    {{unaccent(column)}} AS email
            ), 
            domain_part AS (
                SELECT
                    REGEXP_REPLACE(
                        -- removes trailling dots
                        RTRIM(
                        -- takes what's after the last @
                            SUBSTRING(email FROM '@([^@]+)$'),
                            '.'  
                        ),
                        '[^a-z0-9.\-]', '', 'g'
                    ) AS domain
                FROM unaccented
            ),
            username_part AS (
                SELECT
                    -- takes everything before the last @
                    REGEXP_REPLACE(
                        SUBSTRING(email FROM '^(.*)@[^@]+$'),
                        '[^a-z0-9._\-]', '', 'g'
                    ) AS username
                FROM unaccented
            ),
            combined AS (
                SELECT username, domain, email
                FROM username_part, domain_part, unaccented
            )
            SELECT
                CASE
                    WHEN CONCAT(username, '@', domain) ~ '^[a-z0-9._\-]+@[a-z0-9.\-]+\.[a-z]{2,}$'
                    THEN CONCAT(username, '@', domain)
                    ELSE email
                END AS full_email
            FROM combined
        )
    END
{% endmacro %}