{% macro unaccent(column) %}
    LOWER(TRIM(
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                {{ column }}::text,
                'á','a'),'é','e'),'í','i'),'ó','o'),'ú','u'),
                'à','a'),'è','e'),'ì','i'),'ò','o'),'ù','u'),
                'â','a'),'ê','e'),'î','i'),'ô','o'),'û','u'),
                'ä','a'),'ë','e'),'ï','i'),'ö','o'),'ü','u'),
                'Á','a'),'É','e'),'Í','i'),'Ó','o'),'Ú','u'),
                'ñ','n'),'Ñ','n'),'ç','c'),'Ç','c'),'ã','a')
        ))
{%- endmacro %}