{{ config(
    materialized='table',
    indexes=[
      {'columns': ['date_id'], 'unique': True},
      {'columns': ['full_date'], 'unique': True}
    ]
) }}

with date_spine as (
    select generate_series(
        '2000-01-01'::date,
        '2050-12-31'::date,
        '1 day'::interval
    )::date as full_date
)
select
    TO_CHAR(full_date, 'YYYYMMDD')::integer as date_id,
    full_date,
    TRIM(TO_CHAR(full_date, 'Day')) as day_name,
    EXTRACT(DOW FROM full_date) as day_of_week,
    EXTRACT(WEEK FROM full_date)::integer as week_number,
    EXTRACT(MONTH FROM full_date)::integer as month_number,
    TRIM(TO_CHAR(full_date, 'Month')) as month_name,
    EXTRACT(QUARTER FROM full_date)::integer as quarter,
    EXTRACT(YEAR FROM full_date)::integer as year,
    EXTRACT(DOW FROM full_date) IN (0, 6) as is_weekend,
    full_date = (DATE_TRUNC('month', full_date) + interval '1 month' - interval '1 day')::date as is_month_end
from date_spine
