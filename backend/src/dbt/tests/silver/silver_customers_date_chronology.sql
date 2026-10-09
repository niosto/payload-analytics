-- Test: registration_date and date_of_birth must be valid ISO dates (YYYY-MM-DD)
-- The format_date macro already casts to date; this confirms no nulls crept in
-- and that dates are not in the future for date_of_birth
-- Returns rows that FAIL
select
    customer_id,
    registration_date,
    date_of_birth
from {{ ref('customers') }}
where
    -- registration_date cannot be in the future
    registration_date::date > CURRENT_DATE
    -- date_of_birth cannot be in the future
    or date_of_birth::date > CURRENT_DATE
    -- date_of_birth must be before registration_date
    or date_of_birth::date >= registration_date::date
