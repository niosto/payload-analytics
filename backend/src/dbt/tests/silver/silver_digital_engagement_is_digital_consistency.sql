-- Test: is_digital must match preferred_channel (mobile or web)
-- Returns rows that FAIL
select
    customer_id,
    preferred_channel,
    is_digital
from {{ ref('digital_engagement') }}
where is_digital is distinct from (preferred_channel in ('mobile', 'web'))
