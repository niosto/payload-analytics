-- Test: a rate joined in the stream must come from the window before the
-- transaction: fx_event_time in [event_time - live_fx_join_seconds, event_time].
-- Returns rows that FAIL
select
    transaction_id,
    event_time,
    fx_event_time
from {{ ref('fct_live_transactions') }}
where fx_rate_source = 'event_window'
  and (
      fx_event_time is null
      or fx_event_time > event_time
      or fx_event_time < event_time - interval '{{ var("live_fx_join_seconds", 60) }} seconds'
  )
