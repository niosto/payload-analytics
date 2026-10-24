SELECT status, count(*) FROM perf.stream_transactions
WHERE event_time >= now() - interval '6 hours' GROUP BY status;
