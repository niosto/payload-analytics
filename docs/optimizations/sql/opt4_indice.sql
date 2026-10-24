CREATE INDEX st_event_time_brin ON perf.stream_transactions USING brin (event_time) WITH (pages_per_range = 32);
