SELECT * FROM perf.stream_transactions
WHERE processed_at > (SELECT max(processed_at) FROM perf.stream_transactions) - INTERVAL '10 minutes';
