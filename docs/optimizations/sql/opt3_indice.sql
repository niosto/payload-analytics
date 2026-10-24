CREATE INDEX fct_tx_revenue_idx ON perf.fct_transactions (date_id)
  INCLUDE (customer_country, revenue_amount) WHERE is_revenue;
