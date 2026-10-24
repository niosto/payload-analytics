SELECT f.customer_country, d.year, d.month_number, sum(f.revenue_amount) AS revenue
FROM perf.fct_transactions f JOIN gold.dim_date d USING (date_id)
WHERE f.is_revenue
GROUP BY 1,2,3 ORDER BY 1,2,3;
