-- RFM scoring example (Postgres)
-- Assumes orders table has order_id, customer_id, order_date, sales

WITH customer_metrics AS (
  SELECT
    customer_id,
    MAX(order_date) AS last_order,
    COUNT(DISTINCT order_id) AS frequency,
    SUM(sales) AS monetary
  FROM orders
  GROUP BY customer_id
), params AS (
  SELECT MAX(last_order) + INTERVAL '1 day' AS snapshot
  FROM customer_metrics
)
SELECT cm.customer_id,
       EXTRACT(DAY FROM (p.snapshot - cm.last_order))::INT AS recency,
       cm.frequency,
       cm.monetary
FROM customer_metrics cm CROSS JOIN params p
ORDER BY monetary DESC;
