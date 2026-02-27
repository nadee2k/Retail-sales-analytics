-- Cohort analysis example (month-based cohorts)
WITH orders_cte AS (
  SELECT
    order_id,
    customer_id,
    DATE_TRUNC('month', order_date) AS order_month
  FROM orders
)
, cohorts AS (
  SELECT
    customer_id,
    MIN(order_month) AS cohort_month
  FROM orders_cte
  GROUP BY customer_id
)
SELECT c.cohort_month,
       o.order_month,
       COUNT(DISTINCT o.customer_id) AS customers
FROM orders_cte o
JOIN cohorts c ON o.customer_id = c.customer_id
GROUP BY c.cohort_month, o.order_month
ORDER BY c.cohort_month, o.order_month;
