-- KPI queries for retail sales analytics

-- Revenue by region with rank
SELECT region,
       SUM(sales) AS total_sales,
       RANK() OVER (ORDER BY SUM(sales) DESC) AS sales_rank
FROM orders
GROUP BY region;

-- Customer lifetime value
SELECT customer_id,
       SUM(sales) AS lifetime_value,
       COUNT(order_id) AS total_orders
FROM orders
GROUP BY customer_id
ORDER BY lifetime_value DESC;
SELECT region,
       SUM(sales) AS total_sales,
       RANK() OVER (ORDER BY SUM(sales) DESC) AS sales_rank
FROM orders
GROUP BY region;

SELECT customer_id,
       SUM(sales) AS lifetime_value,
       COUNT(order_id) AS total_orders
FROM orders
GROUP BY customer_id
ORDER BY lifetime_value DESC;