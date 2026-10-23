-- Business questions the platform answers.
-- Cancelled and returned orders are excluded from revenue.

-- 1. Total revenue by day
SELECT d.full_date,
       COUNT(*)       AS orders,
       SUM(f.revenue) AS revenue
FROM sales.fact_sales f
JOIN sales.dim_date d ON d.date_key = f.date_key
WHERE f.order_status NOT IN ('CANCELLED', 'RETURNED')
GROUP BY d.full_date
ORDER BY d.full_date;

-- 2. Products that generate the most revenue
SELECT p.product_name,
       SUM(f.quantity) AS units_sold,
       SUM(f.revenue)  AS total_revenue
FROM sales.fact_sales f
JOIN sales.dim_product p ON p.product_key = f.product_key
WHERE f.order_status NOT IN ('CANCELLED', 'RETURNED')
GROUP BY p.product_name
ORDER BY total_revenue DESC;

-- 3. Cities with the most orders
SELECT c.city_name,
       COUNT(*)       AS orders,
       SUM(f.revenue) AS revenue
FROM sales.fact_sales f
JOIN sales.dim_city c ON c.city_key = f.city_key
GROUP BY c.city_name
ORDER BY orders DESC;

-- 4. Real-time orders in the last hour (streamed events)
SELECT DATE_TRUNC('minute', f.event_timestamp) AS minute,
       COUNT(*)                                AS orders
FROM sales.fact_sales f
WHERE f.source = 'stream'
  AND f.event_timestamp >= DATEADD(hour, -1, GETDATE())
GROUP BY 1
ORDER BY 1;

-- 5. Today vs. the trailing 7-day average
WITH daily AS (
    SELECT d.full_date, SUM(f.revenue) AS revenue
    FROM sales.fact_sales f
    JOIN sales.dim_date d ON d.date_key = f.date_key
    WHERE f.order_status NOT IN ('CANCELLED', 'RETURNED')
    GROUP BY d.full_date
)
SELECT full_date,
       revenue,
       AVG(revenue) OVER (ORDER BY full_date ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) AS avg_prev_7_days,
       revenue / NULLIF(AVG(revenue) OVER (ORDER BY full_date ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING), 0) - 1
           AS pct_vs_prev_7_days
FROM daily
ORDER BY full_date DESC
LIMIT 14;

-- 6. Payment method mix
SELECT payment_method,
       COUNT(*)                                            AS orders,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1)  AS pct_orders
FROM sales.fact_sales
GROUP BY payment_method
ORDER BY orders DESC;

-- 7. Invalid / duplicated transactions come from the Glue run reports in
--    s3://<bucket>/reports/sales/ (query with Athena or load into a dq table).
