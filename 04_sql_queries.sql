-- ═══════════════════════════════════════════════════════════════════════════
-- STEP 4 — SQL ANALYSIS QUERIES
-- Database: data/processed/sales_dw.db  (created by ETL)
--           OR  sales_elt.db and use mart_sales_enriched view
-- ═══════════════════════════════════════════════════════════════════════════


-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  SECTION A — BASIC QUERIES (warm-up)                                    │
-- └─────────────────────────────────────────────────────────────────────────┘

-- A1. Total revenue, orders, and average order value
SELECT
    COUNT(*)                                    AS total_orders,
    ROUND(SUM(total_amount), 2)                 AS total_revenue,
    ROUND(AVG(total_amount), 2)                 AS avg_order_value,
    COUNT(DISTINCT customer_id)                 AS unique_customers,
    COUNT(DISTINCT product_id)                  AS unique_products
FROM fact_sales;


-- A2. Revenue by year
SELECT
    order_year,
    COUNT(*)                          AS orders,
    ROUND(SUM(total_amount), 0)       AS revenue,
    ROUND(AVG(total_amount), 2)       AS avg_order
FROM fact_sales
GROUP BY order_year
ORDER BY order_year;


-- A3. Top 10 products by revenue (with join)
SELECT
    p.category,
    p.sub_category,
    COUNT(f.order_id)               AS orders,
    SUM(f.qty)                      AS units_sold,
    ROUND(SUM(f.total_amount), 0)   AS revenue
FROM fact_sales f
JOIN dim_products p ON f.product_id = p.product_id
GROUP BY p.category, p.sub_category
ORDER BY revenue DESC
LIMIT 10;


-- A4. Revenue by customer tier
SELECT
    c.tier,
    COUNT(DISTINCT f.customer_id)   AS customers,
    COUNT(f.order_id)               AS orders,
    ROUND(SUM(f.total_amount), 0)   AS revenue,
    ROUND(AVG(f.total_amount), 2)   AS avg_order
FROM fact_sales f
JOIN dim_customers c ON f.customer_id = c.customer_id
GROUP BY c.tier
ORDER BY revenue DESC;


-- A5. Orders by channel and status
SELECT
    o.channel,
    o.status,
    COUNT(*)                        AS orders,
    ROUND(SUM(f.total_amount), 0)   AS revenue
FROM fact_sales f
JOIN dim_orders o ON f.order_id = o.order_id
GROUP BY o.channel, o.status
ORDER BY o.channel, orders DESC;


-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  SECTION B — INTERMEDIATE QUERIES                                       │
-- └─────────────────────────────────────────────────────────────────────────┘

-- B1. Monthly revenue trend with MoM growth (LAG window function)
WITH monthly AS (
    SELECT
        order_year                          AS yr,
        order_month                         AS mo,
        ROUND(SUM(total_amount), 0)         AS revenue
    FROM fact_sales
    GROUP BY order_year, order_month
)
SELECT
    yr,
    mo,
    revenue,
    LAG(revenue) OVER (ORDER BY yr, mo)                        AS prev_month_rev,
    ROUND(
        (revenue - LAG(revenue) OVER (ORDER BY yr, mo))
        * 100.0 / NULLIF(LAG(revenue) OVER (ORDER BY yr, mo), 0),
    2) AS mom_growth_pct
FROM monthly
ORDER BY yr, mo;


-- B2. Running total revenue per year
SELECT
    order_year,
    order_month,
    ROUND(SUM(total_amount), 0)                                AS monthly_rev,
    ROUND(SUM(SUM(total_amount)) OVER (
        PARTITION BY order_year
        ORDER BY order_month
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ), 0)                                                      AS ytd_revenue
FROM fact_sales
GROUP BY order_year, order_month
ORDER BY order_year, order_month;


-- B3. Customer RFM scores (Recency, Frequency, Monetary)
WITH rfm_base AS (
    SELECT
        customer_id,
        CAST(JULIANDAY('2025-01-01') - JULIANDAY(MAX(order_date)) AS INT)
                                            AS recency_days,
        COUNT(order_id)                     AS frequency,
        ROUND(SUM(total_amount), 2)         AS monetary
    FROM fact_sales
    GROUP BY customer_id
),
rfm_scored AS (
    SELECT *,
        NTILE(5) OVER (ORDER BY recency_days DESC)  AS r_score,  -- lower days = better
        NTILE(5) OVER (ORDER BY frequency)          AS f_score,
        NTILE(5) OVER (ORDER BY monetary)           AS m_score
    FROM rfm_base
)
SELECT
    customer_id,
    recency_days,
    frequency,
    ROUND(monetary, 0)  AS monetary,
    r_score, f_score, m_score,
    (r_score + f_score + m_score) AS total_rfm_score,
    CASE
        WHEN (r_score + f_score + m_score) >= 13 THEN 'Champion'
        WHEN (r_score + f_score + m_score) >= 10 THEN 'Loyal'
        WHEN (r_score + f_score + m_score) >= 7  THEN 'Potential'
        WHEN r_score <= 2                         THEN 'At Risk'
        ELSE 'Needs Attention'
    END AS rfm_segment
FROM rfm_scored
ORDER BY total_rfm_score DESC;


-- B4. Product category share of wallet (% of total revenue)
SELECT
    p.category,
    ROUND(SUM(f.total_amount), 0)                  AS revenue,
    ROUND(
        SUM(f.total_amount) * 100.0
        / SUM(SUM(f.total_amount)) OVER (),
    2)                                             AS revenue_share_pct,
    ROUND(
        SUM(SUM(f.total_amount)) OVER (
            ORDER BY SUM(f.total_amount) DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) * 100.0 / SUM(SUM(f.total_amount)) OVER (),
    2)                                             AS cumulative_pct
FROM fact_sales f
JOIN dim_products p ON f.product_id = p.product_id
GROUP BY p.category
ORDER BY revenue DESC;


-- B5. Discount impact on revenue
SELECT
    o.discount_pct,
    COUNT(f.order_id)               AS orders,
    ROUND(AVG(f.total_amount), 2)   AS avg_order_value,
    ROUND(SUM(f.total_amount), 0)   AS total_revenue,
    ROUND(AVG(f.qty), 2)            AS avg_qty
FROM fact_sales f
JOIN dim_orders o ON f.order_id = o.order_id
GROUP BY o.discount_pct
ORDER BY o.discount_pct;


-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  SECTION C — ADVANCED QUERIES (window functions, CTEs, subqueries)      │
-- └─────────────────────────────────────────────────────────────────────────┘

-- C1. Year-over-year revenue comparison per category
WITH cat_year AS (
    SELECT
        p.category,
        f.order_year,
        ROUND(SUM(f.total_amount), 0)   AS revenue
    FROM fact_sales f
    JOIN dim_products p ON f.product_id = p.product_id
    GROUP BY p.category, f.order_year
)
SELECT
    category,
    MAX(CASE WHEN order_year = 2022 THEN revenue END) AS rev_2022,
    MAX(CASE WHEN order_year = 2023 THEN revenue END) AS rev_2023,
    MAX(CASE WHEN order_year = 2024 THEN revenue END) AS rev_2024,
    ROUND(
        (MAX(CASE WHEN order_year = 2024 THEN revenue END)
         - MAX(CASE WHEN order_year = 2022 THEN revenue END))
        * 100.0
        / NULLIF(MAX(CASE WHEN order_year = 2022 THEN revenue END), 0),
    2) AS growth_2yr_pct
FROM cat_year
GROUP BY category
ORDER BY rev_2024 DESC NULLS LAST;


-- C2. Top customer per city by lifetime value
WITH ranked AS (
    SELECT
        c.city,
        f.customer_id,
        ROUND(SUM(f.total_amount), 0)   AS ltv,
        ROW_NUMBER() OVER (
            PARTITION BY c.city
            ORDER BY SUM(f.total_amount) DESC
        ) AS city_rank
    FROM fact_sales f
    JOIN dim_customers c ON f.customer_id = c.customer_id
    GROUP BY c.city, f.customer_id
)
SELECT city, customer_id, ltv
FROM ranked
WHERE city_rank = 1
ORDER BY ltv DESC;


-- C3. Cohort retention — customers active in month 1 who returned
WITH first_order AS (
    SELECT
        customer_id,
        STRFTIME('%Y-%m', MIN(order_date)) AS cohort_month
    FROM fact_sales
    GROUP BY customer_id
),
activity AS (
    SELECT DISTINCT
        f.customer_id,
        fo.cohort_month,
        STRFTIME('%Y-%m', f.order_date)    AS activity_month,
        CAST(
            (CAST(STRFTIME('%Y', f.order_date) AS INT) -
             CAST(SUBSTR(fo.cohort_month, 1, 4) AS INT)) * 12 +
            (CAST(STRFTIME('%m', f.order_date) AS INT) -
             CAST(SUBSTR(fo.cohort_month, 6, 2) AS INT))
        AS INT)                            AS months_since_first
    FROM fact_sales f
    JOIN first_order fo ON f.customer_id = fo.customer_id
)
SELECT
    cohort_month,
    months_since_first,
    COUNT(DISTINCT customer_id)          AS active_customers
FROM activity
WHERE months_since_first BETWEEN 0 AND 11
GROUP BY cohort_month, months_since_first
ORDER BY cohort_month, months_since_first;


-- C4. 3-month rolling average order value per channel
WITH monthly_channel AS (
    SELECT
        o.channel,
        f.order_year                    AS yr,
        f.order_month                   AS mo,
        ROUND(AVG(f.total_amount), 2)   AS avg_order
    FROM fact_sales f
    JOIN dim_orders o ON f.order_id = o.order_id
    GROUP BY o.channel, f.order_year, f.order_month
)
SELECT
    channel, yr, mo, avg_order,
    ROUND(AVG(avg_order) OVER (
        PARTITION BY channel
        ORDER BY yr, mo
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ), 2) AS rolling_3m_avg
FROM monthly_channel
ORDER BY channel, yr, mo;


-- C5. Identify cross-sell opportunities — customers who buy Electronics but not Clothing
SELECT DISTINCT
    f.customer_id
FROM fact_sales f
JOIN dim_products p ON f.product_id = p.product_id
WHERE p.category = 'Electronics'
  AND f.customer_id NOT IN (
      SELECT DISTINCT f2.customer_id
      FROM fact_sales f2
      JOIN dim_products p2 ON f2.product_id = p2.product_id
      WHERE p2.category = 'Clothing'
  )
LIMIT 20;


-- C6. Customers with declining spend (2023 revenue < 2022 revenue)
WITH yearly_spend AS (
    SELECT
        customer_id,
        SUM(CASE WHEN order_year = 2022 THEN total_amount ELSE 0 END) AS rev_2022,
        SUM(CASE WHEN order_year = 2023 THEN total_amount ELSE 0 END) AS rev_2023,
        SUM(CASE WHEN order_year = 2024 THEN total_amount ELSE 0 END) AS rev_2024
    FROM fact_sales
    GROUP BY customer_id
    HAVING rev_2022 > 0 AND rev_2023 > 0
)
SELECT
    customer_id,
    ROUND(rev_2022, 0) AS rev_2022,
    ROUND(rev_2023, 0) AS rev_2023,
    ROUND(rev_2024, 0) AS rev_2024,
    ROUND((rev_2023 - rev_2022) * 100.0 / rev_2022, 2) AS yoy_change_pct
FROM yearly_spend
WHERE rev_2023 < rev_2022
ORDER BY yoy_change_pct ASC
LIMIT 20;
