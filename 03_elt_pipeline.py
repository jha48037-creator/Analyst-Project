"""
STEP 3 — ELT PIPELINE  (Extract → Load Raw → Transform with SQL)
Modern pattern: load data as-is into a "raw" schema, then
transform using SQL views/CTEs inside the warehouse (SQLite here,
same concept applies to Snowflake / BigQuery / Databricks).

Key difference vs ETL:
  ETL  → transform BEFORE loading  (Python/pandas does the heavy lifting)
  ELT  → transform AFTER loading   (warehouse SQL does the heavy lifting)
"""

import pandas as pd
import sqlite3
import os
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
log = logging.getLogger(__name__)

RAW    = "data/raw"
DB_ELT = "data/processed/sales_elt.db"
os.makedirs("data/processed", exist_ok=True)

# ═════════════════════════════════════════════════════════════════════════════
# STEP E — EXTRACT (raw read, no transformation)
# ═════════════════════════════════════════════════════════════════════════════
def extract_raw() -> dict:
    log.info("[E] Reading raw CSVs — no transformation at this stage")
    return {
        name: pd.read_csv(f"{RAW}/{fname}")
        for name, fname in {
            "raw_fact_sales":    "fact_sales.csv",
            "raw_dim_customers": "dim_customers.csv",
            "raw_dim_products":  "dim_products.csv",
            "raw_dim_orders":    "dim_orders.csv",
            "raw_dim_date":      "dim_date.csv",
        }.items()
    }


# ═════════════════════════════════════════════════════════════════════════════
# STEP L — LOAD RAW (land everything into the warehouse unchanged)
# ═════════════════════════════════════════════════════════════════════════════
def load_raw(dfs: dict):
    log.info("[L] Loading raw tables into warehouse (no transforms)")
    conn = sqlite3.connect(DB_ELT)
    for tname, df in dfs.items():
        df.to_sql(tname, conn, if_exists="replace", index=False)
        log.info(f"    Loaded raw.{tname}: {len(df):,} rows")
    conn.commit()
    conn.close()


# ═════════════════════════════════════════════════════════════════════════════
# STEP T — TRANSFORM USING SQL (inside the warehouse)
# These become "transformed" views / materialized tables.
# In dbt, each block below would be a separate model (.sql file).
# ═════════════════════════════════════════════════════════════════════════════
TRANSFORM_SQL = {

    # ── Staging layer (clean + cast) ────────────────────────────────────────
    "stg_fact_sales": """
        CREATE VIEW IF NOT EXISTS stg_fact_sales AS
        SELECT
            order_id,
            customer_id,
            product_id,
            DATE(order_date)           AS order_date,
            CAST(qty          AS INT)  AS qty,
            CAST(unit_price   AS REAL) AS unit_price,
            CAST(total_amount AS REAL) AS total_amount,
            CASE WHEN CAST(total_amount AS REAL) > 0
                      AND CAST(qty AS INT) > 0 THEN 1 ELSE 0 END AS is_valid
        FROM raw_fact_sales
        WHERE order_id IS NOT NULL
          AND customer_id IS NOT NULL
          AND product_id  IS NOT NULL;
    """,

    "stg_dim_orders": """
        CREATE VIEW IF NOT EXISTS stg_dim_orders AS
        SELECT
            order_id,
            TRIM(channel)     AS channel,
            TRIM(status)      AS status,
            CAST(discount_pct AS INT) AS discount_pct,
            CASE WHEN TRIM(status) = 'Cancelled' THEN 1 ELSE 0 END AS is_cancelled,
            CASE WHEN TRIM(status) = 'Returned'  THEN 1 ELSE 0 END AS is_returned,
            CASE
                WHEN CAST(discount_pct AS INT) = 0        THEN 'no_discount'
                WHEN CAST(discount_pct AS INT) <= 10      THEN 'low'
                WHEN CAST(discount_pct AS INT) <= 20      THEN 'medium'
                ELSE 'high'
            END AS discount_tier
        FROM raw_dim_orders;
    """,

    "stg_dim_customers": """
        CREATE VIEW IF NOT EXISTS stg_dim_customers AS
        SELECT
            customer_id,
            TRIM(city)      AS city,
            TRIM(tier)      AS tier,
            TRIM(age_group) AS age_group,
            CASE tier
                WHEN 'Bronze'   THEN 1
                WHEN 'Silver'   THEN 2
                WHEN 'Gold'     THEN 3
                WHEN 'Platinum' THEN 4
                ELSE 0
            END AS tier_rank
        FROM raw_dim_customers;
    """,

    # ── Mart layer (business-ready joins) ───────────────────────────────────
    "mart_sales_enriched": """
        CREATE VIEW IF NOT EXISTS mart_sales_enriched AS
        SELECT
            fs.order_id,
            fs.order_date,
            STRFTIME('%Y', fs.order_date)  AS year,
            STRFTIME('%m', fs.order_date)  AS month,
            CAST(STRFTIME('%Y', fs.order_date) AS INT) * 100
                + CAST(STRFTIME('%m', fs.order_date) AS INT) AS year_month,

            fs.customer_id,
            c.city,
            c.tier,
            c.tier_rank,
            c.age_group,

            fs.product_id,
            p.category,
            p.sub_category,

            o.channel,
            o.status,
            o.discount_pct,
            o.discount_tier,
            o.is_cancelled,
            o.is_returned,

            fs.qty,
            fs.unit_price,
            fs.total_amount,
            fs.is_valid,

            -- Derived business metrics
            fs.total_amount * (1.0 - o.is_cancelled) AS net_revenue,
            fs.qty * fs.unit_price                    AS gross_revenue,
            fs.total_amount / NULLIF(fs.qty, 0)       AS avg_unit_revenue

        FROM stg_fact_sales     fs
        LEFT JOIN stg_dim_customers c ON fs.customer_id = c.customer_id
        LEFT JOIN raw_dim_products  p ON fs.product_id  = p.product_id
        LEFT JOIN stg_dim_orders    o ON fs.order_id    = o.order_id
        WHERE fs.is_valid = 1;
    """,

    # ── Aggregated marts ────────────────────────────────────────────────────
    "mart_monthly_revenue": """
        CREATE VIEW IF NOT EXISTS mart_monthly_revenue AS
        SELECT
            year,
            month,
            year_month,
            COUNT(DISTINCT order_id)    AS total_orders,
            COUNT(DISTINCT customer_id) AS unique_customers,
            SUM(net_revenue)            AS total_revenue,
            AVG(total_amount)           AS avg_order_value,
            SUM(qty)                    AS units_sold
        FROM mart_sales_enriched
        GROUP BY year, month, year_month
        ORDER BY year_month;
    """,

    "mart_product_performance": """
        CREATE VIEW IF NOT EXISTS mart_product_performance AS
        SELECT
            category,
            sub_category,
            COUNT(DISTINCT order_id)    AS total_orders,
            SUM(qty)                    AS total_units,
            SUM(net_revenue)            AS total_revenue,
            AVG(total_amount)           AS avg_order_value,
            COUNT(DISTINCT customer_id) AS unique_buyers
        FROM mart_sales_enriched
        GROUP BY category, sub_category
        ORDER BY total_revenue DESC;
    """,

    "mart_customer_segments": """
        CREATE VIEW IF NOT EXISTS mart_customer_segments AS
        SELECT
            customer_id,
            city,
            tier,
            tier_rank,
            age_group,
            COUNT(DISTINCT order_id)    AS total_orders,
            SUM(net_revenue)            AS lifetime_value,
            AVG(total_amount)           AS avg_order_value,
            MIN(order_date)             AS first_order,
            MAX(order_date)             AS last_order,
            CAST(
                JULIANDAY(MAX(order_date)) - JULIANDAY(MIN(order_date))
            AS INT)                     AS tenure_days
        FROM mart_sales_enriched
        GROUP BY customer_id, city, tier, tier_rank, age_group;
    """,
}


def transform_in_warehouse():
    log.info("[T] Creating transformation views inside warehouse")
    conn = sqlite3.connect(DB_ELT)
    for view_name, sql in TRANSFORM_SQL.items():
        conn.execute(sql)
        log.info(f"    View created: {view_name}")
    conn.commit()

    # Verify all marts
    for view_name in TRANSFORM_SQL:
        count = conn.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()[0]
        log.info(f"    {view_name}: {count:,} rows")

    conn.close()


# ═════════════════════════════════════════════════════════════════════════════
# MAIN
# ═════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    log.info("ELT pipeline started")
    raw_dfs = extract_raw()
    load_raw(raw_dfs)
    transform_in_warehouse()
    log.info(f"ELT complete → {DB_ELT} ✅")
    log.info("Available views: stg_*, mart_sales_enriched, mart_monthly_revenue, "
             "mart_product_performance, mart_customer_segments")
