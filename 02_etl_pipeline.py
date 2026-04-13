"""
STEP 2 — ETL PIPELINE  (Extract → Transform → Load)
Reads raw CSVs, applies business transformations, then loads into SQLite.
3-year experience level: type casting, null handling, deduplication,
derived columns, business rules, data quality checks.
"""

import pandas as pd
import numpy as np
import sqlite3
import os
import logging
from datetime import datetime

# ─── LOGGING SETUP ────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.FileHandler("logs/etl_pipeline.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)
os.makedirs("logs", exist_ok=True)
os.makedirs("data/processed", exist_ok=True)

RAW    = "data/raw"
DB_PATH = "data/processed/sales_dw.db"

# ═════════════════════════════════════════════════════════════════════════════
# EXTRACT
# ═════════════════════════════════════════════════════════════════════════════
def extract():
    log.info("── EXTRACT ──────────────────────────────────────")
    dfs = {}
    files = {
        "fact_sales":    "fact_sales.csv",
        "dim_customers": "dim_customers.csv",
        "dim_products":  "dim_products.csv",
        "dim_orders":    "dim_orders.csv",
        "dim_date":      "dim_date.csv",
    }
    for name, fname in files.items():
        path = f"{RAW}/{fname}"
        df = pd.read_csv(path)
        log.info(f"  Extracted {name}: {len(df):,} rows  {df.shape[1]} cols")
        dfs[name] = df
    return dfs


# ═════════════════════════════════════════════════════════════════════════════
# TRANSFORM
# ═════════════════════════════════════════════════════════════════════════════
def transform(dfs: dict) -> dict:
    log.info("── TRANSFORM ────────────────────────────────────")

    # ── fact_sales ─────────────────────────────────────────────────────────
    fs = dfs["fact_sales"].copy()

    # 1. Type casting
    fs["order_date"]   = pd.to_datetime(fs["order_date"])
    fs["qty"]          = fs["qty"].astype(int)
    fs["unit_price"]   = fs["unit_price"].astype(float)
    fs["total_amount"] = fs["total_amount"].astype(float)

    # 2. Null / anomaly checks
    null_counts = fs.isnull().sum()
    if null_counts.any():
        log.warning(f"Nulls detected:\n{null_counts[null_counts > 0]}")
    fs.dropna(subset=["order_id", "customer_id", "product_id"], inplace=True)

    # 3. Deduplication
    before = len(fs)
    fs.drop_duplicates(subset=["order_id"], inplace=True)
    log.info(f"  Deduped fact_sales: {before - len(fs)} duplicates removed")

    # 4. Business rule — flag negative or zero totals
    invalid = fs[fs["total_amount"] <= 0]
    if len(invalid):
        log.warning(f"  {len(invalid)} rows with invalid total_amount — flagged")
    fs["is_valid"] = (fs["total_amount"] > 0) & (fs["qty"] > 0)

    # 5. Derived columns
    fs["order_year"]    = fs["order_date"].dt.year
    fs["order_month"]   = fs["order_date"].dt.month
    fs["order_quarter"] = fs["order_date"].dt.quarter
    fs["order_week"]    = fs["order_date"].dt.isocalendar().week.astype(int)
    fs["revenue_band"]  = pd.cut(
        fs["total_amount"],
        bins=[0, 1000, 5000, 20000, 50000, float("inf")],
        labels=["micro", "small", "medium", "large", "mega"]
    )
    dfs["fact_sales"] = fs
    log.info(f"  fact_sales transformed: {len(fs):,} rows")

    # ── dim_customers ──────────────────────────────────────────────────────
    dc = dfs["dim_customers"].copy()
    dc["tier"] = dc["tier"].str.strip().str.title()
    dc["city"] = dc["city"].str.strip().str.title()
    dc["tier_rank"] = dc["tier"].map(
        {"Bronze": 1, "Silver": 2, "Gold": 3, "Platinum": 4}
    )
    dc.drop_duplicates(subset=["customer_id"], inplace=True)
    dfs["dim_customers"] = dc
    log.info(f"  dim_customers transformed: {len(dc):,} rows")

    # ── dim_products ───────────────────────────────────────────────────────
    dp = dfs["dim_products"].copy()
    dp["category"]     = dp["category"].str.strip()
    dp["sub_category"] = dp["sub_category"].str.strip()
    dp["product_key"]  = dp["category"] + " | " + dp["sub_category"]
    dp.drop_duplicates(subset=["product_id"], inplace=True)
    dfs["dim_products"] = dp
    log.info(f"  dim_products transformed: {len(dp):,} rows")

    # ── dim_orders ─────────────────────────────────────────────────────────
    do = dfs["dim_orders"].copy()
    do["channel"]      = do["channel"].str.strip()
    do["status"]       = do["status"].str.strip()
    do["is_cancelled"] = (do["status"] == "Cancelled").astype(int)
    do["is_returned"]  = (do["status"] == "Returned").astype(int)
    do["discount_tier"] = pd.cut(
        do["discount_pct"],
        bins=[-1, 0, 10, 20, 100],
        labels=["no_discount", "low", "medium", "high"]
    )
    dfs["dim_orders"] = do
    log.info(f"  dim_orders transformed: {len(do):,} rows")

    # ── dim_date ───────────────────────────────────────────────────────────
    dd = dfs["dim_date"].copy()
    dd["date"]          = pd.to_datetime(dd["date"])
    dd["date_str"]      = dd["date"].dt.strftime("%Y-%m-%d")
    dd["month_name"]    = dd["date"].dt.month_name()
    dd["quarter_label"] = "Q" + dd["quarter"].astype(str) + " " + dd["year"].astype(str)
    dfs["dim_date"] = dd
    log.info(f"  dim_date transformed: {len(dd):,} rows")

    return dfs


# ═════════════════════════════════════════════════════════════════════════════
# LOAD
# ═════════════════════════════════════════════════════════════════════════════
def load(dfs: dict):
    log.info("── LOAD ─────────────────────────────────────────")
    conn = sqlite3.connect(DB_PATH)

    table_map = {
        "fact_sales":    dfs["fact_sales"],
        "dim_customers": dfs["dim_customers"],
        "dim_products":  dfs["dim_products"],
        "dim_orders":    dfs["dim_orders"],
        "dim_date":      dfs["dim_date"],
    }

    for tname, df in table_map.items():
        df.to_sql(tname, conn, if_exists="replace", index=False)
        log.info(f"  Loaded  {tname}: {len(df):,} rows → {DB_PATH}")

    # Create indexes for performance
    idx_sql = [
        "CREATE INDEX IF NOT EXISTS idx_fs_customer ON fact_sales(customer_id);",
        "CREATE INDEX IF NOT EXISTS idx_fs_product  ON fact_sales(product_id);",
        "CREATE INDEX IF NOT EXISTS idx_fs_order    ON fact_sales(order_id);",
        "CREATE INDEX IF NOT EXISTS idx_fs_date     ON fact_sales(order_date);",
    ]
    for sql in idx_sql:
        conn.execute(sql)
    conn.commit()
    conn.close()
    log.info(f"  Indexes created. ETL complete. DB: {DB_PATH}")


# ═════════════════════════════════════════════════════════════════════════════
# DATA QUALITY REPORT
# ═════════════════════════════════════════════════════════════════════════════
def dq_report(dfs: dict):
    log.info("── DATA QUALITY REPORT ──────────────────────────")
    fs = dfs["fact_sales"]

    print("\n" + "="*55)
    print("  DATA QUALITY REPORT")
    print("="*55)
    print(f"  Total orders       : {len(fs):>10,}")
    print(f"  Valid orders       : {fs['is_valid'].sum():>10,}")
    print(f"  Invalid orders     : {(~fs['is_valid']).sum():>10,}")
    print(f"  Total revenue      : ₹{fs[fs['is_valid']]['total_amount'].sum():>12,.0f}")
    print(f"  Avg order value    : ₹{fs[fs['is_valid']]['total_amount'].mean():>12,.2f}")
    print(f"  Date range         :  {fs['order_date'].min().date()} → {fs['order_date'].max().date()}")
    print(f"  Unique customers   : {fs['customer_id'].nunique():>10,}")
    print(f"  Unique products    : {fs['product_id'].nunique():>10,}")
    print("\n  Revenue band distribution:")
    print(fs["revenue_band"].value_counts().to_string())
    print("="*55)


# ═════════════════════════════════════════════════════════════════════════════
# MAIN
# ═════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    start = datetime.now()
    log.info("ETL pipeline started")

    raw_dfs       = extract()
    clean_dfs     = transform(raw_dfs)
    dq_report(clean_dfs)
    load(clean_dfs)

    elapsed = (datetime.now() - start).total_seconds()
    log.info(f"ETL pipeline completed in {elapsed:.1f}s ✅")
