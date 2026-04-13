"""
STEP 6 — KPI DASHBOARD
Calculates 25+ business KPIs across revenue, customers, products,
and operations. Outputs a clean summary table and individual KPI cards.
"""

import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime

conn = sqlite3.connect("data/processed/sales_dw.db")

# ─── LOAD ──────────────────────────────────────────────────────────────────────
fs = pd.read_sql("SELECT * FROM fact_sales",    conn)
dc = pd.read_sql("SELECT * FROM dim_customers", conn)
dp = pd.read_sql("SELECT * FROM dim_products",  conn)
do = pd.read_sql("SELECT * FROM dim_orders",    conn)
conn.close()

fs["order_date"] = pd.to_datetime(fs["order_date"])
df = (fs.merge(dc, on="customer_id", how="left")
        .merge(dp, on="product_id",  how="left")
        .merge(do, on="order_id",    how="left"))

valid = df[df["is_valid"] == 1]

# Current year and previous year
CY = 2024
PY = 2023
cy_df = valid[valid["order_year"] == CY]
py_df = valid[valid["order_year"] == PY]

def pct_change(curr, prev):
    if prev == 0:
        return None
    return round((curr - prev) / prev * 100, 2)

def fmt_inr(val):
    if val >= 1e7:
        return f"₹{val/1e7:.2f} Cr"
    elif val >= 1e5:
        return f"₹{val/1e5:.2f} L"
    else:
        return f"₹{val:,.0f}"


print("\n" + "═"*70)
print("  SALES KPI DASHBOARD  |  2024 vs 2023")
print("═"*70)


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — REVENUE KPIs
# ══════════════════════════════════════════════════════════════════════════════
print("\n  📊 REVENUE KPIs")
print("  " + "─"*65)

# 1.1 Total Revenue
cy_rev = cy_df["total_amount"].sum()
py_rev = py_df["total_amount"].sum()
print(f"  {'Total Revenue (CY)':<40} {fmt_inr(cy_rev):>15}   {pct_change(cy_rev, py_rev):>+.1f}% YoY")

# 1.2 Avg Order Value (AOV)
cy_aov = cy_df["total_amount"].mean()
py_aov = py_df["total_amount"].mean()
print(f"  {'Avg Order Value (AOV)':<40} {fmt_inr(cy_aov):>15}   {pct_change(cy_aov, py_aov):>+.1f}% YoY")

# 1.3 Revenue per Customer
cy_rpc = cy_rev / cy_df["customer_id"].nunique()
py_rpc = py_rev / py_df["customer_id"].nunique()
print(f"  {'Revenue per Customer':<40} {fmt_inr(cy_rpc):>15}   {pct_change(cy_rpc, py_rpc):>+.1f}% YoY")

# 1.4 Gross Revenue vs Net Revenue (post-discount)
cy_gross = (cy_df["qty"] * cy_df["unit_price"]).sum()
cy_net   = cy_df["total_amount"].sum()
discount_leakage = cy_gross - cy_net
print(f"  {'Gross Revenue':<40} {fmt_inr(cy_gross):>15}")
print(f"  {'Net Revenue (post-discount)':<40} {fmt_inr(cy_net):>15}")
print(f"  {'Discount Leakage':<40} {fmt_inr(discount_leakage):>15}   ({discount_leakage/cy_gross*100:.1f}% of gross)")

# 1.5 Monthly Recurring Revenue estimate
cy_mrr = cy_rev / 12
print(f"  {'Avg Monthly Revenue':<40} {fmt_inr(cy_mrr):>15}")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — ORDER KPIs
# ══════════════════════════════════════════════════════════════════════════════
print("\n  🛒 ORDER KPIs")
print("  " + "─"*65)

cy_orders = cy_df["order_id"].nunique()
py_orders = py_df["order_id"].nunique()
print(f"  {'Total Orders':<40} {cy_orders:>15,}   {pct_change(cy_orders, py_orders):>+.1f}% YoY")

# 2.2 Cancellation Rate
cy_cancel = (do[do["order_id"].isin(cy_df["order_id"]) &
                (do["status"] == "Cancelled")]["order_id"].nunique())
cy_cancel_rate = cy_cancel / cy_orders * 100
print(f"  {'Cancellation Rate':<40} {cy_cancel_rate:>14.1f}%")

# 2.3 Return Rate
cy_return = (do[do["order_id"].isin(cy_df["order_id"]) &
                (do["status"] == "Returned")]["order_id"].nunique())
cy_return_rate = cy_return / cy_orders * 100
print(f"  {'Return Rate':<40} {cy_return_rate:>14.1f}%")

# 2.4 Fulfillment Rate (Delivered)
cy_delivered = (do[do["order_id"].isin(cy_df["order_id"]) &
                   (do["status"] == "Delivered")]["order_id"].nunique())
fulfillment_rate = cy_delivered / cy_orders * 100
print(f"  {'Fulfillment Rate':<40} {fulfillment_rate:>14.1f}%")

# 2.5 Orders per Channel
print(f"\n  {'Channel':<25} {'Orders':>10} {'Revenue':>15} {'% Revenue':>10}")
print("  " + "─"*65)
for ch, grp in cy_df.groupby("channel"):
    ch_rev  = grp["total_amount"].sum()
    ch_ord  = grp["order_id"].nunique()
    pct     = ch_rev / cy_rev * 100
    print(f"  {ch:<25} {ch_ord:>10,} {fmt_inr(ch_rev):>15} {pct:>9.1f}%")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — CUSTOMER KPIs
# ══════════════════════════════════════════════════════════════════════════════
print("\n  👥 CUSTOMER KPIs")
print("  " + "─"*65)

# 3.1 Total Unique Customers
cy_cust = cy_df["customer_id"].nunique()
py_cust = py_df["customer_id"].nunique()
print(f"  {'Active Customers':<40} {cy_cust:>15,}   {pct_change(cy_cust, py_cust):>+.1f}% YoY")

# 3.2 New vs Returning
first_order = (valid.groupby("customer_id")["order_date"].min()
                    .reset_index()
                    .rename(columns={"order_date": "first_order_date"}))
cy_df2 = cy_df.merge(first_order, on="customer_id", how="left")
new_cust = cy_df2[cy_df2["first_order_date"].dt.year == CY]["customer_id"].nunique()
returning = cy_cust - new_cust
print(f"  {'New Customers':<40} {new_cust:>15,}   ({new_cust/cy_cust*100:.1f}%)")
print(f"  {'Returning Customers':<40} {returning:>15,}   ({returning/cy_cust*100:.1f}%)")

# 3.3 Customer Lifetime Value (CLV)
clv = (valid.groupby("customer_id")["total_amount"].sum())
print(f"  {'Avg Customer Lifetime Value':<40} {fmt_inr(clv.mean()):>15}")
print(f"  {'Median Customer LTV':<40} {fmt_inr(clv.median()):>15}")
print(f"  {'Top 10% Customer LTV threshold':<40} {fmt_inr(clv.quantile(0.9)):>15}")

# 3.4 Repeat Purchase Rate
purchase_counts = valid.groupby("customer_id")["order_id"].nunique()
repeat_rate = (purchase_counts > 1).sum() / len(purchase_counts) * 100
print(f"  {'Repeat Purchase Rate':<40} {repeat_rate:>14.1f}%")

# 3.5 Avg Orders per Customer
avg_orders_cust = purchase_counts.mean()
print(f"  {'Avg Orders per Customer':<40} {avg_orders_cust:>15.2f}")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — PRODUCT KPIs
# ══════════════════════════════════════════════════════════════════════════════
print("\n  📦 PRODUCT KPIs")
print("  " + "─"*65)

# 4.1 Revenue by category
cat_rev = cy_df.groupby("category")["total_amount"].sum().sort_values(ascending=False)
print("  Top category by revenue:")
for cat, rev in cat_rev.items():
    pct = rev / cy_rev * 100
    bar = "█" * int(pct / 2)
    print(f"    {cat:<20} {fmt_inr(rev):>12}  ({pct:5.1f}%)  {bar}")

# 4.2 Units per order
upo = cy_df.groupby("order_id")["qty"].sum().mean()
print(f"\n  {'Units per Order (avg)':<40} {upo:>15.2f}")

# 4.3 Best performing sub-category
best_sub = (cy_df.groupby("sub_category")["total_amount"]
              .sum()
              .sort_values(ascending=False)
              .head(3))
print("  Top 3 sub-categories:")
for sub, rev in best_sub.items():
    print(f"    {sub:<30}  {fmt_inr(rev)}")

# 4.4 Discount effect on avg order
disc_effect = cy_df.groupby("discount_pct")["total_amount"].mean().reset_index()
print(f"\n  Avg order value at key discount levels:")
for _, row in disc_effect[disc_effect["discount_pct"].isin([0, 10, 20, 30])].iterrows():
    print(f"    Discount {int(row['discount_pct'])}%: {fmt_inr(row['total_amount'])}")

print("\n" + "═"*70)
print("  KPI calculation complete ✅")
print("═"*70)
