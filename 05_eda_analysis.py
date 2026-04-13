"""
STEP 5 — EXPLORATORY DATA ANALYSIS (EDA)
Full profiling, distribution, correlation, time-series,
and customer/product/channel insights.
Outputs charts to output/eda/
"""

import pandas as pd
import numpy as np
import sqlite3
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import seaborn as sns
import warnings
import os

warnings.filterwarnings("ignore")
os.makedirs("output/eda", exist_ok=True)

# ─── STYLE ────────────────────────────────────────────────────────────────────
sns.set_theme(style="whitegrid", palette="muted")
plt.rcParams.update({
    "figure.dpi": 120,
    "figure.figsize": (12, 6),
    "axes.spines.top": False,
    "axes.spines.right": False,
    "font.size": 11,
})
COLORS = sns.color_palette("muted", 10)

# ─── LOAD DATA ────────────────────────────────────────────────────────────────
conn = sqlite3.connect("data/processed/sales_dw.db")
fs   = pd.read_sql("SELECT * FROM fact_sales",    conn)
dc   = pd.read_sql("SELECT * FROM dim_customers", conn)
dp   = pd.read_sql("SELECT * FROM dim_products",  conn)
do_  = pd.read_sql("SELECT * FROM dim_orders",    conn)
conn.close()

fs["order_date"] = pd.to_datetime(fs["order_date"])

# ─── MERGE for analysis ────────────────────────────────────────────────────────
df = (fs
      .merge(dc,  on="customer_id", how="left")
      .merge(dp,  on="product_id",  how="left")
      .merge(do_, on="order_id",    how="left"))

print("="*60)
print("  EDA SUMMARY")
print("="*60)
print(df.describe(include="all").T[["count", "unique", "mean", "std", "min", "max"]])


# ══════════════════════════════════════════════════════════════════════════════
# 1. UNIVARIATE — total_amount distribution
# ══════════════════════════════════════════════════════════════════════════════
fig, axes = plt.subplots(1, 2, figsize=(14, 5))
axes[0].hist(df["total_amount"], bins=60, color=COLORS[0], edgecolor="white")
axes[0].set_title("Order Amount Distribution")
axes[0].set_xlabel("Total Amount (₹)")
axes[0].xaxis.set_major_formatter(mtick.FuncFormatter(lambda x, _: f"₹{x:,.0f}"))

axes[1].hist(np.log1p(df["total_amount"]), bins=60, color=COLORS[1], edgecolor="white")
axes[1].set_title("Log-Transformed Order Amount")
axes[1].set_xlabel("log(Total Amount + 1)")

plt.tight_layout()
plt.savefig("output/eda/01_amount_distribution.png")
plt.close()
print("  ✔  Saved 01_amount_distribution.png")


# ══════════════════════════════════════════════════════════════════════════════
# 2. TIME SERIES — monthly revenue
# ══════════════════════════════════════════════════════════════════════════════
monthly = (df.groupby(df["order_date"].dt.to_period("M"))["total_amount"]
             .sum()
             .reset_index())
monthly["order_date"] = monthly["order_date"].dt.to_timestamp()

fig, ax = plt.subplots(figsize=(15, 5))
ax.plot(monthly["order_date"], monthly["total_amount"] / 1e6,
        color=COLORS[0], linewidth=2, marker="o", markersize=3)
ax.fill_between(monthly["order_date"], monthly["total_amount"] / 1e6,
                alpha=0.15, color=COLORS[0])
ax.set_title("Monthly Revenue (2022–2024)")
ax.set_ylabel("Revenue (₹ Millions)")
ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda x, _: f"₹{x:.1f}M"))
plt.tight_layout()
plt.savefig("output/eda/02_monthly_revenue_trend.png")
plt.close()
print("  ✔  Saved 02_monthly_revenue_trend.png")


# ══════════════════════════════════════════════════════════════════════════════
# 3. CATEGORY ANALYSIS — revenue & volume
# ══════════════════════════════════════════════════════════════════════════════
cat = (df.groupby("category")
         .agg(revenue=("total_amount", "sum"),
              orders=("order_id", "count"))
         .sort_values("revenue", ascending=False))

fig, axes = plt.subplots(1, 2, figsize=(14, 5))
axes[0].barh(cat.index, cat["revenue"] / 1e6, color=COLORS[:len(cat)])
axes[0].set_title("Revenue by Category (₹M)")
axes[0].xaxis.set_major_formatter(mtick.FuncFormatter(lambda x, _: f"₹{x:.0f}M"))

axes[1].pie(cat["orders"], labels=cat.index,
            autopct="%1.1f%%", colors=COLORS[:len(cat)],
            startangle=140, pctdistance=0.82)
axes[1].set_title("Order Volume Share by Category")

plt.tight_layout()
plt.savefig("output/eda/03_category_analysis.png")
plt.close()
print("  ✔  Saved 03_category_analysis.png")


# ══════════════════════════════════════════════════════════════════════════════
# 4. CUSTOMER SEGMENTATION — tier & city heatmap
# ══════════════════════════════════════════════════════════════════════════════
tier_city = (df.groupby(["city", "tier"])["total_amount"]
               .sum()
               .unstack()
               .fillna(0))

tier_order = ["Bronze", "Silver", "Gold", "Platinum"]
tier_city  = tier_city.reindex(columns=[c for c in tier_order if c in tier_city.columns])

fig, ax = plt.subplots(figsize=(12, 7))
sns.heatmap(tier_city / 1e6, annot=True, fmt=".1f", cmap="YlOrRd",
            linewidths=0.5, ax=ax,
            annot_kws={"size": 9})
ax.set_title("Revenue (₹M) by City × Customer Tier")
ax.set_xlabel("Customer Tier")
ax.set_ylabel("City")
plt.tight_layout()
plt.savefig("output/eda/04_tier_city_heatmap.png")
plt.close()
print("  ✔  Saved 04_tier_city_heatmap.png")


# ══════════════════════════════════════════════════════════════════════════════
# 5. CHANNEL vs STATUS — stacked bar
# ══════════════════════════════════════════════════════════════════════════════
ch_status = (df.groupby(["channel", "status"])["order_id"]
               .count()
               .unstack()
               .fillna(0))

ch_status.plot(kind="bar", stacked=True, figsize=(12, 6),
               color=COLORS[:len(ch_status.columns)], edgecolor="white")
plt.title("Order Status Distribution by Channel")
plt.xlabel("Channel")
plt.ylabel("Number of Orders")
plt.legend(title="Status", bbox_to_anchor=(1.01, 1))
plt.xticks(rotation=0)
plt.tight_layout()
plt.savefig("output/eda/05_channel_status.png")
plt.close()
print("  ✔  Saved 05_channel_status.png")


# ══════════════════════════════════════════════════════════════════════════════
# 6. CORRELATION MATRIX — numeric features
# ══════════════════════════════════════════════════════════════════════════════
num_cols = ["qty", "unit_price", "total_amount", "discount_pct"]
corr     = df[num_cols].dropna().corr()

fig, ax = plt.subplots(figsize=(7, 6))
sns.heatmap(corr, annot=True, fmt=".2f", cmap="coolwarm",
            vmin=-1, vmax=1, ax=ax, square=True, linewidths=0.5)
ax.set_title("Correlation Matrix — Numeric Features")
plt.tight_layout()
plt.savefig("output/eda/06_correlation_matrix.png")
plt.close()
print("  ✔  Saved 06_correlation_matrix.png")


# ══════════════════════════════════════════════════════════════════════════════
# 7. BOXPLOT — AOV by tier
# ══════════════════════════════════════════════════════════════════════════════
tier_order = ["Bronze", "Silver", "Gold", "Platinum"]
valid      = df[df["tier"].isin(tier_order)]

fig, ax = plt.subplots(figsize=(10, 6))
sns.boxplot(data=valid, x="tier", y="total_amount",
            order=tier_order, palette="muted", ax=ax)
ax.set_title("Order Value Distribution by Customer Tier")
ax.set_xlabel("Customer Tier")
ax.set_ylabel("Order Amount (₹)")
ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda x, _: f"₹{x:,.0f}"))
plt.tight_layout()
plt.savefig("output/eda/07_aov_by_tier.png")
plt.close()
print("  ✔  Saved 07_aov_by_tier.png")


# ══════════════════════════════════════════════════════════════════════════════
# 8. YOY REVENUE by CATEGORY — grouped bars
# ══════════════════════════════════════════════════════════════════════════════
yoy = (df.groupby(["category", "order_year"])["total_amount"]
         .sum()
         .unstack()
         .fillna(0))

yoy.plot(kind="bar", figsize=(13, 6),
         color=COLORS[:3], edgecolor="white")
plt.title("Year-over-Year Revenue by Category")
plt.xlabel("Category")
plt.ylabel("Revenue (₹)")
plt.xticks(rotation=15, ha="right")
plt.gca().yaxis.set_major_formatter(
    mtick.FuncFormatter(lambda x, _: f"₹{x/1e6:.0f}M"))
plt.legend(title="Year")
plt.tight_layout()
plt.savefig("output/eda/08_yoy_by_category.png")
plt.close()
print("  ✔  Saved 08_yoy_by_category.png")


# ══════════════════════════════════════════════════════════════════════════════
# 9. AGE GROUP ANALYSIS
# ══════════════════════════════════════════════════════════════════════════════
age = (df.groupby("age_group")
         .agg(revenue=("total_amount", "sum"),
              orders=("order_id", "count"),
              avg_order=("total_amount", "mean"))
         .sort_values("revenue", ascending=False))

fig, axes = plt.subplots(1, 3, figsize=(15, 5))
for ax, col, title in zip(
    axes,
    ["revenue", "orders", "avg_order"],
    ["Total Revenue (₹)", "Total Orders", "Avg Order Value (₹)"]
):
    axes_list = ax.bar(age.index, age[col], color=COLORS[:len(age)])
    ax.set_title(title)
    ax.set_xlabel("Age Group")
    ax.tick_params(axis="x", rotation=15)

plt.suptitle("Customer Age Group Analysis", fontsize=13, fontweight="bold", y=1.02)
plt.tight_layout()
plt.savefig("output/eda/09_age_group_analysis.png")
plt.close()
print("  ✔  Saved 09_age_group_analysis.png")


# ══════════════════════════════════════════════════════════════════════════════
# 10. OUTLIER DETECTION — IQR method on total_amount
# ══════════════════════════════════════════════════════════════════════════════
Q1  = df["total_amount"].quantile(0.25)
Q3  = df["total_amount"].quantile(0.75)
IQR = Q3 - Q1
outliers = df[(df["total_amount"] < Q1 - 1.5 * IQR) |
              (df["total_amount"] > Q3 + 1.5 * IQR)]

print(f"\n  Outlier Detection (IQR method):")
print(f"  Q1: ₹{Q1:,.2f}  Q3: ₹{Q3:,.2f}  IQR: ₹{IQR:,.2f}")
print(f"  Lower bound: ₹{Q1 - 1.5*IQR:,.2f}")
print(f"  Upper bound: ₹{Q3 + 1.5*IQR:,.2f}")
print(f"  Outlier rows: {len(outliers):,} ({len(outliers)/len(df)*100:.1f}%)")
print(f"  Outlier revenue: ₹{outliers['total_amount'].sum():,.0f}")

print("\n  ✅ All EDA charts saved to output/eda/")
