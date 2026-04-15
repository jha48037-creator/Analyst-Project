"""
STEP 1 — DATA GENERATOR
Generates realistic synthetic sales data matching the star schema.
Run this first to create all CSV files used in the project.
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
import random

np.random.seed(42)
random.seed(42)

OUTPUT_DIR = "data/raw"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ─── CONFIG ──────────────────────────────────────────────────────────────────
START_DATE = datetime(2022, 1, 1)
END_DATE   = datetime(2024, 12, 31)
NUM_CUSTOMERS = 500
NUM_PRODUCTS  = 80
NUM_ORDERS    = 20000

# ─── dim_customers ────────────────────────────────────────────────────────────
cities     = ["Delhi", "Mumbai", "Bangalore", "Hyderabad", "Chennai",
              "Kolkata", "Pune", "Ahmedabad", "Jaipur", "Lucknow"]
tiers      = ["Bronze", "Silver", "Gold", "Platinum"]
age_groups = ["18-25", "26-35", "36-45", "46-55", "55+"]

customers = pd.DataFrame({
    "customer_id": [f"C{str(i).zfill(4)}" for i in range(1, NUM_CUSTOMERS + 1)],
    "city":        np.random.choice(cities, NUM_CUSTOMERS),
    "tier":        np.random.choice(tiers,  NUM_CUSTOMERS, p=[0.45, 0.30, 0.18, 0.07]),
    "age_group":   np.random.choice(age_groups, NUM_CUSTOMERS),
})
customers.to_csv(f"{OUTPUT_DIR}/dim_customers.csv", index=False)
print(f"✔  dim_customers: {len(customers):,} rows")

# ─── dim_products ─────────────────────────────────────────────────────────────
product_catalog = {
    "Electronics":   ["Smartphones", "Laptops", "Tablets", "Headphones", "Cameras"],
    "Clothing":      ["Men's Wear", "Women's Wear", "Footwear", "Accessories"],
    "Home & Kitchen":["Appliances", "Cookware", "Furniture", "Decor"],
    "Sports":        ["Fitness Equipment", "Outdoor Gear", "Sportswear"],
    "Books":         ["Fiction", "Non-Fiction", "Academic", "Comics"],
}

rows = []
pid  = 1
for cat, subs in product_catalog.items():
    per_sub = NUM_PRODUCTS // sum(len(v) for v in product_catalog.values())
    for sub in subs:
        for _ in range(max(3, per_sub)):
            rows.append({
                "product_id":   f"P{str(pid).zfill(4)}",
                "category":     cat,
                "sub_category": sub,
            })
            pid += 1

products = pd.DataFrame(rows[:NUM_PRODUCTS])
products.to_csv(f"{OUTPUT_DIR}/dim_products.csv", index=False)
print(f"✔  dim_products: {len(products):,} rows")

# ─── dim_orders ───────────────────────────────────────────────────────────────
channels  = ["Online", "Retail", "Mobile App", "Marketplace"]
statuses  = ["Delivered", "Shipped", "Cancelled", "Returned", "Pending"]
status_p  = [0.72, 0.12, 0.07, 0.06, 0.03]

orders = pd.DataFrame({
    "order_id":    [f"O{str(i).zfill(6)}" for i in range(1, NUM_ORDERS + 1)],
    "channel":     np.random.choice(channels, NUM_ORDERS, p=[0.40, 0.25, 0.25, 0.10]),
    "discount_pct":np.random.choice([0, 5, 10, 15, 20, 25, 30], NUM_ORDERS,
                                     p=[0.35, 0.20, 0.20, 0.10, 0.08, 0.05, 0.02]),
    "status":      np.random.choice(statuses, NUM_ORDERS, p=status_p),
})
orders.to_csv(f"{OUTPUT_DIR}/dim_orders.csv", index=False)
print(f"✔  dim_orders: {len(orders):,} rows")

# ─── dim_date ─────────────────────────────────────────────────────────────────
date_range = pd.date_range(START_DATE, END_DATE)
dim_date   = pd.DataFrame({
    "date":    date_range,
    "month":   date_range.month,
    "year":    date_range.year,
    "quarter": date_range.quarter,
    "week":    date_range.isocalendar().week.values,
    "day_name":date_range.day_name(),
    "is_weekend": date_range.dayofweek >= 5,
})
dim_date.to_csv(f"{OUTPUT_DIR}/dim_date.csv", index=False)
print(f"✔  dim_date: {len(dim_date):,} rows")

# ─── fact_sales ───────────────────────────────────────────────────────────────
# Price map by category (realistic ranges)
price_map = {
    "Electronics":    (5000,  80000),
    "Clothing":       (500,   5000),
    "Home & Kitchen": (800,   25000),
    "Sports":         (600,   15000),
    "Books":          (200,   1500),
}

# Seasonal weights (simulate higher sales in Nov–Dec)
def seasonal_weight(date):
    m = date.month
    return {11: 3.0, 12: 3.5, 10: 1.8, 1: 0.7, 2: 0.6}.get(m, 1.0)

all_dates = [START_DATE + timedelta(days=random.randint(0, (END_DATE - START_DATE).days))
             for _ in range(NUM_ORDERS)]

fact_rows = []
for i, order_id in enumerate(orders["order_id"]):
    product   = products.sample(1).iloc[0]
    customer  = customers.sample(1).iloc[0]
    order_row = orders.iloc[i]
    order_dt  = all_dates[i]

    lo, hi    = price_map[product["category"]]
    unit_price = round(random.uniform(lo, hi), 2)
    qty        = np.random.choice([1, 2, 3, 4, 5], p=[0.50, 0.25, 0.13, 0.08, 0.04])
    discount   = order_row["discount_pct"] / 100
    total      = round(unit_price * qty * (1 - discount), 2)

    fact_rows.append({
        "order_id":    order_id,
        "customer_id": customer["customer_id"],
        "product_id":  product["product_id"],
        "order_date":  order_dt.strftime("%Y-%m-%d"),
        "qty":         qty,
        "unit_price":  unit_price,
        "total_amount":total,
    })

fact_sales = pd.DataFrame(fact_rows)
fact_sales.to_csv(f"{OUTPUT_DIR}/fact_sales.csv", index=False)
print(f"✔  fact_sales: {len(fact_sales):,} rows")
print(f"\n✅ All CSV files saved to '{OUTPUT_DIR}/'")
print(f"   Total revenue: ₹{fact_sales['total_amount'].sum():,.0f}")
print(f"   Date range:    {fact_sales['order_date'].min()} → {fact_sales['order_date'].max()}")
