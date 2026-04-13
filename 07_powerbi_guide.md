# STEP 7 — POWER BI GUIDE
## Complete setup: data model, relationships, DAX measures & dashboard design

---

## 1. CONNECT TO DATA

### Option A — SQLite (from ETL output)
1. Power BI Desktop → **Get Data** → **ODBC**
2. DSN: point to `data/processed/sales_dw.db`
3. Import all 5 tables: `fact_sales`, `dim_customers`, `dim_products`, `dim_orders`, `dim_date`

### Option B — Direct CSV import
1. **Get Data → Text/CSV**
2. Import each file from `data/raw/`
3. Use Power Query to apply transforms (see Section 3)

---

## 2. DATA MODEL (Star Schema)

```
dim_customers ──┐
dim_products  ──┤── fact_sales ──── dim_orders
dim_date      ──┘
```

### Relationships to create (Manage Relationships):
| From table       | Column      | To table         | Column      | Cardinality |
|-----------------|-------------|-----------------|-------------|-------------|
| fact_sales       | customer_id | dim_customers    | customer_id | Many-to-1   |
| fact_sales       | product_id  | dim_products     | product_id  | Many-to-1   |
| fact_sales       | order_id    | dim_orders       | order_id    | Many-to-1   |
| fact_sales       | order_date  | dim_date         | date        | Many-to-1   |

**Set Cross-filter direction = Single (from fact to dim) for all relationships.**

---

## 3. POWER QUERY TRANSFORMS
Open **Transform Data** and apply these steps:

### fact_sales
```m
// Change types
= Table.TransformColumnTypes(Source, {
    {"order_date",   type date},
    {"qty",          Int64.Type},
    {"unit_price",   type number},
    {"total_amount", type number}
})
```

### dim_customers
```m
// Add Tier Rank column
= Table.AddColumn(#"Changed Type", "TierRank",
    each if [tier] = "Platinum" then 4
         else if [tier] = "Gold"   then 3
         else if [tier] = "Silver" then 2
         else 1, Int64.Type)
```

### dim_date (if not already in DB — create in Power Query)
```m
let
    StartDate = #date(2022, 1, 1),
    EndDate   = #date(2024, 12, 31),
    NoDays    = Duration.Days(EndDate - StartDate) + 1,
    DateList  = List.Dates(StartDate, NoDays, #duration(1,0,0,0)),
    Table     = Table.FromList(DateList, Splitter.SplitByNothing(), {"date"}),
    AddYear   = Table.AddColumn(Table, "Year",    each Date.Year([date]),    Int64.Type),
    AddMonth  = Table.AddColumn(AddYear,"Month",  each Date.Month([date]),   Int64.Type),
    AddQ      = Table.AddColumn(AddMonth,"Quarter",each Date.QuarterOfYear([date]),Int64.Type),
    AddMName  = Table.AddColumn(AddQ,  "MonthName",each Date.MonthName([date]),  type text),
    AddQLabel = Table.AddColumn(AddMName,"QuarterLabel",
                    each "Q" & Text.From(Date.QuarterOfYear([date]))
                         & " " & Text.From(Date.Year([date])),  type text)
in AddQLabel
```

---

## 4. DAX MEASURES

### 4.1 Core Revenue Measures
```dax
// ─── Base measures ───────────────────────────────────────────────────────
Total Revenue =
    SUMX(fact_sales, fact_sales[qty] * fact_sales[unit_price] *
         (1 - RELATED(dim_orders[discount_pct]) / 100))

Total Orders =
    DISTINCTCOUNT(fact_sales[order_id])

Total Customers =
    DISTINCTCOUNT(fact_sales[customer_id])

Units Sold =
    SUM(fact_sales[qty])

AOV =                                       -- Avg Order Value
    DIVIDE([Total Revenue], [Total Orders])

Revenue per Customer =
    DIVIDE([Total Revenue], [Total Customers])


// ─── Time intelligence ────────────────────────────────────────────────────
Revenue PY =
    CALCULATE([Total Revenue],
        SAMEPERIODLASTYEAR(dim_date[date]))

Revenue YTD =
    TOTALYTD([Total Revenue], dim_date[date])

Revenue PY YTD =
    CALCULATE([Revenue YTD],
        SAMEPERIODLASTYEAR(dim_date[date]))

YoY Revenue Growth % =
    DIVIDE([Total Revenue] - [Revenue PY], [Revenue PY])

MoM Revenue Growth % =
    VAR CurrRev = [Total Revenue]
    VAR PrevRev = CALCULATE([Total Revenue],
                    DATEADD(dim_date[date], -1, MONTH))
    RETURN DIVIDE(CurrRev - PrevRev, PrevRev)

Revenue 3M Rolling =
    CALCULATE([Total Revenue],
        DATESINPERIOD(dim_date[date], LASTDATE(dim_date[date]), -3, MONTH))
```

### 4.2 Customer KPIs
```dax
New Customers =
    VAR CurrentPeriodMin = MIN(dim_date[date])
    RETURN
        CALCULATE(
            DISTINCTCOUNT(fact_sales[customer_id]),
            FILTER(fact_sales,
                CALCULATE(MIN(fact_sales[order_date]),
                    ALL(dim_date)) >= CurrentPeriodMin
            )
        )

Returning Customers =
    [Total Customers] - [New Customers]

Customer Retention Rate % =
    DIVIDE([Returning Customers], [Total Customers])

Repeat Purchase Rate % =
    VAR CustomersWithMultipleOrders =
        COUNTROWS(
            FILTER(
                SUMMARIZE(fact_sales, fact_sales[customer_id],
                    "OrderCount", DISTINCTCOUNT(fact_sales[order_id])),
                [OrderCount] > 1
            )
        )
    RETURN DIVIDE(CustomersWithMultipleOrders, [Total Customers])

Avg Orders per Customer =
    DIVIDE([Total Orders], [Total Customers])
```

### 4.3 Product KPIs
```dax
Category Revenue % =
    DIVIDE([Total Revenue],
        CALCULATE([Total Revenue], ALL(dim_products[category])))

Sub-Category Rank =
    RANKX(ALL(dim_products[sub_category]), [Total Revenue],, DESC)

Revenue Contribution % =
    DIVIDE([Total Revenue],
        CALCULATE([Total Revenue], ALL(dim_products)))
```

### 4.4 Order / Operations KPIs
```dax
Cancellation Rate % =
    DIVIDE(
        CALCULATE([Total Orders],
            dim_orders[status] = "Cancelled"),
        [Total Orders]
    )

Return Rate % =
    DIVIDE(
        CALCULATE([Total Orders],
            dim_orders[status] = "Returned"),
        [Total Orders]
    )

Fulfillment Rate % =
    DIVIDE(
        CALCULATE([Total Orders],
            dim_orders[status] = "Delivered"),
        [Total Orders]
    )

Discount Leakage =
    SUMX(fact_sales,
        fact_sales[qty] * fact_sales[unit_price]
            * (RELATED(dim_orders[discount_pct]) / 100))

Effective Discount % =
    DIVIDE([Discount Leakage],
        SUMX(fact_sales, fact_sales[qty] * fact_sales[unit_price]))
```

---

## 5. DASHBOARD PAGES — RECOMMENDED LAYOUT

### Page 1: Executive Summary
| Visual              | Type               | Fields                                    |
|--------------------|--------------------|-------------------------------------------|
| Revenue card        | KPI Card           | Total Revenue, YoY%                       |
| Orders card         | KPI Card           | Total Orders, YoY%                        |
| AOV card            | KPI Card           | AOV, YoY%                                 |
| Monthly trend       | Line Chart         | dim_date[MonthName], Total Revenue, PY    |
| Revenue by category | Donut Chart        | category, Total Revenue                   |
| YTD progress        | Gauge              | Revenue YTD vs target                     |

### Page 2: Customer Insights
| Visual              | Type               | Fields                                    |
|--------------------|--------------------|-------------------------------------------|
| New vs Returning    | Clustered Bar      | Months, New Customers, Returning Customers|
| Tier breakdown      | Stacked Bar        | tier, Total Revenue, Orders               |
| City map            | Map / Filled Map   | city, Total Revenue                       |
| Age group analysis  | Bar Chart          | age_group, Revenue, AOV                   |
| RFM table           | Table              | customer_id, Orders, LTV, Last Order      |

### Page 3: Product Performance
| Visual              | Type               | Fields                                    |
|--------------------|--------------------|-------------------------------------------|
| Category matrix     | Matrix             | category, sub_category, Revenue, Units    |
| Top sub-categories  | Bar Chart (sorted) | sub_category, Total Revenue               |
| Discount impact     | Scatter Plot       | discount_pct, AOV, bubble size = Orders   |
| YoY by category     | Clustered Bar      | category, Revenue 2022/2023/2024          |

### Page 4: Operations
| Visual              | Type               | Fields                                    |
|--------------------|--------------------|-------------------------------------------|
| Fulfillment KPIs    | KPI Cards          | Fulfillment Rate, Cancel Rate, Return Rate|
| Channel performance | Donut              | channel, Orders                           |
| Status distribution | Stacked Bar        | channel, status, Orders                   |
| Discount leakage    | Waterfall Chart    | Gross Revenue, Discount, Net Revenue      |

---

## 6. FORMATTING TIPS

- **Theme:** Use a custom corporate theme (JSON) or built-in "Executive" theme
- **Slicers:** Add Year, Quarter, Category, City, Tier slicers on every page
- **Conditional formatting:** Apply to AOV and Revenue columns in tables
  - Green if > last year, Red if < last year
- **Drill-through:** Year → Quarter → Month on all time charts
- **Bookmarks:** Create "2022", "2023", "2024" bookmarks for one-click year view
- **Tooltips:** Add custom tooltip pages with mini sparklines per visual

---

## 7. PUBLISH & SHARE
1. **File → Publish → Power BI Service**
2. Create a **Dashboard** by pinning visuals from reports
3. Set **Scheduled Refresh** if connected to live database
4. Share via **workspace** or **embed** in SharePoint/Teams
