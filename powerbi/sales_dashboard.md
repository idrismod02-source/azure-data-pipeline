# Power BI Sales Dashboard — Documentation

## Overview

The Power BI dashboard connects to the Gold Delta tables via an Azure Synapse Analytics
SQL endpoint. It provides four report pages covering executive KPIs, revenue trends,
regional performance, and customer segmentation.

---

## Data Sources

| Gold Table | Synapse External Table | Refresh |
|---|---|---|
| `gold/daily_revenue` | `gold_db.daily_revenue` | Every 6 hours |
| `gold/monthly_trend` | `gold_db.monthly_trend` | Every 6 hours |
| `gold/customer_segments` | `gold_db.customer_segments` | Daily |
| `gold/product_performance` | `gold_db.product_performance` | Daily |

**Connection string (Synapse SQL endpoint):**
```
Server: <synapse-workspace>.sql.azuresynapse.net
Database: gold_db
Authentication: Azure Active Directory
```

---

## DAX Measures

### Revenue Measures

```dax
-- Total Net Revenue
Total Net Revenue =
    SUMX(
        daily_revenue,
        daily_revenue[net_revenue]
    )

-- Revenue MTD
Revenue MTD =
    TOTALMTD([Total Net Revenue], 'Date'[Date])

-- Revenue YTD
Revenue YTD =
    TOTALYTD([Total Net Revenue], 'Date'[Date])

-- MoM Revenue Growth %
MoM Growth % =
VAR CurrentMonth = [Total Net Revenue]
VAR PrevMonth =
    CALCULATE(
        [Total Net Revenue],
        DATEADD('Date'[Date], -1, MONTH)
    )
RETURN
    DIVIDE(CurrentMonth - PrevMonth, PrevMonth, 0)

-- Rolling 30-Day Average Order Value
Rolling 30D AOV =
    AVERAGEX(
        DATESINPERIOD('Date'[Date], LASTDATE('Date'[Date]), -30, DAY),
        [Total Net Revenue] / [Total Orders]
    )
```

### Order Measures

```dax
-- Total Orders
Total Orders =
    SUM(daily_revenue[completed_orders])

-- Refund Rate %
Refund Rate % =
    DIVIDE(
        SUM(daily_revenue[refunded_orders]),
        SUM(daily_revenue[completed_orders]) + SUM(daily_revenue[refunded_orders]),
        0
    ) * 100

-- Average Order Value
Avg Order Value =
    DIVIDE([Total Net Revenue], [Total Orders], 0)
```

### Customer Measures

```dax
-- Total Unique Customers
Unique Customers =
    DISTINCTCOUNT(customer_segments[customer_id])

-- Champions Count
Champions =
    CALCULATE(
        COUNTROWS(customer_segments),
        customer_segments[customer_segment] = "Champion"
    )

-- Customer Retention Rate (repeat buyers)
Retention Rate % =
    DIVIDE(
        COUNTROWS(FILTER(customer_segments, customer_segments[frequency] > 1)),
        COUNTROWS(customer_segments),
        0
    ) * 100
```

---

## Report Pages

### Page 1 — Executive Summary
- **KPI Cards:** Total Revenue, Total Orders, Avg Order Value, Unique Customers
- **Trend Line:** Monthly revenue (current vs prior year)
- **Bar Chart:** Revenue by product category
- **Slicer:** Date range, Region

### Page 2 — Regional Performance
- **Map Visual:** Net revenue by US region (filled map)
- **Table:** Region × Category revenue breakdown with conditional formatting
- **Bar Chart:** Top 10 products by region

### Page 3 — Revenue Trends
- **Line Chart:** Daily revenue with 7-day moving average
- **Area Chart:** MoM growth % by region
- **Combo Chart:** Orders vs Revenue (dual axis)

### Page 4 — Customer Segments
- **Donut Chart:** Customer segment distribution (Champion / Loyal / At Risk / Lost)
- **Scatter Plot:** Frequency vs Monetary value (RFM bubble chart)
- **Table:** Customer segment counts with avg order value

---

## Refresh Schedule

| Dataset | Schedule | Trigger |
|---|---|---|
| Sales Dashboard | Every 6 hours (6am, 12pm, 6pm, 12am UTC) | Power BI Service scheduled refresh |
| Customer Segments | Daily at 7am UTC | ADF pipeline completion event |

**Row-level security (RLS):** Applied by region so regional managers
only see their own data. Managed via Azure AD groups.

---

## Setup Instructions

1. Open `sales_dashboard.pbix` in Power BI Desktop
2. In **Home → Transform data → Data source settings**, update the
   Synapse connection string to your workspace
3. Sign in with Azure AD credentials
4. Click **Refresh** to pull live data
5. Publish to Power BI Service workspace
6. Configure scheduled refresh with service principal credentials
