# Cohort LTV Analysis

## Overview
Cumulative lifetime value (LTV) analysis on the BigQuery public dataset `thelook_ecommerce`, measuring how much revenue each acquisition cohort generates per user over time.

## Dataset
- **Source:** `bigquery-public-data.thelook_ecommerce`
- **Tables used:** `order_items`
- **Date range:** January 2019 – April 2026

## Methodology
- **Cohort definition:** Month of each user's first order
- **LTV metric:** Cumulative revenue per cohort user, calculated as a running sum of cohort revenue divided by cohort size
- **Cohort size:** Distinct users who placed at least one order in the cohort month (excludes users who signed up but never ordered)
- **Exclusions:** Cancelled and returned orders are excluded from all revenue calculations
- **Two-step aggregation:** Revenue is first summed to cohort/month grain, then a window function accumulates the running total — this avoids aggregating over an aggregate in a single pass

## Key Findings
- LTV at month 0 is consistent across cohorts, ranging from roughly **$87–$119 per user**
- LTV growth steadies to approximately **3% month-over-month** across all cohorts once the initial purchase period passes
- Older cohorts (2024, 2025) show higher absolute LTV than newer ones simply due to more elapsed time, not higher per-order value
- LTV curves are smooth with no sharp acceleration, confirming the retention finding: a small but consistent subset of users continues purchasing at a low but steady rate over years
- Average order value appears stable across cohorts — cohort size differences do not meaningfully change per-user revenue

## Query Structure
| CTE | Purpose |
|-----|---------|
| `userFirstPurchase` | Finds each user's first order date |
| `orderItems` | Aggregates order items to order grain with total sale price |
| `cohort` | Joins orders to first purchase date, assigns cohort and order month |
| `orderMonthCohortSales` | Aggregates total sales to cohort/month grain |
| `cohortSize` | Counts distinct users per cohort |
| `LTVtable` | Computes running total and running LTV via window functions |
| Final SELECT | Adds month-over-month pctChange via LAG |

## Files
- `ltv.sql` — Full BigQuery SQL query
- `ltv_chart.png` — LTV by cohort year visualization

## Tools
- BigQuery (SQL)
- Looker Studio
