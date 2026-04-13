# eCommerce Cohort Retention Analysis

## Overview
Cohort retention analysis on the BigQuery public dataset `thelook_ecommerce`, examining how well the platform retains customers over time after their first purchase.

## Dataset
- **Source:** `bigquery-public-data.thelook_ecommerce`
- **Tables used:** `orders`, `users`
- **Date range:** January 2019 – April 2026

## Methodology
- **Cohort definition:** Month of each user's first order (not account creation date)
- **Retention metric:** Distinct users placing an order in a given month, divided by the cohort's size at month 0
- **Exclusions:** Cancelled and returned orders are excluded from all calculations

## Key Findings
- Retention drops sharply within the first 1-2 months, stabilizing at roughly 5-13% of the original cohort
- The business profile is dominated by one-time buyers — most users place a single order and do not return
- A small but consistent subset of users from older cohorts re-engage after extended dormancy, suggesting occasional seasonal or lapsed-buyer behavior
- Retention rates do not translate to strong revenue, indicating low average order values among returning users

## Files
- `cohort_retention.sql` — Full BigQuery SQL query
- `retention_chart.png` — Retention curve visualization by cohort

## Tools
- BigQuery (SQL)
- Looker Studio
