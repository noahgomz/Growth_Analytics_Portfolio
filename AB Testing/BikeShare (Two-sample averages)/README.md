# Austin Bikeshare: Electric vs Classic Bike Usage Analysis

## Overview

Statistical analysis of electric vs classic bike usage rates using the Austin Bikeshare public dataset in BigQuery. The goal was to determine whether the observed difference in monthly usage between electric and classic bikes is statistically significant.

This is an observational analysis, not a controlled experiment. Bike type is not randomly assigned — users self-select based on availability at each station.

---

## Dataset

**Source:** `bigquery-public-data.austin_bikeshare.bikeshare_trips`

**Unit of observation:** Individual bike (`bike_id`)

**Outcome metric:** Trips per month — computed as total trips divided by number of distinct months active per bike. This normalizes for deployment duration, ensuring bikes that entered the fleet later are not penalized in the comparison.

One malformed record (`bike_id = '198BB'`) was excluded from the analysis.

---

## Hypothesis

**Null hypothesis (H₀):** There is no difference in average monthly trips between electric and classic bikes.

**Alternative hypothesis (H₁):** Electric bikes generate more monthly trips than classic bikes.

**Test type:** Two-sample z-test on means (two-tailed)

**Significance threshold:** α = 0.05 (z > 1.96)

---

## Methodology

### Step 1 — Observation-level aggregation
Each bike's trips per month was computed by dividing total trips by the number of distinct months the bike was active. This controls for deployment duration across a multi-year dataset where electric bikes were introduced at different points in the fleet lifecycle.

### Step 2 — Group-level summary statistics
For each bike type: total trips, number of bikes, average trips per month, and sample standard deviation were computed.

### Step 3 — Pivot to single row
A conditional aggregation (MAX CASE WHEN) was used to compress both groups onto a single row for z-score calculation.

### Step 4 — Z-score calculation
The two-sample z-test for means was applied:

```
z = (x̄₁ - x̄₂) / √(s₁²/n₁ + s₂²/n₂)
```

Where x̄ is average trips per month, s is sample standard deviation, and n is number of bikes per type.

---

## Results

| Metric | Electric | Classic |
|---|---|---|
| Number of bikes | 382 | 641 |
| Avg trips per month | 63.40 | 32.57 |
| Sample std deviation | 16.03 | 11.49 |
| **Z-score** | **32.89** | |
| **Result** | **Statistically significant** | |

Electric bikes average approximately **2x more trips per month** than classic bikes. The z-score of 32.89 far exceeds the 1.96 threshold, making the result statistically significant with extremely high confidence (p ≈ 0).

---

## Caveats

**Observational design:** Users self-select bike type based on station availability and personal preference. The difference in usage rates cannot be attributed solely to bike type — confounding factors such as station placement and user demographics are not controlled for.

**Fleet composition over time:** In years where electric bikes were scarce, users who may have preferred electric were forced to ride classic bikes, inflating classic usage rates. This means the true preference gap is likely understated — both bike types equally available, electric bikes would likely show an even larger usage advantage.

**Deployment duration is controlled:** Trips per month normalizes for when each bike entered the fleet, making the comparison fair across a multi-year window.

---

## Files

| File | Description |
|---|---|
| `queries.sql` | Full BigQuery SQL — observation-level aggregation, group stats, pivot, and z-score |
| `ScoringOutput.csv` | Final scoring output with z-score and group metrics |
| `CASEcleaning.xlsx` | Working notes on CASE WHEN pivot logic |

---

## Tools

- **BigQuery** — data access and SQL execution
- **Google Cloud Public Datasets** — Austin Bikeshare source data
