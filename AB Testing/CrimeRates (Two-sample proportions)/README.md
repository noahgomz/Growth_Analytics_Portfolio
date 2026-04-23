# Statistical Analysis: Crime Clearance Rates in Austin, TX

## Project Overview
This project investigates the efficiency of law enforcement in Austin, Texas, by comparing the **clearance rates** (the proportion of reported crimes that are solved or "cleared") between two major categories: **Theft** and **Violent/Other Crimes**. 

The goal was to determine if the disparity in clearance rates is statistically significant or simply a result of random variation in crime reporting.

## The Data
The data was sourced from the `bigquery-public-data.austin_crime.crime` dataset via Google BigQuery. 

| Crime Category | Total Incidents ($n$) | Cleared Incidents | Clearance Rate (%) |
| :--- | :--- | :--- | :--- |
| **Theft** | 85,985 | 12,236 | **14.23%** |
| **Violent/Other** | 25,573 | 7,138 | **27.91%** |

## Methodology: Two-Sample Z-Test for Proportions
To compare these two independent groups, a **Two-Sample Z-Test for Proportions** was conducted. This test is ideal for large datasets where the outcome is binary (Cleared vs. Not Cleared).

### Hypotheses
* **Null Hypothesis ($H_0$):** There is no significant difference between the clearance rates of Theft and Violent/Other crimes ($p_1 = p_2$).
* **Alternative Hypothesis ($H_a$):** There is a significant difference between the clearance rates ($p_1 \\neq p_2$).

### Statistical Execution
The analysis was performed using Python's `statsmodels` library.

```python
from statsmodels.stats.proportion import proportions_ztest

count = [12236, 7138]
nobs = [85985, 25573]

zstat, pval = proportions_ztest(count, nobs, alternative='two-sided')
