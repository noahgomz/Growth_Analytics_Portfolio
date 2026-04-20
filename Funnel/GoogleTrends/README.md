# Google Trends Trajectory Analysis

**Tools:** BigQuery (SQL) · Looker Studio  
**Data:** `bigquery-public-data.google_trends_hourly.top_terms_hourly`  
**Scope:** Q1 2026 (Jan 1 – Mar 31), refreshed April 13, 2026

---

## Overview

Google Trends ranks the top search terms by DMA (media market) each week, but raw rankings tell you little about momentum. This project asks a more interesting question: **among terms that sustain meaningful search presence, how many actually grow — and by how much?**

The analysis filters to terms with at least 8 weeks of recorded scores (indicating sustained relevance rather than a one-week spike), then measures what share of those terms reached 25%, 50%, 75%, and 100% growth relative to their initial score.

---

## The Funnel

Rather than a traditional session-based conversion funnel, this project treats a search term's score trajectory as a funnel — each growth threshold representing a deeper stage of momentum:

| Stage | Description | % of Terms |
|---|---|---|
| Entered & sustained | Appeared in rankings for 8+ weeks | 100% (baseline) |
| Grew 25%+ | Peak score ≥ 1.25× initial score | 83% |
| Grew 50%+ | Peak score ≥ 1.50× initial score | 63% |
| Grew 75%+ | Peak score ≥ 1.75× initial score | 39% |
| Grew 100%+ | Peak score ≥ 2.00× initial score | 24% |

The drop-off is consistent and intuitive — most terms show some upward momentum, but only about 1 in 4 sustains enough to double from where they started.

---

## Key Query Logic

The full query is [available here](https://github.com/noahgomz/Growth_Analytics_Portfolio/blob/master/Funnel/GoogleTrends/queries.sql). The core logic works in several stages:

**1. Snapshot isolation** — filter to a single refresh window to avoid double-counting rows updated across multiple refresh cycles:

```sql
WHERE refresh_time BETWEEN DATETIME("2026-04-13T17:00:00") 
  AND DATETIME_ADD("2026-04-13T17:00:00", INTERVAL 1 DAY)
AND week >= '2026-01-01' AND week <= '2026-03-31'
AND score IS NOT NULL
```

**2. Deduplication to week grain** — the raw table has multiple rows per term/DMA/week (one per refresh); collapse to one row by taking `MAX(score)` per week.

**3. Sustained presence filter** — keep only term-DMA combinations with 8+ weeks of scored data, removing terms that briefly spiked and disappeared.

**4. Baseline establishment** — use the score at first appearance (`rk = 1` via `RANK() OVER (PARTITION BY term, dma_name ORDER BY week)`) as the initial score for each term-DMA pair.

**5. Bump classification** — compare each term's peak score against its initial score across four thresholds (25%, 50%, 75%, 100%), then aggregate to the term-DMA level.

**6. Final rollup** — compute the share of term-DMA pairs that cleared each threshold.

---

## Results

![Trend Growth Distribution](Graph.pdf)

83% of sustained terms grew at least 25% from their initial score. Growth at higher thresholds decays steadily — 63% at 50%, 39% at 75%, 24% at 100% — producing a clean funnel shape consistent with how search momentum typically behaves: most trending topics build modestly, few sustain explosive growth.

---

## Methodology Notes

- **Baseline risk:** Terms that enter rankings mid-spike may have an artificially elevated initial score, which would bias bump rates downward. The true share of growing terms is likely slightly higher than reported.
- **Score nulls:** Google suppresses scores below a minimum volume threshold. Only rows with non-null scores are included, which means low-volume DMAs are underrepresented.
- **DMA grain:** Results are at the term × DMA level, not term-only. A single term can appear multiple times if it trends differently across markets.

---

## Potential Extensions

- **Full funnel including attrition:** Add upstream stages (appeared at all → lasted 4+ weeks → lasted 8+ weeks) to show how many terms wash out before reaching the sustained presence filter. This would give the analysis a complete top-to-bottom funnel shape.
- **Category segmentation:** Break results by term type (news events vs. evergreen queries) to see whether trajectory patterns differ by content category.
- **Time-to-peak analysis:** Measure how many weeks it typically takes a term to reach its maximum score after first appearing.
