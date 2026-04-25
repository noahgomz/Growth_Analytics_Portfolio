# CafeLaptop A/B Experiment — Ad Copy Test

## Overview

CafeLaptop is a desktop app that alarms when someone leaves a café with your laptop. This project documents a live A/B test on ad copy variants run on Meta Ads, designed to determine whether problem-led or benefit-led framing drives higher click-through rates to the waitlist landing page.

This is Test 1 of a two-test sequential experiment. Test 2 will use the winning ad to drive traffic to two landing page variants (with vs. without pricing), measuring waitlist signup rate.

---

## Business Question

Does problem-led or benefit-led ad copy generate higher landing page CTR among café remote workers on Meta?

---

## Hypothesis

Benefit-led copy will outperform problem-led copy because it frames the product around a desired outcome rather than amplifying anxiety, which may resonate better with a cold audience that hasn't yet formed a threat perception.

---

## Experiment Design

| Parameter | Value |
|---|---|
| Test type | A/B (two variants) |
| Unit of randomization | User |
| Unit of analysis | User (reach) |
| Primary metric | CTR (LP views / reach) |
| MDE | 50% relative |
| Alpha (α) | 0.05 |
| Power | 90% |
| Duration | 14 days (Apr 8 – Apr 22, 2026) |
| Budget | $5.50/day per variant (~$77 each) |
| Platform | Meta Ads (Facebook + Instagram Feed) |

**Variants:**

- **Control (Problem-led):** *"When someone walks out of the café with your laptop, CafeLaptop alarms before they reach the door."*
- **Treatment (Benefit-led):** *"Leave your laptop to step away, stretch, or order, without fearing it gets stolen."*

---

## Stack

| Layer | Tool |
|---|---|
| Ad platform | Meta Ads Manager |
| Landing page | Carrd |
| Event logging | Google Tag Manager → GA4 |
| Storage | BigQuery (GA4 native export) |
| Analysis | DuckDB (SQL) + Python (statsmodels) |
| Email capture | Brevo |

---

## Key Design Decisions

**Randomization at user level via Meta's A/B framework.** Meta's experiment tool ensures each user sees only one variant, preventing contamination across ad sets.

**Reach as denominator, not impressions.** Impressions are non-additive across demographic breakdowns and can count the same user multiple times. Reach (unique users exposed) is the correct denominator for a user-level randomization test.

**Reach is non-additive in raw breakdown data.** When summing reach across age/gender breakdowns a user can be counted in multiple buckets. Meta's aggregate report provides the correct non-duplicated reach figure, which is hardcoded in the SQL query with documentation.

**CTR as primary metric, not signup rate.** The ad only has causal power over the click decision — the user hasn't seen the signup form yet. Signup rate is reserved for Test 2 where the landing page is the treatment. Each metric must have a direct causal relationship with its treatment.

**UTM attribution limitation.** Meta's in-app browser stripped UTM parameters for the majority of sessions, making GA4 session-level attribution unreliable for this test. Ad-level CTR analysis relied on Meta's native reporting instead. This is a known limitation of Meta traffic and is documented as such.

---

## Results

| Variant | Impressions | Reach (Meta aggregate) | LP Views | CTR |
|---|---|---|---|---|
| Problem-led | 5,141 | 4,126 | 107 | 2.6% |
| Benefit-led | 5,369 | 4,411 | 172 | 3.9% |

**Relative lift:** +50% in favor of benefit-led

---

## Statistical Test

Two-proportion z-test (two-tailed), implemented via `statsmodels.stats.proportion.proportions_ztest`.

| Metric | Value |
|---|---|
| Z-statistic | 3.39 |
| P-value | 0.0007 |
| Significance (α = 0.05) | ✅ Yes |

P-value of 0.0007 is well below the α = 0.05 threshold. The result is statistically significant.

---

## Limitations

- **Underpowered relative to observed effect.** Pre-experiment sample size calculation required ~8,300 impressions per variant at 50% relative MDE and 90% power. Final reach was ~4,100–4,400 per variant, yielding actual power of approximately 61%. The result reached significance despite this because the observed effect was consistent and clean throughout the test window.
- **UTM stripping.** Meta's in-app browser stripped UTM parameters for the majority of sessions, preventing GA4-level attribution by variant. Native Meta reporting was used instead.
- **Cold audience.** No prior brand awareness. Results may differ with a warm or retargeted audience.
- **Single creative per variant.** Only one image (logo) was used. Creative fatigue and image-copy interaction effects were not tested.

---

## Decision

**Benefit-led copy wins.** It will be used as the fixed ad in Test 2, which will test landing page copy variants (with vs. without pricing) measuring waitlist signup rate.

---

## Extensions

The SQL queries include age and gender breakdown tables as sanity checks. These can be extended into chi-squared tests of independence to determine whether CTR differences between variants are statistically consistent across demographic segments. A significant chi-squared result would indicate that the treatment effect varies by group (interaction effect), while an insignificant result would confirm the benefit-led advantage is uniform across the audience.

---

## Files

| File | Description |
|---|---|
| `queries.sql` | DuckDB queries for data cleaning, sanity checks (age/gender breakdowns), and final aggregation |
| `stats.ipynb` | Python notebook with two-proportion z-test |
| `outputTable.csv` | Final aggregated results table |
| `CafeLaptop_Campaign_AB_1_raw.csv` | Raw Meta Ads export |
