# Funnel Analysis — TheLook Ecommerce
**Dataset:** `bigquery-public-data.thelook_ecommerce.events`  
**Tool:** BigQuery (Google SQL)  
**Output:** `funnelOutputInlinePcts.csv`, `funnel_chart.png`

---

## Objective

Measure session-level conversion rates across the five stages of the TheLook ecommerce user funnel: home → department → product → cart → purchase.

---

## Approach

### 1. Determine Grain
The raw table grain is `user_id, session_id, sequence_number`. Sessions with a null `user_id` were excluded — these represented 45% of total sessions but never contained home or purchase events and could not be attributed to identifiable users.

After filtering: **1,315,573 sessions** with a valid user_id.

### 2. Define Funnel Stages
Six event types exist in the dataset: `home`, `department`, `product`, `cart`, `purchase`, `cancel`. The funnel uses the five product-journey events in sequence. `cancel` is excluded as it is not a progression stage.

### 3. Pivot to Session-Level Flags
Each session was collapsed to a single row using `MAX(CASE WHEN event_type = 'x' THEN 1 ELSE 0 END)`, producing a binary flag per stage per session. `MAX` was chosen over `SUM` to capture stage presence rather than raw event count — sessions can contain multiple cart, product, and department events within a single session.

### 4. Aggregate and Compute Ratios
Stage flags were summed to produce distinct session counts at each funnel stage. Step-by-step conversion rates were computed by dividing each stage count by the preceding stage count.

---

## Results

| Stage | Sessions | Step Conversion |
|---|---|---|
| Home | 87,142 | — |
| Department | 180,539 | 207% |
| Product | 180,539 | 100% |
| Cart | 180,539 | 100% |
| Purchase | 180,539 | 100% |

---

## Data Limitation

TheLook is a synthetic dataset. The flat conversion rates after the home stage (department through purchase all equal 180,539) indicate that sessions in this dataset are generated with a fixed event sequence — any session that progresses past home contains all remaining stages. This prevents meaningful drop-off analysis beyond the home→department step.

The query logic and methodology are valid and would produce realistic funnel drop-off on behavioral event data.

---

## Key Technical Decisions

**Null user_id exclusion:** Sessions without a user_id were excluded because they never contained home or purchase events and could not contribute to any funnel stage. This was verified before exclusion.

**MAX vs SUM aggregation:** Using SUM to aggregate CASE flags returns raw event counts (matching event-level row counts), while MAX returns distinct session presence. The funnel requires the latter — a session either reached a stage or it didn't, regardless of how many times that event fired.
