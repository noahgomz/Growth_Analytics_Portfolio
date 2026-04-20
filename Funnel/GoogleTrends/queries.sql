WITH base AS (
SELECT term, dma_name, week, score, refresh_time
FROM `bigquery-public-data.google_trends_hourly.top_terms_hourly`
WHERE refresh_time BETWEEN DATETIME("2026-04-13T17:00:00") AND DATETIME_ADD("2026-04-13T17:00:00", INTERVAL 1 DAY)
AND week >= '2026-01-01' AND week <= '2026-03-31'
AND score IS NOT NULL
),
#Raw grain is term, dma_name, week, refresh_time|score
weeksAtGrain AS (
SELECT term, dma_name, week, max(score) AS maxScore, COUNT(week) OVER (PARTITION BY term, dma_name) totalWeeks
FROM base
GROUP BY term, dma_name, week
ORDER BY totalWeeks DESC, term, dma_name, week
),
terms2MonthsPlusInRankings AS (
SELECT *
FROM weeksAtGrain
WHERE totalWeeks >= 8
ORDER BY totalWeeks DESC, term, dma_name
),
#Week duration of appearance varies by term but is stable across dma; decision is to include only those appearing >= 8 weeks
rnk AS (
  SELECT *, RANK() OVER (PARTITION BY term, dma_name ORDER BY week) as rk
  FROM terms2MonthsPlusInRankings
),
bigTable AS (
SELECT r.term, r.dma_name, r.week, r.maxScore as score,
  t.mxScore AS minScore,
  MAX(r.maxScore) OVER (PARTITION BY r.term, r.dma_name) as maxscore,
  CASE WHEN MAX(r.maxScore) OVER (PARTITION BY r.term, r.dma_name) >= t.mxScore * 1.25 THEN 1 ELSE 0 END as `25%_bump`,
  CASE WHEN MAX(r.maxScore) OVER (PARTITION BY r.term, r.dma_name) >= t.mxScore * 1.50 THEN 1 ELSE 0 END as `50%_bump`,
  CASE WHEN MAX(r.maxScore) OVER (PARTITION BY r.term, r.dma_name) >= t.mxScore * 1.75 THEN 1 ELSE 0 END as `75%_bump`,
  CASE WHEN MAX(r.maxScore) OVER (PARTITION BY r.term, r.dma_name) >= t.mxScore * 2 THEN 1 ELSE 0 END as `100%_bump`
FROM rnk r
JOIN
  (
    SELECT term, dma_name, MAX(maxScore) mxScore
    FROM rnk
    WHERE rk = 1
    GROUP BY term, dma_name
  ) t
ON t.term = r.term
AND t.dma_name = r.dma_name
ORDER BY r.term, r.dma_name, week
),
tablet AS (
SELECT term, dma_name, COUNT(week) numwks, MIN(minScore) minScore, MAX(maxscore) maxScore, MAX(`25%_bump`) tfbump, MAX(`50%_bump`) ftbump, MAX(`75%_bump`) sfbump, MAX(`100%_bump`) hundredbump
FROM bigTable
GROUP BY term, dma_name
)

SELECT
  #COUNT(*),
  #SUM(tfbump),
  #SUM(ftbump),
  #SUM(sfbump),
  #SUM(hundredbump),
  ROUND(SUM(tfbump) / COUNT(*),2) `PctBumbing25%`,
  ROUND(SUM(ftbump) / COUNT(*),2) `PctBumbing50%`,
  ROUND(SUM(sfbump) / COUNT(*),2) `PctBumbing75%`,
  ROUND(SUM(hundredbump) / COUNT(*),2) `PctBumbing100%`
FROM tablet
