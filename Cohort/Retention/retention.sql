WITH jointableb AS (
  SELECT o.*,
    date_trunc(o.created_at, MONTH) AS orderMonth,
    u.country,
    u.age,
    u.traffic_source,
    u.created_at as userCreatedAt
  FROM `bigquery-public-data.thelook_ecommerce.orders` o
  JOIN `bigquery-public-data.thelook_ecommerce.users` u
  ON o.user_id = u.id
),
userFirstOrder AS (
  SELECT user_id, MIN(created_at) as fristOrderDate
  FROM jointableb
  GROUP BY user_id
),
jointable AS (
  SELECT a.*, date_trunc(b.fristOrderDate, MONTH) AS cohortMonth
  FROM jointableb a
  JOIN userFirstOrder b
  ON b.user_id = a.user_id
  WHERE a.status <> 'Cancelled'
  AND a.status <> 'Returned'
),
cohortStructure AS (
  SELECT orderMonth, cohortMonth, COUNT(DISTINCT user_id) as cntDusers,
    LAG(COUNT(DISTINCT user_id),1,0) OVER (PARTITION BY cohortMonth ORDER BY orderMonth ASC) as prevUserCnt
  FROM jointable
  GROUP BY orderMonth, cohortMonth
  ORDER BY orderMonth DESC, cohortMonth DESC
),
firstMonthOfCohort AS (
  SELECT orderMonth, cohortMonth, COUNT(DISTINCT user_id) cntUsersFirstMonth
  FROM jointable
  WHERE orderMonth = cohortMonth
  GROUP BY orderMonth, cohortMonth
  ORDER BY orderMonth DESC, cohortMonth DESC
)

SELECT c.orderMonth, c.cohortMonth, c.cntDusers, /*c.prevUserCnt, f.cohortMonth,*/ f.cntUsersFirstMonth,
  ROUND((c.cntDusers / f.cntUsersFirstMonth) - 1,2) AS pctChangeFromFrist,
  ROUND((c.cntDusers / f.cntUsersFirstMonth),2) AS retention
FROM cohortStructure c
JOIN firstMonthOfCohort f
ON f.cohortMonth = c.cohortMonth
WHERE c.orderMonth < '2026-04-01 00:00:00'
ORDER BY c.orderMonth DESC, c.cohortMonth DESC
