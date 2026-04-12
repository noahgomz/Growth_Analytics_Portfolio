WITH orderItems AS (
  SELECT order_id, user_id, status, ROUND(SUM(sale_price),2) orderTotalSales, count(*) as numItems
  FROM `bigquery-public-data.thelook_ecommerce.order_items`
  GROUP BY order_id, user_id, status
  ORDER BY order_id ASC
),
usersTable AS (
  SELECT id, created_at as userCreatedAt, traffic_source, country, state
  FROM `bigquery-public-data.thelook_ecommerce.users`
),
cohort AS (
  SELECT o.user_id as ouser_id, *, DATE_TRUNC(userCreatedAt, QUARTER) as cohort_quarter
  FROM `bigquery-public-data.thelook_ecommerce.orders` o
  JOIN orderItems oi
  ON oi.order_id = o.order_id
  JOIN usersTable u
  ON u.id = o.user_id
  WHERE oi.status = 'Complete'
  OR oi.status = 'Shipped'
  OR oi.status = 'Processing'
  ORDER BY o.order_id ASC
),

/*COHORT GRAPH OUTPUT
SELECT c.cohort_quarter, created_at, SUM(orderTotalSales) as sales, COUNT(DISTINCT ouser_id) as numUsers, count(numItems) as numProductsSold
FROM cohort c
GROUP BY c.cohort_quarter, created_at
*/
  
/* AVG PURCHASE PER USER CONSISTENT ACROSS COHORTS, DOES NOT CAUSE REV SPIKE IN Q12025
SELECT cohort.cohort_quarter, SUM(orderTotalSales) / COUNT(DISTINCT ouser_id) as avgPurch
FROM cohort
GROUP BY cohort.cohort_quarter
ORDER BY EXTRACT(MONTH FROM cohort_quarter) DESC, EXTRACT(YEAR FROM cohort_quarter) DESC
LIMIT 100
*/
  
quarterCohortGrain AS (
  SELECT EXTRACT(YEAR FROM cohort.created_at) yr, EXTRACT(QUARTER FROM cohort.created_at) qt, traffic_source, COUNT(DISTINCT ouser_id) numUsers
  FROM cohort
  GROUP BY EXTRACT(YEAR FROM cohort.created_at), EXTRACT(QUARTER FROM cohort.created_at), traffic_source
  ORDER BY yr DESC, qt, traffic_source DESC
)

SELECT *
FROM quarterCohortGrain
WHERE (yr = 2026 AND qt = 1)
OR
(yr = 2025 AND qt = 4)
