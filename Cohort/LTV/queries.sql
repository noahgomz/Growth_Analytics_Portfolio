/* BASE 
SELECT *
FROM `bigquery-public-data.thelook_ecommerce.order_items`
LIMIT 100
*/

/*ONE SHIPPEDAT & ONE DELIVEREDAT PER ORDER_ID
SELECT user_id, order_id,
  count(distinct product_id) cntP,
  count(distinct created_at) cntC,
  count(distinct shipped_at) cntS,
  count(distinct delivered_at) cntD
FROM `bigquery-public-data.thelook_ecommerce.order_items`
GROUP BY user_id, order_id
ORDER BY cntD DESC
LIMIT 1000
*/

/*ORDER ITEMS AT ORDERID GRAIN WITH SHIPPED AND DELIVERED DATES
SELECT user_id, order_id,
  MAX(status),
  MAX(shipped_at) ShippedAt,
  MAX(delivered_at) DelieredAt
FROM `bigquery-public-data.thelook_ecommerce.order_items`
GROUP BY user_id, order_id
ORDER BY user_id, order_id
LIMIT 1000
*/

WITH userFirstPurchase AS(
SELECT user_id, MIN(created_at) as userFirstPurchDate
FROM `bigquery-public-data.thelook_ecommerce.order_items`
GROUP BY user_id
),
orderItems AS (
SELECT user_id, order_id, MIN(created_at) AS orderDate, ROUND(SUM(sale_price),2) as sales,
FROM `bigquery-public-data.thelook_ecommerce.order_items` 
GROUP BY user_id, order_id
),
cohort AS (
SELECT o.*, u.userFirstPurchDate,
  date_trunc(o.orderDate,MONTH) orderMonth,
  date_trunc(u.userFirstPurchDate,MONTH) cohortMonth
FROM orderItems o
JOIN userFirstPurchase u
ON o.user_id = u.user_id
),
orderMonthCohortSales AS (
SELECT cohortMonth, orderMonth, ROUND(SUM(sales),2) AS sales
FROM cohort
GROUP BY cohortMonth, orderMonth
),
cohortSize AS (
  SELECT cohortMonth, COUNT(distinct user_id) numUsers
  FROM cohort
  GROUP BY cohortMonth
),
LTVtable AS (
SELECT o.*, s.numUsers,
  ROUND(SUM(o.sales) OVER (PARTITION BY o.cohortMonth ORDER BY o.orderMonth),2) AS runningTotal,
  ROUND(SUM(o.sales) OVER (PARTITION BY o.cohortMonth ORDER BY o.orderMonth) / s.numUsers,2) as runningLTV
FROM orderMonthCohortSales o
JOIN cohortSize s
ON o.cohortMonth = s.cohortMonth
)

SELECT *,
  ROUND((runningLTV / NULLIF(LAG(runningLTV,1,0) OVER (PARTITION BY cohortMonth ORDER BY orderMonth),0)) - 1,2) AS pctChange
FROM LTVtable
ORDER BY cohortMonth DESC, orderMonth DESC
