WITH fieldsTable AS (
  SELECT id, user_id, sequence_number, session_id, created_at, browser, traffic_source, event_type
  FROM `bigquery-public-data.thelook_ecommerce.events`
),
baseTable AS (
  SELECT user_id, session_id, sequence_number, created_at, event_type
  FROM fieldsTable
  WHERE user_id IS NOT NULL
  ORDER BY user_id, session_id, sequence_number
),
funnelCnts AS (
  SELECT user_id, session_id,
    MAX(CASE WHEN event_type = 'home' THEN 1 ELSE 0 END) as homeCount,
    MAX(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) as cartCount,
    MAX(CASE WHEN event_type = 'product' THEN 1 ELSE 0 END) as productCount,
    MAX(CASE WHEN event_type = 'department' THEN 1 ELSE 0 END) as deptCount,
    MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchCount
  /* INFO: SUM agg vs MAX shows that sessions have >1 instance of cart, product, and dept event types
    SUM(CASE WHEN event_type = 'home' THEN 1 ELSE 0 END) as homeCount,
    SUM(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) as cartCount,
    SUM(CASE WHEN event_type = 'product' THEN 1 ELSE 0 END) as productCount,
    SUM(CASE WHEN event_type = 'department' THEN 1 ELSE 0 END) as deptCount,
    SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchCount
   */
  FROM baseTable
  GROUP BY user_id, session_id
),
/* INFO: Raw event_type count for sanity check
SELECT event_type, COUNT(*)
FROM baseTable
GROUP BY event_type
*/
inlinePctCalc AS (
  SELECT 
    SUM(homeCount) as homeEvents,
    SUM(deptCount) as deptEvents,
    SUM(productCount) as prodEvents,
    SUM(cartCount) as cartEvents,
    SUM(purchCount) as purchEvents,
    ROUND(SUM(deptCount) / SUM(homeCount),2) as dptPct,
    SUM(productCount) / SUM(deptCount) as pdctPct,
    SUM(cartCount) / SUM(productCount) as crtPct,
    SUM(purchCount) / SUM(cartCount) as pchPct
  FROM funnelCnts
)
SELECT 'home' as stage, 1 as stage_order, SUM(homeCount) as session_count FROM funnelCnts
UNION ALL
SELECT 'department', 2, SUM(deptCount) FROM funnelCnts
UNION ALL
SELECT 'product', 3, SUM(productCount) FROM funnelCnts
UNION ALL
SELECT 'cart', 4, SUM(cartCount) FROM funnelCnts
UNION ALL
SELECT 'purchase', 5, SUM(purchCount) FROM funnelCnts
ORDER BY stage_order
