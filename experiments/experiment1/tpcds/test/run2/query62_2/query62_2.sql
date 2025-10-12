SELECT
  SUBSTR(w.w_warehouse_name, 1, 20),
  sm.sm_type,
  wsite.web_name,
  SUM(CASE WHEN (wsale.ws_ship_date_sk - wsale.ws_sold_date_sk <= 30) THEN 1 ELSE 0 END) AS "30 days",
  SUM(CASE WHEN (wsale.ws_ship_date_sk - wsale.ws_sold_date_sk > 30) AND (wsale.ws_ship_date_sk - wsale.ws_sold_date_sk <= 60) THEN 1 ELSE 0 END) AS "31-60 days",
  SUM(CASE WHEN (wsale.ws_ship_date_sk - wsale.ws_sold_date_sk > 60) AND (wsale.ws_ship_date_sk - wsale.ws_sold_date_sk <= 90) THEN 1 ELSE 0 END) AS "61-90 days",
  SUM(CASE WHEN (wsale.ws_ship_date_sk - wsale.ws_sold_date_sk > 90) AND (wsale.ws_ship_date_sk - wsale.ws_sold_date_sk <= 120) THEN 1 ELSE 0 END) AS "91-120 days",
  SUM(CASE WHEN (wsale.ws_ship_date_sk - wsale.ws_sold_date_sk > 120) THEN 1 ELSE 0 END) AS ">120 days"
FROM web_sales AS wsale,
  warehouse AS w,
  ship_mode AS sm,
  web_site AS wsite,
  date_dim AS dd
WHERE
  dd.d_month_seq BETWEEN 1190 AND 1190 + 11 AND wsale.ws_ship_date_sk = dd.d_date_sk AND wsale.ws_warehouse_sk = w.w_warehouse_sk AND wsale.ws_ship_mode_sk = sm.sm_ship_mode_sk AND wsale.ws_web_site_sk = wsite.web_site_sk
GROUP BY
  SUBSTR(w.w_warehouse_name, 1, 20),
  sm.sm_type,
  wsite.web_name
ORDER BY
  SUBSTR(w.w_warehouse_name, 1, 20),
  sm.sm_type,
  wsite.web_name
LIMIT 100;