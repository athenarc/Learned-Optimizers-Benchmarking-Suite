SELECT
  i.i_item_id,
  s.s_state,
  GROUPING(s.s_state) AS g_state,
  AVG(ss.ss_quantity) AS agg1,
  AVG(ss.ss_list_price) AS agg2,
  AVG(ss.ss_coupon_amt) AS agg3,
  AVG(ss.ss_sales_price) AS agg4
FROM store_sales AS ss,
  customer_demographics AS cd,
  date_dim AS dd,
  store AS s,
  item AS i
WHERE
  ss.ss_sold_date_sk = dd.d_date_sk AND ss.ss_item_sk = i.i_item_sk AND ss.ss_store_sk = s.s_store_sk AND ss.ss_cdemo_sk = cd.cd_demo_sk AND cd.cd_gender = 'F' AND cd.cd_marital_status = 'U' AND cd.cd_education_status = 'College' AND dd.d_year = 1999 AND s.s_state IN ('TN', 'TN', 'TN', 'TN', 'TN', 'TN')
GROUP BY ROLLUP (i.i_item_id, s.s_state)
ORDER BY
  i.i_item_id,
  s.s_state
LIMIT 100;