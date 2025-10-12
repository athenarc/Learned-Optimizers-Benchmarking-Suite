SELECT
  dd.d_year,
  i.i_brand_id AS brand_id,
  i.i_brand AS brand,
  SUM(ss.ss_ext_sales_price) AS sum_agg
FROM date_dim AS dd,
  store_sales AS ss,
  item AS i
WHERE
  dd.d_date_sk = ss.ss_sold_date_sk AND ss.ss_item_sk = i.i_item_sk AND i.i_manufact_id = 436 AND dd.d_moy = 12
GROUP BY
  dd.d_year,
  i.i_brand,
  i.i_brand_id
ORDER BY
  dd.d_year,
  sum_agg DESC,
  brand_id
LIMIT 100;