SELECT
  i.i_brand_id AS brand_id,
  i.i_brand AS brand,
  SUM(ss.ss_ext_sales_price) AS ext_price
FROM date_dim AS dd,
  store_sales AS ss,
  item AS i
WHERE
  dd.d_date_sk = ss.ss_sold_date_sk AND ss.ss_item_sk = i.i_item_sk AND i.i_manager_id = 36 AND dd.d_moy = 12 AND dd.d_year = 2001
GROUP BY
  i.i_brand,
  i.i_brand_id
ORDER BY
  ext_price DESC,
  i.i_brand_id
LIMIT 100;