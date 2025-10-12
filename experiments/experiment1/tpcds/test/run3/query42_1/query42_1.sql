SELECT
  dd.d_year,
  i.i_category_id,
  i.i_category,
  SUM(ss.ss_ext_sales_price)
FROM date_dim AS dd,
  store_sales AS ss,
  item AS i
WHERE
  dd.d_date_sk = ss.ss_sold_date_sk AND ss.ss_item_sk = i.i_item_sk AND i.i_manager_id = 1 AND dd.d_moy = 12 AND dd.d_year = 1998
GROUP BY
  dd.d_year,
  i.i_category_id,
  i.i_category
ORDER BY
  SUM(ss.ss_ext_sales_price) DESC,
  dd.d_year,
  i.i_category_id,
  i.i_category
LIMIT 100;