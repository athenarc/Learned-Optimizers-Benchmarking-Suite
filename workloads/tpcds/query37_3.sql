SELECT
  i.i_item_id,
  i.i_item_desc,
  i.i_current_price
FROM item AS i,
  inventory AS inv,
  date_dim AS dd,
  catalog_sales AS cs
WHERE
  i.i_current_price BETWEEN 43 AND 43 + 30 AND inv.inv_item_sk = i.i_item_sk AND dd.d_date_sk = inv.inv_date_sk AND dd.d_date BETWEEN CAST('1999-04-12' AS date) AND CAST('1999-06-11' AS date) AND i.i_manufact_id IN (913, 977, 884, 822) AND inv.inv_quantity_on_hand BETWEEN 100 AND 500 AND cs.cs_item_sk = i.i_item_sk
GROUP BY
  i.i_item_id,
  i.i_item_desc,
  i.i_current_price
ORDER BY
  i.i_item_id
LIMIT 100;