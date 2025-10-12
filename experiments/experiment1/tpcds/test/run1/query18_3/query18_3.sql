SELECT
  i.i_item_id,
  ca.ca_country,
  ca.ca_state,
  ca.ca_county,
  AVG(CAST(cs.cs_quantity AS DECIMAL(12, 2))) AS agg1,
  AVG(CAST(cs.cs_list_price AS DECIMAL(12, 2))) AS agg2,
  AVG(CAST(cs.cs_coupon_amt AS DECIMAL(12, 2))) AS agg3,
  AVG(CAST(cs.cs_sales_price AS DECIMAL(12, 2))) AS agg4,
  AVG(CAST(cs.cs_net_profit AS DECIMAL(12, 2))) AS agg5,
  AVG(CAST(c.c_birth_year AS DECIMAL(12, 2))) AS agg6,
  AVG(CAST(cd1.cd_dep_count AS DECIMAL(12, 2))) AS agg7
FROM catalog_sales AS cs,
  customer_demographics AS cd1,
  customer_demographics AS cd2,
  customer AS c,
  customer_address AS ca,
  date_dim AS dd,
  item AS i
WHERE
  cs.cs_sold_date_sk = dd.d_date_sk AND cs.cs_item_sk = i.i_item_sk AND cs.cs_bill_cdemo_sk = cd1.cd_demo_sk AND cs.cs_bill_customer_sk = c.c_customer_sk AND cd1.cd_gender = 'F' AND cd1.cd_education_status = '2 yr Degree' AND c.c_current_cdemo_sk = cd2.cd_demo_sk AND c.c_current_addr_sk = ca.ca_address_sk AND c.c_birth_month IN (7, 10, 12, 2, 4, 5) AND dd.d_year = 1999 AND ca.ca_state IN ('AK', 'IL', 'OH', 'UT', 'MO', 'SD', 'TN')
GROUP BY ROLLUP (i.i_item_id, ca.ca_country, ca.ca_state, ca.ca_county)
ORDER BY
  ca.ca_country,
  ca.ca_state,
  ca.ca_county,
  i.i_item_id
LIMIT 100;