SELECT
  COUNT(*)
FROM store_sales AS ss,
  household_demographics AS hd,
  time_dim AS td,
  store AS s
WHERE
  ss.ss_sold_time_sk = td.t_time_sk AND ss.ss_hdemo_sk = hd.hd_demo_sk AND ss.ss_store_sk = s.s_store_sk AND td.t_hour = 20 AND td.t_minute >= 30 AND hd.hd_dep_count = 5 AND s.s_store_name = 'ese'
ORDER BY
  COUNT(*)
LIMIT 100;