SELECT SUM(l.l_revenue), d.d_year, p.p_brand1
FROM lineitem AS l, orders AS o, date AS d, part AS p, supplier AS s
WHERE l.l_orderkey = o.o_orderkey
  AND o.o_orderdate = d.d_datekey
  AND l.l_partkey = p.p_partkey
  AND l.l_suppkey = s.s_suppkey
  AND p.p_category = 'MFGR#12'
  AND s.s_region = 'AMERICA'
GROUP BY d.d_year, p.p_brand1
ORDER BY d.d_year, p.p_brand1;