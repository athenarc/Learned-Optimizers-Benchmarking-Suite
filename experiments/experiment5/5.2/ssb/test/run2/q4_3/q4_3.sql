SELECT d.d_year, s.s_city, p.p_brand1, SUM(l.l_revenue - l.l_supplycost) AS profit
FROM date AS d, customer AS c, supplier AS s, part AS p, lineitem AS l, orders AS o
WHERE o.o_custkey = c.c_custkey
  AND l.l_orderkey = o.o_orderkey
  AND l.l_suppkey = s.s_suppkey
  AND l.l_partkey = p.p_partkey
  AND o.o_orderdate = d.d_datekey
  AND c.c_region = 'AMERICA'
  AND s.s_nation = 'UNITED STATES'
  AND (d.d_year = 1997 OR d.d_year = 1998)
  AND p.p_category = 'MFGR#14'
GROUP BY d.d_year, s.s_city, p.p_brand1
ORDER BY d.d_year, s.s_city, p.p_brand1;