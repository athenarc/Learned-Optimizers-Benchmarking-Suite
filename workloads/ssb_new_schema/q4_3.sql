SELECT d.d_year, s.s_city, p.p_brand1, SUM(l_revenue - l_supplycost) AS profit
FROM orders o, lineitem l, customer c, supplier s, part p, date d
WHERE o.o_orderkey = l.l_orderkey
  AND o.o_orderdate = d.d_datekey
  AND o.o_custkey = c.c_custkey
  AND l.l_suppkey = s.s_suppkey
  AND l.l_partkey = p.p_partkey
  AND c.c_region = 'AMERICA'
  AND s.s_nation = 'UNITED STATES'
  AND d.d_year IN (1997, 1998)
  AND p.p_category = 'MFGR#14'
GROUP BY d.d_year, s.s_city, p.p_brand1
ORDER BY d.d_year, s.s_city, p.p_brand1;
