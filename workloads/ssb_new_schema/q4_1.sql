SELECT d.d_year, c.c_nation, SUM(l.l_revenue - l.l_supplycost) AS profit
FROM date AS d, customer AS c, supplier AS s, part AS p, lineitem AS l, orders AS o
WHERE o.o_custkey = c.c_custkey
  AND l.l_orderkey = o.o_orderkey
  AND l.l_suppkey = s.s_suppkey
  AND l.l_partkey = p.p_partkey
  AND o.o_orderdate = d.d_datekey
  AND c.c_region = 'AMERICA'
  AND s.s_region = 'AMERICA'
  AND (p.p_mfgr = 'MFGR#1' OR p.p_mfgr = 'MFGR#2')
GROUP BY d.d_year, c.c_nation
ORDER BY d.d_year, c.c_nation;