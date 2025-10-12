SELECT c.c_nation, s.s_nation, d.d_year, SUM(l.l_revenue) AS revenue
FROM customer AS c, lineitem AS l, orders AS o, supplier AS s, date AS d
WHERE o.o_custkey = c.c_custkey
  AND l.l_orderkey = o.o_orderkey
  AND l.l_suppkey = s.s_suppkey
  AND o.o_orderdate = d.d_datekey
  AND c.c_region = 'ASIA'
  AND s.s_region = 'ASIA'
  AND d.d_year >= 1992 AND d.d_year <= 1997
GROUP BY c.c_nation, s.s_nation, d.d_year
ORDER BY d.d_year ASC, revenue DESC;