SELECT c.c_city, s.s_city, d.d_year, SUM(l_revenue) AS revenue
FROM customer c, orders o, lineitem l, supplier s, date d
WHERE o.o_orderkey = l.l_orderkey
  AND o.o_orderdate = d.d_datekey
  AND o.o_custkey = c.c_custkey
  AND l.l_suppkey = s.s_suppkey
  AND (c.c_city = 'UNITED KI1' OR c.c_city = 'UNITED KI5')
  AND (s.s_city = 'UNITED KI1' OR s.s_city = 'UNITED KI5')
  AND d.d_yearmonth = 'Dec1997'
GROUP BY c.c_city, s.s_city, d.d_year
ORDER BY d.d_year ASC, revenue DESC;
