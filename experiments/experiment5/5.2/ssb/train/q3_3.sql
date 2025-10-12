SELECT c.c_city, s.s_city, d.d_year, SUM(lo.lo_revenue) AS revenue
FROM customer AS c, lineorder AS lo, supplier AS s, date AS d
WHERE lo.lo_custkey = c.c_custkey
AND lo.lo_suppkey = s.s_suppkey
AND lo.lo_orderdate = d.d_datekey
AND (c.c_city = 'UNITED KI1' OR c.c_city = 'UNITED KI5')
AND (s.s_city = 'UNITED KI1' OR s.s_city = 'UNITED KI5')
AND d.d_year >= 1992 AND d.d_year <= 1997
GROUP BY c.c_city, s.s_city, d.d_year
ORDER BY d.d_year ASC, revenue DESC;