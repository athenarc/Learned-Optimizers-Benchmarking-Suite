SELECT SUM(l_revenue), d.d_year, p.p_brand1
FROM orders o, lineitem l, date d, part p, supplier s
WHERE o.o_orderkey = l.l_orderkey
  AND o.o_orderdate = d.d_datekey
  AND l.l_partkey = p.p_partkey
  AND l.l_suppkey = s.s_suppkey
  AND p.p_brand1 BETWEEN 'MFGR#2221' AND 'MFGR#2228'
  AND s.s_region = 'ASIA'
GROUP BY d.d_year, p.p_brand1
ORDER BY d.d_year, p.p_brand1;
