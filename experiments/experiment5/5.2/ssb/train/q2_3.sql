SELECT SUM(lo.lo_revenue), d.d_year, p.p_brand1
FROM lineorder AS lo, date AS d, part AS p, supplier AS s
WHERE lo.lo_orderdate = d.d_datekey
AND lo.lo_partkey = p.p_partkey
AND lo.lo_suppkey = s.s_suppkey
AND p.p_brand1 = 'MFGR#2339'
AND s.s_region = 'EUROPE'
GROUP BY d.d_year, p.p_brand1
ORDER BY d.d_year, p.p_brand1;