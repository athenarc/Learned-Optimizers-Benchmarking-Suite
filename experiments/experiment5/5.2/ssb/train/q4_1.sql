SELECT d.d_year, c.c_nation, SUM(lo.lo_revenue - lo.lo_supplycost) AS profit
FROM date AS d, customer AS c, supplier AS s, part AS p, lineorder AS lo
WHERE lo.lo_custkey = c.c_custkey
AND lo.lo_suppkey = s.s_suppkey
AND lo.lo_partkey = p.p_partkey
AND lo.lo_orderdate = d.d_datekey
AND c.c_region = 'AMERICA'
AND s.s_region = 'AMERICA'
AND (p.p_mfgr = 'MFGR#1' OR p.p_mfgr = 'MFGR#2')
GROUP BY d.d_year, c.c_nation
ORDER BY d.d_year, c.c_nation;