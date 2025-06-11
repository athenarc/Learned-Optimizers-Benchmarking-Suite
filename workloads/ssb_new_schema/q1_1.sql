SELECT SUM(l_extendedprice * l_discount) AS revenue
FROM orders o, lineitem l, date d
WHERE o.o_orderkey = l.l_orderkey
  AND o.o_orderdate = d.d_datekey
  AND d.d_year = 1993
  AND l.l_discount BETWEEN 1 AND 3
  AND l.l_quantity < 25;
