SELECT SUM(l_extendedprice * l_discount) AS revenue
FROM orders o, lineitem l, date d
WHERE o.o_orderkey = l.l_orderkey
  AND o.o_orderdate = d.d_datekey
  AND d.d_weeknuminyear = 6
  AND d.d_year = 1994
  AND l.l_discount BETWEEN 5 AND 7
  AND l.l_quantity BETWEEN 26 AND 35;
