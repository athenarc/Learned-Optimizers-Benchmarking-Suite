SELECT SUM(l.l_extendedprice * l.l_discount) AS revenue
FROM lineitem AS l, orders AS o, date AS d
WHERE l.l_orderkey = o.o_orderkey
  AND o.o_orderdate = d.d_datekey
  AND d.d_weeknuminyear = 6
  AND d.d_year = 1994
  AND l.l_discount BETWEEN 5 AND 7
  AND l.l_quantity BETWEEN 26 AND 35;