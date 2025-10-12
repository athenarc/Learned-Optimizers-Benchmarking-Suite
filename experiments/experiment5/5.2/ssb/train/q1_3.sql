SELECT SUM(lo.lo_extendedprice * lo.lo_discount) AS revenue
FROM lineorder AS lo, date AS d
WHERE lo.lo_orderdate = d.d_datekey
AND d.d_weeknuminyear = 6
AND d.d_year = 1994
AND lo.lo_discount BETWEEN 5 AND 7
AND lo.lo_quantity BETWEEN 26 AND 35;