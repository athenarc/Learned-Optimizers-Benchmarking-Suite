 -- Q1.1: Find revenue when:
 -- Year = 1993
 -- Discount = 2 (with a range of 1 to 3)
 -- Quantity < 25
 SELECT SUM(lo_extendedprice * lo_discount) AS revenue
 FROM lineorder, date
 WHERE lo_orderdate = d_datekey
 AND d_year = 1993
 AND lo_discount BETWEEN 1 AND 3
 AND lo_quantity < 25;

 -- Q1.2: Find revenue when:
 -- YearMonth = 199401
 -- Discount between 4 and 6
 -- Quantity between 26 and 35
 SELECT SUM(lo_extendedprice * lo_discount) AS revenue
 FROM lineorder, date
 WHERE lo_orderdate = d_datekey
 AND d_yearmonthnum = 199401
 AND lo_discount BETWEEN 4 AND 6
 AND lo_quantity BETWEEN 26 AND 35;

 -- Q1.3: Find revenue when:
 -- Week number = 6
 -- Year = 1994
 -- Discount between 5 and 7
 -- Quantity between 26 and 35
 SELECT SUM(lo_extendedprice * lo_discount) AS revenue
 FROM lineorder, date
 WHERE lo_orderdate = d_datekey
 AND d_weeknuminyear = 6
 AND d_year = 1994
 AND lo_discount BETWEEN 5 AND 7
 AND lo_quantity BETWEEN 26 AND 35;

 -- Q2.1: Compare revenue for 'MFGR#12' category and 'AMERICA' region
 SELECT SUM(lo_revenue), d_year, p_brand1
 FROM lineorder, date, part, supplier
 WHERE lo_orderdate = d_datekey
 AND lo_partkey = p_partkey
 AND lo_suppkey = s_suppkey
 AND p_category = 'MFGR#12'
 AND s_region = 'AMERICA'
 GROUP BY d_year, p_brand1
 ORDER BY d_year, p_brand1;

 -- Q2.2: Compare revenue for 'MFGR#2221' to 'MFGR#2228' and 'ASIA' region
 SELECT SUM(lo_revenue), d_year, p_brand1
 FROM lineorder, date, part, supplier
 WHERE lo_orderdate = d_datekey
 AND lo_partkey = p_partkey
 AND lo_suppkey = s_suppkey
 AND p_brand1 BETWEEN 'MFGR#2221' AND 'MFGR#2228'
 AND s_region = 'ASIA'
 GROUP BY d_year, p_brand1
 ORDER BY d_year, p_brand1;

 -- Q2.3: Compare revenue for 'MFGR#2339' brand and 'EUROPE' region
 SELECT SUM(lo_revenue), d_year, p_brand1
 FROM lineorder, date, part, supplier
 WHERE lo_orderdate = d_datekey
 AND lo_partkey = p_partkey
 AND lo_suppkey = s_suppkey
 AND p_brand1 = 'MFGR#2339'
 AND s_region = 'EUROPE'
 GROUP BY d_year, p_brand1
 ORDER BY d_year, p_brand1;

 -- Q3.1: Compare revenue by customer nation and supplier nation for a 6-year period (1992-1997)
 SELECT c_nation, s_nation, d_year, SUM(lo_revenue) AS revenue
 FROM customer, lineorder, supplier, date
 WHERE lo_custkey = c_custkey
 AND lo_suppkey = s_suppkey
 AND lo_orderdate = d_datekey
 AND c_region = 'ASIA' 
 AND s_region = 'ASIA'
 AND d_year >= 1992 AND d_year <= 1997
 GROUP BY c_nation, s_nation, d_year
 ORDER BY d_year ASC, revenue DESC;

 -- Q3.2: Compare revenue by customer city and supplier city for 'UNITED STATES' nation (1992-1997)
 SELECT c_city, s_city, d_year, SUM(lo_revenue) AS revenue
 FROM customer, lineorder, supplier, date
 WHERE lo_custkey = c_custkey
 AND lo_suppkey = s_suppkey
 AND lo_orderdate = d_datekey
 AND c_nation = 'UNITED STATES'
 AND s_nation = 'UNITED STATES'
 AND d_year >= 1992 AND d_year <= 1997
 GROUP BY c_city, s_city, d_year
 ORDER BY d_year ASC, revenue DESC;

 -- Q3.3: Compare revenue by customer city and supplier city for two cities in 'UNITED KINGDOM' (1992-1997)
 SELECT c_city, s_city, d_year, SUM(lo_revenue) AS revenue
 FROM customer, lineorder, supplier, date
 WHERE lo_custkey = c_custkey
 AND lo_suppkey = s_suppkey
 AND lo_orderdate = d_datekey
 AND (c_city = 'UNITED KI1' OR c_city = 'UNITED KI5')
 AND (s_city = 'UNITED KI1' OR s_city = 'UNITED KI5')
 AND d_year >= 1992 AND d_year <= 1997
 GROUP BY c_city, s_city, d_year
 ORDER BY d_year ASC, revenue DESC;

 -- Q3.4: Drill down to a specific month (December 1997) for revenue by customer city and supplier city
 SELECT c_city, s_city, d_year, SUM(lo_revenue) AS revenue
 FROM customer, lineorder, supplier, date
 WHERE lo_custkey = c_custkey
 AND lo_suppkey = s_suppkey
 AND lo_orderdate = d_datekey
 AND (c_city = 'UNITED KI1' OR c_city = 'UNITED KI5')
 AND (s_city = 'UNITED KI1' OR s_city = 'UNITED KI5')
 AND d_yearmonth = 'Dec1997'
 GROUP BY c_city, s_city, d_year
 ORDER BY d_year ASC, revenue DESC;

 -- Q4.1: Measure aggregate profit (revenue - supply cost) for 'AMERICA' region and 'MFGR#1' or 'MFGR#2' manufacturers
 SELECT d_year, c_nation, SUM(lo_revenue - lo_supplycost) AS profit
 FROM date, customer, supplier, part, lineorder
 WHERE lo_custkey = c_custkey
 AND lo_suppkey = s_suppkey
 AND lo_partkey = p_partkey
 AND lo_orderdate = d_datekey
 AND c_region = 'AMERICA'
 AND s_region = 'AMERICA'
 AND (p_mfgr = 'MFGR#1' OR p_mfgr = 'MFGR#2')
 GROUP BY d_year, c_nation
 ORDER BY d_year, c_nation;

 -- Q4.2: Drill down profit by year, supplier nation, and product category for 1997 and 1998
 SELECT d_year, s_nation, p_category, SUM(lo_revenue - lo_supplycost) AS profit
 FROM date, customer, supplier, part, lineorder
 WHERE lo_custkey = c_custkey
 AND lo_suppkey = s_suppkey
 AND lo_partkey = p_partkey
 AND lo_orderdate = d_datekey
 AND c_region = 'AMERICA'
 AND s_region = 'AMERICA'
 AND (d_year = 1997 OR d_year = 1998)
 AND (p_mfgr = 'MFGR#1' OR p_mfgr = 'MFGR#2')
 GROUP BY d_year, s_nation, p_category
 ORDER BY d_year, s_nation, p_category;

 -- Q4.3: Drill down further to cities in the 'UNITED STATES' and product brand within 'MFGR#14' category
 SELECT d_year, s_city, p_brand1, SUM(lo_revenue - lo_supplycost) AS profit
 FROM date, customer, supplier, part, lineorder
 WHERE lo_custkey = c_custkey
 AND lo_suppkey = s_suppkey
 AND lo_partkey = p_partkey
 AND lo_orderdate = d_datekey
 AND c_region = 'AMERICA'
 AND s_nation = 'UNITED STATES'
 AND (d_year = 1997 OR d_year = 1998)
 AND p_category = 'MFGR#14'
 GROUP BY d_year, s_city, p_brand1
 ORDER BY d_year, s_city, p_brand1;