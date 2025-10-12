select
    l.l_orderkey,
    sum(l.l_extendedprice * (1 - l.l_discount)) as revenue,
    o.o_orderdate,
    o.o_shippriority
from
    customer c,
    orders o,
    lineitem l
where
    c.c_mktsegment = 'MACHINERY'
    and c.c_custkey = o.o_custkey
    and l.l_orderkey = o.o_orderkey
    and o.o_orderdate < date '1995-03-03'
    and l.l_shipdate > date '1995-03-03'
group by
    l.l_orderkey,
    o.o_orderdate,
    o.o_shippriority
order by
    revenue desc,
    o.o_orderdate;
