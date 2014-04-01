SELECT
  nation.name,
  sum(lineitem.extendedprice * (1 - lineitem.discount)) AS revenue 
FROM
  customer, orders, lineitem, nation, region
WHERE
  customer.custkey = orders.custkey
  and lineitem.orderkey = orders.orderkey
  and customer.nationkey = nation.nationkey 
  and nation.regionkey = region.regionkey
  and region.name = 'ASIA'
  and orders.orderdate >= DATE( '1994-01-01')
  and orders.orderdate < DATE ('1995-01-01')
GROUP BY nation.name
ORDER BY revenue desc;

