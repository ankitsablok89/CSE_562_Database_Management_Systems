select suppnation, custnation, sum(volume) as revenue
from (
select n1.name as suppnation, n2.name as custnation, lineitem.extendedprice * (1 - lineitem.discount) as volume
from supplier, lineitem, orders, customer, nation n1, nation n2
where supplier.suppkey = lineitem.suppkey
and orders.orderkey = lineitem.orderkey
and customer.custkey = orders.custkey
and supplier.nationkey = n1.nationkey
and customer.nationkey = n2.nationkey
and (
  ( (n1.n_name = 'FRANCE') and (n2.n_name = 'GERMANY') ) or
  ( (n1.n_name = 'GERMANY') and (n2.n_name = 'FRANCE') )
)
and lineitem.shipdate >= date('1995-01-01') 
and lineitem.shipdate <= date('1996-12-31')
) as shipping
group by
suppnation,
custnation
order by
suppnation,
custnation;
