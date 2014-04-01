select lineitem.shipmode, count(distinct orders.orderkey)
from orders, lineitem
where orders.orderkey = lineitem.orderkey
and (lineitem.shipmode='MAIL' or lineitem.shipmode='SHIP')
and orders.orderpriority <> '1-URGENT' and orders.orderpriority <> '2-HIGH'
and lineitem.commitdate < lineitem.receiptdate
and lineitem.shipdate < lineitem.commitdate
and lineitem.receiptdate >= date('1994-01-01')
and lineitem.receiptdate < date('1995-01-01')
group by lineitem.shipmode
order by lineitem.shipmode;
