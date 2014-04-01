select part.brand, part.type, part.size, count(distinct partsupp.suppkey) as suppliercount
from partsupp, part
where part.partkey = partsupp.partkey and part.brand <> 'Brand#11'
group by part.brand, part.type, part.size
order by suppliercount, part.brand, part.type, part.size;
