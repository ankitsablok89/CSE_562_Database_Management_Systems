SELECT
  s1.acctbal, s1.name, n1.name, p1.partkey, p1.mfgr, s1.address, s1.phone, s1.comment
FROM
  part p1, supplier s1, partsupp ps1, nation n1, region r1
WHERE
  p1.partkey = ps1.partkey
  AND s1.suppkey = ps1.suppkey and p1.size = 15
  AND p1.type like '%BRASS'
  AND s1.nationkey = n1.nationkey and n1.regionkey = r1.regionkey and r1.name = 'EUROPE'
  AND ps1.supplycost = ( 
  SELECT min(ps2.supplycost) 
                        FROM partsupp ps2, supplier s2, nation n2, region r2
                        WHERE
                            p1.partkey = ps2.partkey
                            AND s2.suppkey = ps2.suppkey 
                            AND s2.nationkey = n2.nationkey 
                            AND n2.regionkey = r2.regionkey 
                            AND r2.name = 'EUROPE'
                      ) 
ORDER BY s1.acctbal desc, n1.name, s1.name, p1.partkey;