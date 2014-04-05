CREATE TABLE PART (
        partkey      INT,
        name         VARCHAR(55),
        mfgr         VARCHAR(25),
        brand        VARCHAR(10),
        type         VARCHAR(25),
        size         INT,
        container    VARCHAR(10),
        retailprice  DECIMAL,
        comment      VARCHAR(23)
    );

CREATE TABLE SUPPLIER (
        suppkey      INT,
        name         VARCHAR(25),
        address      VARCHAR(40),
        nationkey    INT,
        phone        VARCHAR(15),
        acctbal      DECIMAL,
        comment      VARCHAR(101)
    );

CREATE TABLE PARTSUPP (
        partkey      INT,
        suppkey      INT,
        availqty     INT,
        supplycost   DECIMAL,
        comment      VARCHAR(199)
    );

CREATE TABLE NATION (
        nationkey    INT,
        name         VARCHAR(25),
        regionkey    INT,
        comment      VARCHAR(152)
    );

CREATE TABLE REGION (
        regionkey    INT,
        name         VARCHAR(25),
        comment      VARCHAR(152)
    );

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
