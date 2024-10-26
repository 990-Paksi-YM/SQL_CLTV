CREATE OR REPLACE TABLE dqlab-yudha-sample1.dqlab.temp2017 AS 
SELECT
  Customer_ID,
  SUM(Sales) AS sumsales,
  SUM(Profit) AS sumprofit,
  COUNT(Order_ID) AS countorder,
  MAX(Order_Date) AS maxorderdate,
  MIN(Order_Date) AS minorderdate,
  2017 AS yeary
FROM
  `dqlab-yudha-sample1.dqlab.SuperStores`
WHERE
  Order_Date <= '2017-12-31'
GROUP BY
  Customer_ID
;

CREATE OR REPLACE TABLE dqlab-yudha-sample1.dqlab.temp2018 AS 
SELECT
  Customer_ID,
  SUM(Sales) AS sumsales,
  SUM(Profit) AS sumprofit,
  COUNT(Order_ID) AS countorder,
  MAX(Order_Date) AS maxorderdate,
  MIN(Order_Date) AS minorderdate,
  2018 AS yeary
FROM
  `dqlab-yudha-sample1.dqlab.SuperStores`
WHERE
  Order_Date >= '2018-01-01'
  AND
  Order_Date <= '2018-12-31'
GROUP BY
  1
;

CREATE OR REPLACE TABLE dqlab-yudha-sample1.dqlab.temp2019 AS 
SELECT
  Customer_ID,
  SUM(Sales) AS sumsales,
  SUM(Profit) AS sumprofit,
  COUNT(Order_ID) AS countorder,
  MAX(Order_Date) AS maxorderdate,
  MIN(Order_Date) AS minorderdate,
  2019 AS yeary
FROM
  `dqlab-yudha-sample1.dqlab.SuperStores`
WHERE
  Order_Date >= '2019-01-01'
  AND
  Order_Date <= '2019-12-31'
GROUP BY
  1
;

CREATE OR REPLACE TABLE dqlab-yudha-sample1.dqlab.temp2020 AS 
SELECT
  Customer_ID,
  SUM(Sales) AS sumsales,
  SUM(Profit) AS sumprofit,
  COUNT(Order_ID) AS countorder,
  MAX(Order_Date) AS maxorderdate,
  MIN(Order_Date) AS minorderdate,
  2020 AS yeary
FROM
  `dqlab-yudha-sample1.dqlab.SuperStores`
WHERE
  Order_Date >= '2020-01-01'
  AND
  Order_Date <= '2020-12-31'
GROUP BY
  1
;

WITH AOV2017 AS (
  SELECT
    Customer_ID,
    (sumsales / countorder) AS aovsales,
    (sumprofit / countorder) AS aovprofit
  FROM
    dqlab-yudha-sample1.dqlab.temp2017
),

AOV2018 AS (
  SELECT
    Customer_ID,
    (sumsales / countorder) AS aovsales,
    (sumprofit / countorder) AS aovprofit
  FROM
    dqlab-yudha-sample1.dqlab.temp2018
),

AOV2019 AS (
  SELECT
    Customer_ID,
    (sumsales / countorder) AS aovsales,
    (sumprofit / countorder) AS aovprofit
  FROM
    dqlab-yudha-sample1.dqlab.temp2019
),

AOV2020 AS (
  SELECT
    Customer_ID,
    (sumsales / countorder) AS aovsales,
    (sumprofit / countorder) AS aovprofit
  FROM
    dqlab-yudha-sample1.dqlab.temp2020
),

PF2017 AS (
  SELECT
    Customer_ID,
    AVG(countorder) OVER (PARTITION BY Customer_ID) AS pfnew
  FROM
    dqlab-yudha-sample1.dqlab.temp2017
),

PF2018 AS (
  SELECT
    Customer_ID,
    AVG(countorder) OVER (PARTITION BY Customer_ID) AS pfnew
  FROM
    dqlab-yudha-sample1.dqlab.temp2018
),

PF2019 AS (
  SELECT
    Customer_ID,
    AVG(countorder) OVER (PARTITION BY Customer_ID) AS pfnew
  FROM
    dqlab-yudha-sample1.dqlab.temp2019
),

PF2020 AS (
  SELECT
    Customer_ID,
    AVG(countorder) OVER (PARTITION BY Customer_ID) AS pfnew
  FROM
    dqlab-yudha-sample1.dqlab.temp2020
),

CL2017 AS (
  SELECT
    Customer_ID,
    CASE WHEN 
    DATE_DIFF(maxorderdate , minorderdate, DAY) = 0 THEN 1 ELSE DATE_DIFF(maxorderdate , minorderdate, DAY) 
    END AS clnew
  FROM
    dqlab-yudha-sample1.dqlab.temp2017
),

CL2018 AS (
  SELECT
    Customer_ID,
    CASE WHEN 
    DATE_DIFF(maxorderdate , minorderdate, DAY) = 0 THEN 1 ELSE DATE_DIFF(maxorderdate , minorderdate, DAY) 
    END AS clnew
  FROM
    dqlab-yudha-sample1.dqlab.temp2018
),

CL2019 AS (
  SELECT
    Customer_ID,
    CASE WHEN 
    DATE_DIFF(maxorderdate , minorderdate, DAY) = 0 THEN 1 ELSE DATE_DIFF(maxorderdate , minorderdate, DAY) 
    END AS clnew
  FROM
    dqlab-yudha-sample1.dqlab.temp2019
),

CL2020 AS (
  SELECT
    Customer_ID,
    CASE WHEN 
    DATE_DIFF(maxorderdate , minorderdate, DAY) = 0 THEN 1 ELSE DATE_DIFF(maxorderdate , minorderdate, DAY) 
    END AS clnew
  FROM
    dqlab-yudha-sample1.dqlab.temp2020
),

CLTV2017 AS (
  SELECT
    temp2017.yeary,
    temp2017.Customer_ID,
    AOV2017.aovsales,
    AOV2017.aovprofit,
    PF2017.pfnew,
    CL2017.clnew,
    (AOV2017.aovsales * PF2017.pfnew * CL2017.clnew) AS cltvsales,
    (AOV2017.aovprofit * PF2017.pfnew * CL2017.clnew) AS cltvprofit
  FROM
    dqlab-yudha-sample1.dqlab.temp2017
      JOIN AOV2017 ON temp2017.Customer_ID = AOV2017.Customer_ID
      JOIN PF2017 ON temp2017.Customer_ID = PF2017.Customer_ID
      JOIN CL2017 ON temp2017.Customer_ID = CL2017.Customer_ID
),

CLTV2018 AS (
  SELECT
    temp2018.yeary,
    temp2018.Customer_ID,
    AOV2018.aovsales,
    AOV2018.aovprofit,
    PF2018.pfnew,
    CL2018.clnew,
    (AOV2018.aovsales * PF2018.pfnew * CL2018.clnew) AS cltvsales,
    (AOV2018.aovprofit * PF2018.pfnew * CL2018.clnew) AS cltvprofit
  FROM
    dqlab-yudha-sample1.dqlab.temp2018
      JOIN AOV2018 ON temp2018.Customer_ID = AOV2018.Customer_ID
      JOIN PF2018 ON temp2018.Customer_ID = PF2018.Customer_ID
      JOIN CL2018 ON temp2018.Customer_ID = CL2018.Customer_ID
),

CLTV2019 AS (
  SELECT
    temp2019.yeary,
    temp2019.Customer_ID,
    AOV2019.aovsales,
    AOV2019.aovprofit,
    PF2019.pfnew,
    CL2019.clnew,
    (AOV2019.aovsales * PF2019.pfnew * CL2019.clnew) AS cltvsales,
    (AOV2019.aovprofit * PF2019.pfnew * CL2019.clnew) AS cltvprofit
  FROM
    dqlab-yudha-sample1.dqlab.temp2019
      JOIN AOV2019 ON temp2019.Customer_ID = AOV2019.Customer_ID
      JOIN PF2019 ON temp2019.Customer_ID = PF2019.Customer_ID
      JOIN CL2019 ON temp2019.Customer_ID = CL2019.Customer_ID
),

CLTV2020 AS (
  SELECT
    temp2020.yeary,
    temp2020.Customer_ID,
    AOV2020.aovsales,
    AOV2020.aovprofit,
    PF2020.pfnew,
    CL2020.clnew,
    (AOV2020.aovsales * PF2020.pfnew * CL2020.clnew) AS cltvsales,
    (AOV2020.aovprofit * PF2020.pfnew * CL2020.clnew) AS cltvprofit
  FROM
    dqlab-yudha-sample1.dqlab.temp2020
      JOIN AOV2020 ON temp2020.Customer_ID = AOV2020.Customer_ID
      JOIN PF2020 ON temp2020.Customer_ID = PF2020.Customer_ID
      JOIN CL2020 ON temp2020.Customer_ID = CL2020.Customer_ID
),

newcltvdb as (
  select
    CLTV2017.yeary,
    CLTV2017.Customer_ID,
    CLTV2017.aovsales,
    CLTV2017.aovprofit,
    CLTV2017.pfnew,
    CLTV2017.clnew,
    CLTV2017.cltvsales,
    CLTV2017.cltvprofit
  from
    CLTV2017
  union all
  select
    CLTV2018.yeary,
    CLTV2018.Customer_ID,
    CLTV2018.aovsales,
    CLTV2018.aovprofit,
    CLTV2018.pfnew,
    CLTV2018.clnew,
    CLTV2018.cltvsales,
    CLTV2018.cltvprofit
  from
    CLTV2018
  union all
  select
    CLTV2019.yeary,
    CLTV2019.Customer_ID,
    CLTV2019.aovsales,
    CLTV2019.aovprofit,
    CLTV2019.pfnew,
    CLTV2019.clnew,
    CLTV2019.cltvsales,
    CLTV2019.cltvprofit
  from
    CLTV2019
  union all
  select
    CLTV2020.yeary,
    CLTV2020.Customer_ID,
    CLTV2020.aovsales,
    CLTV2020.aovprofit,
    CLTV2020.pfnew,
    CLTV2020.clnew,
    CLTV2020.cltvsales,
    CLTV2020.cltvprofit
  from
    CLTV2020
),

/* 1 = increase , 0 = decrease, same = 2, else = 5 */

dbcompare as (
  select
    yeary,
    Customer_ID,
    aovsales,
    aovprofit,
    pfnew,
    clnew,
    cltvsales,
    cltvprofit,
    lag(aovsales) over(partition by Customer_ID order by yeary) as prevaovsales,
    lag(aovprofit) over(partition by Customer_ID order by yeary) as prevaovprofit,
    lag(pfnew) over(partition by Customer_ID order by yeary) as prevpfnew,
    lag(clnew) over(partition by Customer_ID order by yeary) as prevclnew,
    lag(cltvsales) over(partition by Customer_ID order by yeary) as prevcltvsales,
    lag(cltvprofit) over(partition by Customer_ID order by yeary) as prevcltvprofit,
    case
      when aovsales > lag(aovsales) over(partition by Customer_ID order by yeary) then 1
      when aovsales < lag(aovsales) over(partition by Customer_ID order by yeary) then 0
      when aovsales = lag(aovsales) over(partition by Customer_ID order by yeary) then 2
      else 5 end as aovsalesstatus,
    case
      when pfnew > lag(pfnew) over(partition by Customer_ID order by yeary) then 1
      when pfnew < lag(pfnew) over(partition by Customer_ID order by yeary) then 0
      when pfnew = lag(pfnew) over(partition by Customer_ID order by yeary) then 2
      else 5 end as pfnewstatus,
    case
      when clnew > lag(clnew) over(partition by Customer_ID order by yeary) then 1
      when clnew < lag(clnew) over(partition by Customer_ID order by yeary) then 0
      when clnew = lag(clnew) over(partition by Customer_ID order by yeary) then 2
      else 5 end as clnewstatus,
    case
      when cltvsales > lag(cltvsales) over(partition by Customer_ID order by yeary) then 1
      when cltvsales < lag(cltvsales) over(partition by Customer_ID order by yeary) then 0
      when cltvsales = lag(cltvsales) over(partition by Customer_ID order by yeary) then 2
      else 5 end as cltvsalesstatus,
    case
      when cltvprofit > lag(cltvprofit) over(partition by Customer_ID order by yeary) then 1
      when cltvprofit < lag(cltvprofit) over(partition by Customer_ID order by yeary) then 0
      when cltvprofit = lag(cltvprofit) over(partition by Customer_ID order by yeary) then 2
      else 5 end as cltvprofitstatus      
  from
    newcltvdb
)

select
  db.Customer_ID,
  ds.Sub_Category,
  ds.Product_Name,
  ds.prch
from
  dbcompare as db
inner join
  (select
    Customer_ID,
    Sub_Category,
    Product_Name,
    sum(sales) as prch
  FROM
    `dqlab-yudha-sample1.dqlab.SuperStores`
  group by
    1,2,3
  ) as ds
  using(Customer_ID)
where
  db.cltvsalesstatus <> 0
  and
  db.cltvsalesstatus <> 5
group by
  1,2,3,4
having
  sum(db.cltvsalesstatus) = 3
;
