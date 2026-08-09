你是一个ETL专家, 正在查看如下ETL的定义(注意: 这里的所有节点都会运行在 Apache Spark 3.4上, 所有的SQL语法都是Spark的语法, 当用户让优化性能时, 不要给出建索引等通用的建议, 因为Spark不能建立索引, 并且这个ETL只能使用Spark SQL(不能使用DataFrame API), 主要给出可以"通过优化ETL节点的写法来优化性能"这种优化建议):

## 基本信息
- UniformResourceType: DATA_PROCESS_ETL
---
## ETL 流程摘要

- **总节点数:** 4
- **节点类型分布:**
  - INPUT_DATASET: 2
  - OUTPUT_DATASET: 1
  - SQL_SCRIPT: 1
- **数据输入源:**
  - h551155a12fc04d88a57d319 (dim_会员主档)
  - j23ea7e60564e47458b71d82 (dwd_订单)
- **数据输出目标:**
  - dws_会员同期群留存 (目录: 0523-马甲-demo)
---
## ETL 节点详细信息


### 节点1
- Id: id_1779326818722
- Name: dim_会员主档
- Type: INPUT_DATASET
- **Used By (Outputs):**
  - id_1779326818724 (SQL处理)
- Position: (200,100)
- InputDsId: h551155a12fc04d88a57d319
- DisplayType: CSV
- PreviewScope: ALL
- 等价SQL:
```sql
SELECT * FROM input
```


### 节点2
- Id: id_1779326818723
- Name: dwd_订单
- Type: INPUT_DATASET
- **Used By (Outputs):**
  - id_1779326818724 (SQL处理)
- Position: (200,250)
- InputDsId: j23ea7e60564e47458b71d82
- DisplayType: CSV
- PreviewScope: ALL
- 等价SQL:
```sql
SELECT * FROM input
```


### 节点3
- Id: id_1779326818724
- Name: SQL处理
- Type: SQL_SCRIPT
- **Sources (Inputs):**
  - id_1779326818722 (dim_会员主档)
  - id_1779326818723 (dwd_订单)

- **Used By (Outputs):**
  - id_1779326818725 (dws_会员同期群留存)
- Position: (500,100)
- SqlScript:
```sql
WITH params AS (
  -- 统一运行参数：调度到其他快照时只替换此处
  SELECT DATE '2026-06-24' AS as_of_date
),
member_base AS (
  SELECT
    m.`会员ID`,
    CAST(m.`注册日期` AS DATE) AS `注册日期`,
    CAST(DATE_TRUNC('MONTH', CAST(m.`注册日期` AS DATE)) AS DATE) AS `同期群月份`
  FROM input1 m
  CROSS JOIN params p
  WHERE m.`会员ID` IS NOT NULL
    AND m.`会员ID` <> ''
    AND m.`注册日期` IS NOT NULL
    AND CAST(m.`注册日期` AS DATE) <= p.as_of_date
),
cohort_sizes AS (
  SELECT
    `同期群月份`,
    COUNT(DISTINCT `会员ID`) AS `同期群人数`
  FROM member_base
  GROUP BY `同期群月份`
),
completed_orders AS (
  SELECT DISTINCT
    o.`会员ID`,
    CAST(o.`业务日期` AS DATE) AS `订单日期`
  FROM input2 o
  CROSS JOIN params p
  WHERE o.`订单状态` = '已完成'
    AND o.`会员ID` IS NOT NULL
    AND o.`会员ID` <> ''
    AND o.`业务日期` IS NOT NULL
    AND CAST(o.`业务日期` AS DATE) <= p.as_of_date
),
member_activity AS (
  SELECT DISTINCT
    m.`会员ID`,
    m.`同期群月份`,
    CAST(DATE_TRUNC('MONTH', o.`订单日期`) AS DATE) AS `活跃月份`
  FROM member_base m
  JOIN completed_orders o
    ON m.`会员ID` = o.`会员ID`
   AND o.`订单日期` >= m.`注册日期`
),
retention_counts AS (
  SELECT
    `同期群月份`,
    `活跃月份`,
    COUNT(DISTINCT `会员ID`) AS `留存人数`
  FROM member_activity
  GROUP BY `同期群月份`, `活跃月份`
),
cohort_grid AS (
  SELECT
    c.`同期群月份`,
    c.`同期群人数`,
    p.as_of_date,
    EXPLODE(
      SEQUENCE(
        0,
        CAST(
          MONTHS_BETWEEN(
            CAST(DATE_TRUNC('MONTH', p.as_of_date) AS DATE),
            c.`同期群月份`
          ) AS INT
        )
      )
    ) AS `留存月序号`
  FROM cohort_sizes c
  CROSS JOIN params p
)
SELECT
  g.`同期群月份`,
  g.`同期群人数`,
  CONCAT('M', CAST(g.`留存月序号` AS STRING)) AS `留存月份序号`,
  ADD_MONTHS(g.`同期群月份`, g.`留存月序号`) AS `留存月份`,
  CASE
    WHEN g.`留存月序号` = 0 THEN g.`同期群人数`
    WHEN LAST_DAY(ADD_MONTHS(g.`同期群月份`, g.`留存月序号`)) <= g.as_of_date
      THEN COALESCE(r.`留存人数`, 0)
    ELSE CAST(NULL AS BIGINT)
  END AS `留存人数`,
  CASE
    WHEN g.`留存月序号` = 0 THEN CAST(1.0 AS DOUBLE)
    WHEN LAST_DAY(ADD_MONTHS(g.`同期群月份`, g.`留存月序号`)) <= g.as_of_date
      THEN ROUND(COALESCE(r.`留存人数`, 0) * 1.0 / g.`同期群人数`, 4)
    ELSE CAST(NULL AS DOUBLE)
  END AS `留存率`,
  CASE
    WHEN LAST_DAY(ADD_MONTHS(g.`同期群月份`, g.`留存月序号`)) <= g.as_of_date
      THEN CAST(1 AS BIGINT)
    ELSE CAST(0 AS BIGINT)
  END AS `是否完整观察期`,
  g.as_of_date AS `数据快照日期`
FROM cohort_grid g
LEFT JOIN retention_counts r
  ON g.`同期群月份` = r.`同期群月份`
 AND ADD_MONTHS(g.`同期群月份`, g.`留存月序号`) = r.`活跃月份`
```
- 等价SQL:
```sql
WITH params AS (
  -- 统一运行参数：调度到其他快照时只替换此处
  SELECT DATE '2026-06-24' AS as_of_date
),
member_base AS (
  SELECT
    m.`会员ID`,
    CAST(m.`注册日期` AS DATE) AS `注册日期`,
    CAST(DATE_TRUNC('MONTH', CAST(m.`注册日期` AS DATE)) AS DATE) AS `同期群月份`
  FROM input1 m
  CROSS JOIN params p
  WHERE m.`会员ID` IS NOT NULL
    AND m.`会员ID` <> ''
    AND m.`注册日期` IS NOT NULL
    AND CAST(m.`注册日期` AS DATE) <= p.as_of_date
),
cohort_sizes AS (
  SELECT
    `同期群月份`,
    COUNT(DISTINCT `会员ID`) AS `同期群人数`
  FROM member_base
  GROUP BY `同期群月份`
),
completed_orders AS (
  SELECT DISTINCT
    o.`会员ID`,
    CAST(o.`业务日期` AS DATE) AS `订单日期`
  FROM input2 o
  CROSS JOIN params p
  WHERE o.`订单状态` = '已完成'
    AND o.`会员ID` IS NOT NULL
    AND o.`会员ID` <> ''
    AND o.`业务日期` IS NOT NULL
    AND CAST(o.`业务日期` AS DATE) <= p.as_of_date
),
member_activity AS (
  SELECT DISTINCT
    m.`会员ID`,
    m.`同期群月份`,
    CAST(DATE_TRUNC('MONTH', o.`订单日期`) AS DATE) AS `活跃月份`
  FROM member_base m
  JOIN completed_orders o
    ON m.`会员ID` = o.`会员ID`
   AND o.`订单日期` >= m.`注册日期`
),
retention_counts AS (
  SELECT
    `同期群月份`,
    `活跃月份`,
    COUNT(DISTINCT `会员ID`) AS `留存人数`
  FROM member_activity
  GROUP BY `同期群月份`, `活跃月份`
),
cohort_grid AS (
  SELECT
    c.`同期群月份`,
    c.`同期群人数`,
    p.as_of_date,
    EXPLODE(
      SEQUENCE(
        0,
        CAST(
          MONTHS_BETWEEN(
            CAST(DATE_TRUNC('MONTH', p.as_of_date) AS DATE),
            c.`同期群月份`
          ) AS INT
        )
      )
    ) AS `留存月序号`
  FROM cohort_sizes c
  CROSS JOIN params p
)
SELECT
  g.`同期群月份`,
  g.`同期群人数`,
  CONCAT('M', CAST(g.`留存月序号` AS STRING)) AS `留存月份序号`,
  ADD_MONTHS(g.`同期群月份`, g.`留存月序号`) AS `留存月份`,
  CASE
    WHEN g.`留存月序号` = 0 THEN g.`同期群人数`
    WHEN LAST_DAY(ADD_MONTHS(g.`同期群月份`, g.`留存月序号`)) <= g.as_of_date
      THEN COALESCE(r.`留存人数`, 0)
    ELSE CAST(NULL AS BIGINT)
  END AS `留存人数`,
  CASE
    WHEN g.`留存月序号` = 0 THEN CAST(1.0 AS DOUBLE)
    WHEN LAST_DAY(ADD_MONTHS(g.`同期群月份`, g.`留存月序号`)) <= g.as_of_date
      THEN ROUND(COALESCE(r.`留存人数`, 0) * 1.0 / g.`同期群人数`, 4)
    ELSE CAST(NULL AS DOUBLE)
  END AS `留存率`,
  CASE
    WHEN LAST_DAY(ADD_MONTHS(g.`同期群月份`, g.`留存月序号`)) <= g.as_of_date
      THEN CAST(1 AS BIGINT)
    ELSE CAST(0 AS BIGINT)
  END AS `是否完整观察期`,
  g.as_of_date AS `数据快照日期`
FROM cohort_grid g
LEFT JOIN retention_counts r
  ON g.`同期群月份` = r.`同期群月份`
 AND ADD_MONTHS(g.`同期群月份`, g.`留存月序号`) = r.`活跃月份`
```


### 节点4
- Id: id_1779326818725
- Name: dws_会员同期群留存
- Type: OUTPUT_DATASET
- **Sources (Inputs):**
  - id_1779326818724 (SQL处理)
- Position: (800,100)
- OutputDsName: dws_会员同期群留存
- ParentDirId: v2b6bde3d41444cfd9e6d7ef
- ParentDirName: 0523-马甲-demo
- DataSourceDsId: t2a6721b5e2d04f58ad6b8f9
- DataSourceCreated: true
- DirPath: 根目录 > 0523-马甲-demo
- 等价SQL:
```sql
SELECT * FROM input1
```


---

## 血缘关系

### 上游资源 (2)
- **dim_会员主档** (DATA_SET_FILE)
  - ID: h551155a12fc04d88a57d319
- **dwd_订单** (DATA_SET_FILE)
  - ID: j23ea7e60564e47458b71d82

### 下游资源 (1)
- **dws_会员同期群留存** (DATA_SET_ETL)
  - ID: t2a6721b5e2d04f58ad6b8f9
