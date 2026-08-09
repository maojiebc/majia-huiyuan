你是一个ETL专家, 正在查看如下ETL的定义(注意: 这里的所有节点都会运行在 Apache Spark 3.4上, 所有的SQL语法都是Spark的语法, 当用户让优化性能时, 不要给出建索引等通用的建议, 因为Spark不能建立索引, 并且这个ETL只能使用Spark SQL(不能使用DataFrame API), 主要给出可以"通过优化ETL节点的写法来优化性能"这种优化建议):

## 基本信息
- UniformResourceType: DATA_PROCESS_ETL
---
## ETL 流程摘要

- **总节点数:** 8
- **节点类型分布:**
  - CALCULATOR: 2
  - FILTER_ROWS: 1
  - GROUP_BY: 1
  - INPUT_DATASET: 2
  - JOIN_DATA: 1
  - OUTPUT_DATASET: 1
- **数据输入源:**
  - j23ea7e60564e47458b71d82 (dwd_订单)
  - sedfdd84abacc4cb496c15e7 (dim_门店主档)
- **数据输出目标:**
  - dws_新店爬坡_Comp老店 (目录: 马甲的demo-0523)
---
## ETL 节点详细信息


### 节点1
- Id: id_1779337109457
- Name: dwd_订单
- Type: INPUT_DATASET
- **Used By (Outputs):**
  - id_1779337109459 (筛选已完成订单)
- Position: (227,64)
- InputDsId: j23ea7e60564e47458b71d82
- DisplayType: CSV
- PreviewScope: ALL
- 等价SQL:
```sql
SELECT * FROM input
```


### 节点2
- Id: id_1779337109458
- Name: dim_门店主档
- Type: INPUT_DATASET
- **Used By (Outputs):**
  - id_1779337109462 (关联门店维度)
- Position: (839,232)
- InputDsId: sedfdd84abacc4cb496c15e7
- DisplayType: EXCEL
- PreviewScope: ALL
- 等价SQL:
```sql
SELECT * FROM input
```


### 节点3
- Id: id_1779337109459
- Name: 筛选已完成订单
- Type: FILTER_ROWS
- **Sources (Inputs):**
  - id_1779337109457 (dwd_订单)

- **Used By (Outputs):**
  - id_1779337109460 (标记订单/会员订单)
- Position: (431,64)
- 等价SQL:
```sql
WITH params AS (
  -- 统一运行参数：调度到其他快照时只替换此处
  SELECT DATE '2026-06-24' AS as_of_date
)
SELECT
  o.*,
  p.as_of_date AS `数据快照日期`
FROM input1 o
CROSS JOIN params p
WHERE o.`订单状态` = '已完成'
  AND o.`业务日期` IS NOT NULL
  AND CAST(o.`业务日期` AS DATE) <= p.as_of_date
```


### 节点4
- Id: id_1779337109460
- Name: 标记订单/会员订单
- Type: CALCULATOR
- **Sources (Inputs):**
  - id_1779337109459 (筛选已完成订单)

- **Used By (Outputs):**
  - id_1779337109461 (门店日聚合)
- Position: (635,64)
- FormulaNames:
  - 订单计数
  - 会员订单计数
- 等价SQL:
```sql
SELECT
  *,
  1 AS `订单计数`,
  case when `会员ID` is not null and `会员ID` <> '' then 1 else 0 end AS `会员订单计数`
FROM input1
```


### 节点5
- Id: id_1779337109461
- Name: 门店日聚合
- Type: GROUP_BY
- **Sources (Inputs):**
  - id_1779337109460 (标记订单/会员订单)

- **Used By (Outputs):**
  - id_1779337109462 (关联门店维度)
- Position: (839,64)
- 等价SQL:
```sql
SELECT
  `门店ID`,
  CAST(`业务日期` AS DATE) AS `业务日期`,
  `数据快照日期`,
  COUNT(DISTINCT `订单ID`) AS `订单数`,
  SUM(COALESCE(`实付金额`, 0)) AS `销售额`,
  COUNT(DISTINCT CASE
    WHEN `会员ID` IS NOT NULL AND `会员ID` <> '' THEN `订单ID`
  END) AS `会员订单数`
FROM input1
GROUP BY `门店ID`, CAST(`业务日期` AS DATE), `数据快照日期`
```


### 节点6
- Id: id_1779337109462
- Name: 关联门店维度
- Type: JOIN_DATA
- **Sources (Inputs):**
  - id_1779337109461 (门店日聚合)
  - id_1779337109458 (dim_门店主档)

- **Used By (Outputs):**
  - id_1779337109463 (计算爬坡阶段+成长类型)
- Position: (1043,64)
- 等价SQL:
```sql
SELECT
  f.`门店ID`,
  d.`门店名称`,
  d.`城市`,
  d.`城市层级`,
  d.`店型`,
  d.`商圈`,
  CAST(d.`开业日期` AS DATE) AS `开业日期`,
  f.`业务日期`,
  f.`订单数`,
  f.`销售额`,
  f.`会员订单数`,
  f.`数据快照日期`
FROM input1 f
LEFT OUTER JOIN input2 d
  ON f.`门店ID` = d.`门店ID`
 AND f.`业务日期` >= CAST(d.`生效起始日期` AS DATE)
 AND f.`业务日期` <= COALESCE(CAST(d.`生效截止日期` AS DATE), DATE '9999-12-31')
```


### 节点7
- Id: id_1779337109463
- Name: 计算爬坡阶段+成长类型
- Type: CALCULATOR
- **Sources (Inputs):**
  - id_1779337109462 (关联门店维度)

- **Used By (Outputs):**
  - id_1779337109464 (dws_新店爬坡_Comp老店)
- Position: (1247,64)
- FormulaNames:
  - 新店标签（按事实日重算）
  - 是否90天内新店（按事实日重算）
  - 开业天数
  - 爬坡阶段
  - 门店成长类型
  - 平均客单价
  - 会员订单占比
- 等价SQL:
```sql
WITH base AS (
  SELECT
    *,
    CAST(DATEDIFF(`业务日期`, `开业日期`) AS BIGINT) AS `开业天数`
  FROM input1
)
SELECT
  `门店ID`, `门店名称`, `城市`, `城市层级`, `店型`, `商圈`,
  CASE
    WHEN `开业天数` IS NULL OR `开业天数` < 0 THEN '待核验'
    WHEN `开业天数` <= 89 THEN '新店'
    ELSE '非新店'
  END AS `新店标签`,
  CASE
    WHEN `开业天数` IS NULL THEN CAST(NULL AS STRING)
    WHEN `开业天数` BETWEEN 0 AND 89 THEN 'TRUE'
    ELSE 'FALSE'
  END AS `是否90天内新店`,
  `开业日期`, `业务日期`, `订单数`, `销售额`, `会员订单数`, `开业天数`,
  CASE
    WHEN `开业天数` IS NULL OR `开业天数` < 0 THEN '待核验'
    WHEN `开业天数` <= 7  THEN 'W1'
    WHEN `开业天数` <= 14 THEN 'W2'
    WHEN `开业天数` <= 30 THEN 'M1'
    WHEN `开业天数` <= 60 THEN 'M2'
    WHEN `开业天数` <= 89 THEN 'M3'
    ELSE 'Comp老店'
  END AS `爬坡阶段`,
  CASE
    WHEN `开业天数` IS NULL OR `开业天数` < 0 THEN '待核验'
    WHEN `开业天数` <= 89  THEN '新店'
    WHEN `开业天数` <= 365 THEN '次新店'
    ELSE 'Comp老店'
  END AS `门店成长类型`,
  CASE
    WHEN `订单数` > 0 THEN `销售额` * 1.0 / `订单数`
    ELSE CAST(NULL AS DOUBLE)
  END AS `平均客单价`,
  CASE
    WHEN `订单数` > 0 THEN `会员订单数` * 1.0 / `订单数`
    ELSE CAST(NULL AS DOUBLE)
  END AS `会员订单占比`,
  `数据快照日期`
FROM base
```


### 节点8
- Id: id_1779337109464
- Name: dws_新店爬坡_Comp老店
- Type: OUTPUT_DATASET
- **Sources (Inputs):**
  - id_1779337109463 (计算爬坡阶段+成长类型)
- Position: (1451,64)
- OutputDsName: dws_新店爬坡_Comp老店
- ParentDirId: v2b6bde3d41444cfd9e6d7ef
- ParentDirName: 马甲的demo-0523
- DataSourceDsId: o88b336d58b5047de98993b1
- DataSourceCreated: true
- DirPath: 根目录 > 马甲的demo-0523
- 等价SQL:
```sql
SELECT * FROM input1
```


---

## 血缘关系

### 上游资源 (2)
- **dim_门店主档** (DATA_SET_FILE)
  - ID: sedfdd84abacc4cb496c15e7
- **dwd_订单** (DATA_SET_FILE)
  - ID: j23ea7e60564e47458b71d82

### 下游资源 (1)
- **dws_新店爬坡_Comp老店** (DATA_SET_ETL)
  - ID: o88b336d58b5047de98993b1
