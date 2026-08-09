你是一个ETL专家, 正在查看如下ETL的定义(注意: 这里的所有节点都会运行在 Apache Spark 3.4上, 所有的SQL语法都是Spark的语法, 当用户让优化性能时, 不要给出建索引等通用的建议, 因为Spark不能建立索引, 并且这个ETL只能使用Spark SQL(不能使用DataFrame API), 主要给出可以"通过优化ETL节点的写法来优化性能"这种优化建议):

## 基本信息
- UniformResourceType: DATA_PROCESS_ETL
---
## ETL 流程摘要

- **总节点数:** 10
- **节点类型分布:**
  - CALCULATOR: 2
  - FILTER_ROWS: 1
  - GROUP_BY: 1
  - INPUT_DATASET: 2
  - JOIN_DATA: 1
  - OUTPUT_DATASET: 1
  - SQL_SCRIPT: 2
- **数据输入源:**
  - j23ea7e60564e47458b71d82 (dwd_订单)
  - sedfdd84abacc4cb496c15e7 (dim_门店主档)
- **数据输出目标:**
  - dws_门店日报 (目录: 马甲的demo-0523)
- **运行参数:** `as_of_date`（本模拟快照默认 `2026-06-24`，调度时统一替换）
---
## ETL 节点详细信息


### 节点1
- Id: id_1779348711910
- Name: 门店日聚合(主)
- Type: GROUP_BY
- **Sources (Inputs):**
  - id_1779348711909 (派生标志字段)

- **Used By (Outputs):**
  - id_1779348711912 (合并去重会员数(2表 SQL JOIN))
- Position: (839,64)
- 等价SQL:
```sql
SELECT
  `门店ID`, `业务日期`,
  SUM(`订单计数`) AS `订单计数`,
  SUM(`实付金额`) AS `实付金额`,
  SUM(`原价金额`) AS `原价金额`,
  SUM(`折扣金额`) AS `折扣金额`,
  SUM(`商品件数`) AS `商品件数`,
  SUM(`会员订单计数`) AS `会员订单计数`,
  SUM(`到店销售`) AS `到店销售`,
  SUM(`外卖销售`) AS `外卖销售`,
  SUM(`到店订单计数`) AS `到店订单计数`,
  SUM(`外卖订单计数`) AS `外卖订单计数`
FROM input1
GROUP BY `门店ID`, `业务日期`
```


### 节点2
- Id: id_1779348711911
- Name: 去重会员数旁路
- Type: SQL_SCRIPT
- **Sources (Inputs):**
  - id_1779348711908 (筛选已完成订单)

- **Used By (Outputs):**
  - id_1779348711912 (合并去重会员数(2表 SQL JOIN))
- Position: (839,232)
- SqlScript:
```sql
SELECT `门店ID`, `业务日期`,
  COUNT(DISTINCT `会员ID`) AS `去重会员数`
FROM input1
WHERE `会员ID` IS NOT NULL AND `会员ID` <> ''
GROUP BY `门店ID`, `业务日期`
```
- 等价SQL:
```sql
SELECT `门店ID`, `业务日期`,
  COUNT(DISTINCT `会员ID`) AS `去重会员数`
FROM input1
WHERE `会员ID` IS NOT NULL AND `会员ID` <> ''
GROUP BY `门店ID`, `业务日期`
```


### 节点3
- Id: id_1779348711906
- Name: dwd_订单
- Type: INPUT_DATASET
- **Used By (Outputs):**
  - id_1779348711908 (筛选已完成订单)
- Position: (227,64)
- InputDsId: j23ea7e60564e47458b71d82
- DisplayType: CSV
- PreviewScope: ALL
- 等价SQL:
```sql
SELECT * FROM input
```


### 节点4
- Id: id_1779348711908
- Name: 筛选已完成订单
- Type: FILTER_ROWS
- **Sources (Inputs):**
  - id_1779348711906 (dwd_订单)

- **Used By (Outputs):**
  - id_1779348711911 (去重会员数旁路)
  - id_1779348711909 (派生标志字段)
- Position: (431,64)
- 等价SQL:
```sql
SELECT
  *
FROM input1
WHERE (`订单状态` = '已完成')
  AND `业务日期` <= CAST('${as_of_date}' AS DATE)
```


### 节点5
- Id: id_1779348711912
- Name: 合并去重会员数(2表 SQL JOIN)
- Type: SQL_SCRIPT
- **Sources (Inputs):**
  - id_1779348711910 (门店日聚合(主))
  - id_1779348711911 (去重会员数旁路)

- **Used By (Outputs):**
  - id_1779348711913 (关联门店信息)
- Position: (1043,64)
- SqlScript:
```sql
SELECT
  a.`门店ID`, a.`业务日期`,
  a.`订单计数` AS `订单数`,
  a.`实付金额` AS `销售额`,
  a.`原价金额` AS `原价销售额`,
  a.`折扣金额` AS `折扣金额合计`,
  a.`商品件数` AS `商品件数合计`,
  a.`会员订单计数` AS `会员订单数`,
  a.`到店销售` AS `到店销售额`,
  a.`外卖销售` AS `外卖销售额`,
  a.`到店订单计数` AS `到店订单数`,
  a.`外卖订单计数` AS `外卖订单数`,
  COALESCE(b.`去重会员数`, 0) AS `去重会员数`
FROM input1 a
LEFT JOIN input2 b ON a.`门店ID` = b.`门店ID` AND a.`业务日期` = b.`业务日期`
```
- 等价SQL:
```sql
SELECT
  a.`门店ID`, a.`业务日期`,
  a.`订单计数` AS `订单数`,
  a.`实付金额` AS `销售额`,
  a.`原价金额` AS `原价销售额`,
  a.`折扣金额` AS `折扣金额合计`,
  a.`商品件数` AS `商品件数合计`,
  a.`会员订单计数` AS `会员订单数`,
  a.`到店销售` AS `到店销售额`,
  a.`外卖销售` AS `外卖销售额`,
  a.`到店订单计数` AS `到店订单数`,
  a.`外卖订单计数` AS `外卖订单数`,
  COALESCE(b.`去重会员数`, 0) AS `去重会员数`
FROM input1 a
LEFT JOIN input2 b ON a.`门店ID` = b.`门店ID` AND a.`业务日期` = b.`业务日期`
```


### 节点6
- Id: id_1779348711907
- Name: dim_门店主档
- Type: INPUT_DATASET
- **Used By (Outputs):**
  - id_1779348711913 (关联门店信息)
- Position: (1043,232)
- InputDsId: sedfdd84abacc4cb496c15e7
- DisplayType: EXCEL
- PreviewScope: ALL
- 等价SQL:
```sql
SELECT * FROM input
```


### 节点7
- Id: id_1779348711913
- Name: 关联门店信息
- Type: JOIN_DATA
- **Sources (Inputs):**
  - id_1779348711912 (合并去重会员数(2表 SQL JOIN))
  - id_1779348711907 (dim_门店主档)

- **Used By (Outputs):**
  - id_1779348711914 (计算占比和均值)
- Position: (1247,64)
- 等价SQL:
```sql
WITH matched AS (
  SELECT
    f.*,
    s.`门店版本ID`, s.`门店名称`, s.`门店类型`, s.`省份`, s.`城市`,
    s.`城市层级`, s.`店型`, s.`商圈`, s.`品牌线`, s.`直营加盟类型`,
    s.`开业日期`, s.`闭店日期`,
    ROW_NUMBER() OVER (
      PARTITION BY f.`门店ID`, f.`业务日期`
      ORDER BY s.`生效起始日期` DESC, s.`门店版本ID` DESC
    ) AS scd_rn
  FROM input1 f
  LEFT JOIN input2 s
    ON f.`门店ID` = s.`门店ID`
   AND f.`业务日期` BETWEEN CAST(s.`生效起始日期` AS DATE)
                       AND COALESCE(CAST(s.`生效截止日期` AS DATE), DATE '9999-12-31')
)
SELECT
  `门店ID`, `门店名称`, `省份`, `城市`, `城市层级`, `店型`, `门店类型`, `商圈`,
  `品牌线`, `直营加盟类型`,
  CASE WHEN DATEDIFF(`业务日期`, `开业日期`) BETWEEN 0 AND 89 THEN TRUE ELSE FALSE END AS `是否90天内新店`,
  CASE WHEN DATEDIFF(`业务日期`, `开业日期`) BETWEEN 0 AND 89 THEN '新店' ELSE '非新店' END AS `新店标签`,
  `业务日期`, `订单数`, `销售额`, `原价销售额`, `折扣金额合计`, `商品件数合计`,
  `会员订单数`, `到店销售额`, `外卖销售额`, `到店订单数`, `外卖订单数`, `去重会员数`,
  `门店版本ID`, CAST('${as_of_date}' AS DATE) AS `数据快照日期`
FROM matched
WHERE scd_rn = 1
```


### 节点8
- Id: id_1779348711909
- Name: 派生标志字段
- Type: CALCULATOR
- **Sources (Inputs):**
  - id_1779348711908 (筛选已完成订单)

- **Used By (Outputs):**
  - id_1779348711910 (门店日聚合(主))
- Position: (635,64)
- FormulaNames:
  - 订单计数
  - 会员订单计数
  - 到店销售
  - 外卖销售
  - 到店订单计数
  - 外卖订单计数
- 等价SQL:
```sql
SELECT
  *,
  1 AS `订单计数`,
  case when `会员ID` is not null and `会员ID` <> '' then 1 else 0 end AS `会员订单计数`,
  case when `是否到店` = 1 then `实付金额` else 0 end AS `到店销售`,
  case when `是否到店` = 0 then `实付金额` else 0 end AS `外卖销售`,
  case when `是否到店` = 1 then 1 else 0 end AS `到店订单计数`,
  case when `是否到店` = 0 then 1 else 0 end AS `外卖订单计数`
FROM input1
```


### 节点9
- Id: id_1779348711914
- Name: 计算占比和均值
- Type: CALCULATOR
- **Sources (Inputs):**
  - id_1779348711913 (关联门店信息)

- **Used By (Outputs):**
  - id_1779348711915 (dws_门店日报)
- Position: (1451,64)
- FormulaNames:
  - 平均客单价
  - 会员订单占比
  - 折扣率
  - 到店占比
  - 外卖占比
  - 平均件数
- 等价SQL:
```sql
SELECT
  `门店ID`, `门店名称`, `省份`, `城市`, `城市层级`, `店型`, `门店类型`, `商圈`,
  `品牌线`, `直营加盟类型`, `是否90天内新店`, `新店标签`,
  `业务日期`, `订单数`, `销售额`, `原价销售额`, `折扣金额合计`, `商品件数合计`,
  `会员订单数`, `到店销售额`, `外卖销售额`, `到店订单数`, `外卖订单数`, `去重会员数`,
  case when `订单数` > 0 then `销售额` / `订单数` else 0 end AS `平均客单价`,
  case when `订单数` > 0 then `会员订单数` * 1.0 / `订单数` else 0 end AS `会员订单占比`,
  case when `原价销售额` > 0 then `折扣金额合计` / `原价销售额` else 0 end AS `折扣率`,
  case when `订单数` > 0 then `到店订单数` * 1.0 / `订单数` else 0 end AS `到店占比`,
  case when `订单数` > 0 then `外卖订单数` * 1.0 / `订单数` else 0 end AS `外卖占比`,
  case when `订单数` > 0 then `商品件数合计` * 1.0 / `订单数` else 0 end AS `平均件数`,
  `门店版本ID`, `数据快照日期`
FROM input1
```


### 节点10
- Id: id_1779348711915
- Name: dws_门店日报
- Type: OUTPUT_DATASET
- **Sources (Inputs):**
  - id_1779348711914 (计算占比和均值)
- Position: (1655,64)
- OutputDsName: dws_门店日报
- ParentDirId: v2b6bde3d41444cfd9e6d7ef
- ParentDirName: 马甲的demo-0523
- DataSourceDsId: l335bc476e2f343ed8c721bd
- DataSourceCreated: true
- DirPath: 根目录 > 马甲的demo-0523
- 等价SQL:
```sql
SELECT * FROM input1
```


---

## 血缘关系

### 上游资源 (2)
- **dwd_订单** (DATA_SET_FILE)
  - ID: j23ea7e60564e47458b71d82
- **dim_门店主档** (DATA_SET_FILE)
  - ID: sedfdd84abacc4cb496c15e7

### 下游资源 (1)
- **dws_门店日报** (DATA_SET_ETL)
  - ID: l335bc476e2f343ed8c721bd
