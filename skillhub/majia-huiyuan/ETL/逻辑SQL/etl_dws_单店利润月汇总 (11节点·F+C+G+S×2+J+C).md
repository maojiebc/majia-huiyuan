你是一个ETL专家, 正在查看如下ETL的定义(注意: 这里的所有节点都会运行在 Apache Spark 3.4上, 所有的SQL语法都是Spark的语法, 当用户让优化性能时, 不要给出建索引等通用的建议, 因为Spark不能建立索引, 并且这个ETL只能使用Spark SQL(不能使用DataFrame API), 主要给出可以"通过优化ETL节点的写法来优化性能"这种优化建议):

## 基本信息
- UniformResourceType: DATA_PROCESS_ETL
---
## ETL 流程摘要

- **总节点数:** 11
- **节点类型分布:**
  - CALCULATOR: 2
  - FILTER_ROWS: 1
  - GROUP_BY: 1
  - INPUT_DATASET: 3
  - JOIN_DATA: 1
  - OUTPUT_DATASET: 1
  - SQL_SCRIPT: 2
- **数据输入源:**
  - j23ea7e60564e47458b71d82 (dwd_订单)
  - sedfdd84abacc4cb496c15e7 (dim_门店主档)
  - ff7b4cae808ca4ecab894f53 (dwd_门店成本明细)
- **数据输出目标:**
  - dws_单店利润月汇总 (目录: 马甲的demo-0523)
---
## ETL 节点详细信息


### 节点1
- Id: id_1779345679920
- Name: 筛选已完成订单
- Type: FILTER_ROWS
- **Sources (Inputs):**
  - id_1779345679919 (dwd_订单)

- **Used By (Outputs):**
  - id_1779345679921 (派生月份+营收分流)
- Position: (431,64)
- 等价SQL:
```sql
WITH params AS (
  SELECT DATE '2026-06-24' AS as_of_date -- 生产由调度参数替换
)
SELECT
  input1.*
FROM input1
CROSS JOIN params p
WHERE `订单状态` = '已完成'
  AND `业务日期` <= p.as_of_date
```


### 节点2
- Id: id_1779345679921
- Name: 派生月份+营收分流
- Type: CALCULATOR
- **Sources (Inputs):**
  - id_1779345679920 (筛选已完成订单)

- **Used By (Outputs):**
  - id_1779345679922 (门店月营收聚合)
- Position: (635,64)
- FormulaNames:
  - 月份
  - 堂食营收
  - 外卖营收
  - 订单计数
- 等价SQL:
```sql
SELECT
  *,
  substr(`业务日期`, 1, 7) AS `月份`,
  case when `是否到店` = 1 then `实付金额` else 0 end AS `堂食营收`,
  case when `是否到店` = 0 then `实付金额` else 0 end AS `外卖营收`,
  1 AS `订单计数`
FROM input1
```


### 节点3
- Id: id_1779345679919
- Name: dwd_订单
- Type: INPUT_DATASET
- **Used By (Outputs):**
  - id_1779345679920 (筛选已完成订单)
- Position: (227,64)
- InputDsId: j23ea7e60564e47458b71d82
- DisplayType: CSV
- PreviewScope: ALL
- 等价SQL:
```sql
SELECT * FROM input
```


### 节点4
- Id: id_1779345679928
- Name: PnL 四层 + 占比指标
- Type: CALCULATOR
- **Sources (Inputs):**
  - id_1779345679927 (关联门店维度)

- **Used By (Outputs):**
  - id_1779345679929 (dws_单店利润月汇总)
- Position: (1451,64)
- FormulaNames:
  - 毛利
  - 店面贡献利润
  - 单店净利润
  - 毛利率
  - 店面贡献利润率
  - 单店净利率
  - 堂食占比
  - 外卖占比
  - 人工占比
  - 房租占比
  - 客单价
- 等价SQL:
```sql
SELECT
  `门店ID`, `门店名称`, `省份`, `城市`, `城市层级`, `门店类型`, `商圈`,
  `品牌线`, `直营加盟类型`, `是否90天内新店`, `新店标签`, `开业日期`,
  `月份`, `月营收`, `堂食营收`, `外卖营收`, `订单数`,
  `原材料成本`, `包材成本`, `平台抽佣`, `人工成本`, `房租物业`,
  `能耗水电`, `设备折旧`, `总部分摊`, `变动成本合计`,
  `半固定成本合计`, `固定成本合计`, `成本总计`,
  `月营收` - `变动成本合计` AS `毛利`,
  `月营收` - `变动成本合计` - `半固定成本合计` - `房租物业` AS `店面贡献利润`,
  `月营收` - `成本总计` AS `单店净利润`,
  case when `月营收` > 0 then (`月营收` - `变动成本合计`) / `月营收` else 0 end AS `毛利率`,
  case when `月营收` > 0 then (`月营收` - `变动成本合计` - `半固定成本合计` - `房租物业`) / `月营收` else 0 end AS `店面贡献利润率`,
  case when `月营收` > 0 then (`月营收` - `成本总计`) / `月营收` else 0 end AS `单店净利率`,
  case when `月营收` > 0 then `堂食营收` / `月营收` else 0 end AS `堂食占比`,
  case when `月营收` > 0 then `外卖营收` / `月营收` else 0 end AS `外卖占比`,
  case when `月营收` > 0 then `人工成本` / `月营收` else 0 end AS `人工占比`,
  case when `月营收` > 0 then `房租物业` / `月营收` else 0 end AS `房租占比`,
  case when `订单数` > 0 then `月营收` / `订单数` else 0 end AS `客单价`,
  `数据快照日期`
FROM input1
```


### 节点5
- Id: id_1779345679922
- Name: 门店月营收聚合
- Type: GROUP_BY
- **Sources (Inputs):**
  - id_1779345679921 (派生月份+营收分流)

- **Used By (Outputs):**
  - id_1779345679925 (营业门店月份骨架+营收成本)
- Position: (839,64)
- 等价SQL:
```sql
SELECT
  `门店ID`,
  `月份`,
  SUM(`实付金额`) AS `实付金额`,
  SUM(`堂食营收`) AS `堂食营收`,
  SUM(`外卖营收`) AS `外卖营收`,
  SUM(`订单计数`) AS `订单计数`
FROM input1
GROUP BY `门店ID`, `月份`
```


### 节点6
- Id: id_1779345679926
- Name: dim_门店主档
- Type: INPUT_DATASET
- **Used By (Outputs):**
  - id_1779345679927 (关联门店维度)
  - id_1779345679925 (营业门店月份骨架+营收成本)
- Position: (1043,232)
- InputDsId: sedfdd84abacc4cb496c15e7
- DisplayType: EXCEL
- PreviewScope: ALL
- 等价SQL:
```sql
SELECT * FROM input
```


### 节点7
- Id: id_1779345679923
- Name: dwd_门店成本明细
- Type: INPUT_DATASET
- **Used By (Outputs):**
  - id_1779345679924 (成本透视(8 大科目))
- Position: (635,232)
- InputDsId: ff7b4cae808ca4ecab894f53
- DisplayType: CSV
- PreviewScope: ALL
- 等价SQL:
```sql
SELECT * FROM input
```


### 节点8
- Id: id_1779345679929
- Name: dws_单店利润月汇总
- Type: OUTPUT_DATASET
- **Sources (Inputs):**
  - id_1779345679928 (PnL 四层 + 占比指标)
- Position: (1655,64)
- OutputDsName: dws_单店利润月汇总
- ParentDirId: v2b6bde3d41444cfd9e6d7ef
- ParentDirName: 马甲的demo-0523
- DataSourceDsId: l6ee75fc812be413583215e4
- DataSourceCreated: true
- DirPath: 根目录 > 马甲的demo-0523
- 等价SQL:
```sql
SELECT * FROM input1
```


### 节点9
- Id: id_1779345679927
- Name: 关联门店维度
- Type: JOIN_DATA
- **Sources (Inputs):**
  - id_1779345679925 (营业门店月份骨架+营收成本)
  - id_1779345679926 (dim_门店主档)

- **Used By (Outputs):**
  - id_1779345679928 (PnL 四层 + 占比指标)
- Position: (1247,64)
- 等价SQL:
```sql
WITH matched AS (
  SELECT
    p.`门店ID`,
    s.`门店名称`, s.`省份`, s.`城市`, s.`城市层级`, s.`门店类型`, s.`商圈`,
    s.`品牌线`, s.`直营加盟类型`,
    CASE WHEN DATEDIFF(p.`维度命中日期`, s.`开业日期`) BETWEEN 0 AND 89
         THEN 'TRUE' ELSE 'FALSE' END AS `是否90天内新店`,
    CASE WHEN DATEDIFF(p.`维度命中日期`, s.`开业日期`) BETWEEN 0 AND 89
         THEN '90天新店' ELSE '成熟店' END AS `新店标签`,
    s.`开业日期`,
    p.`月份`, p.`月营收`, p.`堂食营收`, p.`外卖营收`, p.`订单数`,
    p.`原材料成本`, p.`包材成本`, p.`平台抽佣`, p.`人工成本`, p.`房租物业`,
    p.`能耗水电`, p.`设备折旧`, p.`总部分摊`, p.`变动成本合计`,
    p.`半固定成本合计`, p.`固定成本合计`, p.`成本总计`, p.`数据快照日期`,
    ROW_NUMBER() OVER (
      PARTITION BY p.`门店ID`, p.`月份`
      ORDER BY s.`生效起始日期` DESC, s.`门店版本ID` DESC
    ) AS scd_rn
  FROM input1 p
  LEFT JOIN input2 s
    ON p.`门店ID` = s.`门店ID`
   AND p.`维度命中日期` >= s.`生效起始日期`
   AND p.`维度命中日期` <= COALESCE(s.`生效截止日期`, DATE '9999-12-31')
)
SELECT
  `门店ID`, `门店名称`, `省份`, `城市`, `城市层级`, `门店类型`, `商圈`,
  `品牌线`, `直营加盟类型`, `是否90天内新店`, `新店标签`, `开业日期`,
  `月份`, `月营收`, `堂食营收`, `外卖营收`, `订单数`,
  `原材料成本`, `包材成本`, `平台抽佣`, `人工成本`, `房租物业`,
  `能耗水电`, `设备折旧`, `总部分摊`, `变动成本合计`,
  `半固定成本合计`, `固定成本合计`, `成本总计`, `数据快照日期`
FROM matched
WHERE scd_rn = 1
```


### 节点10
- Id: id_1779345679924
- Name: 成本透视(8 大科目)
- Type: SQL_SCRIPT
- **Sources (Inputs):**
  - id_1779345679923 (dwd_门店成本明细)

- **Used By (Outputs):**
  - id_1779345679925 (营业门店月份骨架+营收成本)
- Position: (839,232)
- SqlScript:
```sql
WITH params AS (
  SELECT DATE '2026-06-24' AS as_of_date -- 生产由调度参数替换
)
SELECT
  `门店ID`,
  `月份`,
  SUM(CASE WHEN `成本科目ID` = 'CST_RAW'  THEN `成本金额` ELSE 0 END) AS `原材料成本`,
  SUM(CASE WHEN `成本科目ID` = 'CST_PKG'  THEN `成本金额` ELSE 0 END) AS `包材成本`,
  SUM(CASE WHEN `成本科目ID` = 'CST_PLAT' THEN `成本金额` ELSE 0 END) AS `平台抽佣`,
  SUM(CASE WHEN `成本科目ID` = 'CST_LBR'  THEN `成本金额` ELSE 0 END) AS `人工成本`,
  SUM(CASE WHEN `成本科目ID` = 'CST_RENT' THEN `成本金额` ELSE 0 END) AS `房租物业`,
  SUM(CASE WHEN `成本科目ID` = 'CST_UTL'  THEN `成本金额` ELSE 0 END) AS `能耗水电`,
  SUM(CASE WHEN `成本科目ID` = 'CST_DEP'  THEN `成本金额` ELSE 0 END) AS `设备折旧`,
  SUM(CASE WHEN `成本科目ID` = 'CST_HQ'   THEN `成本金额` ELSE 0 END) AS `总部分摊`,
  SUM(CASE WHEN `成本大类` = '变动成本'   THEN `成本金额` ELSE 0 END) AS `变动成本合计`,
  SUM(CASE WHEN `成本大类` = '半固定成本' THEN `成本金额` ELSE 0 END) AS `半固定成本合计`,
  SUM(CASE WHEN `成本大类` = '固定成本'   THEN `成本金额` ELSE 0 END) AS `固定成本合计`,
  SUM(`成本金额`) AS `成本总计`
FROM input1
CROSS JOIN params p
WHERE TRUNC(CAST(`月份` AS DATE), 'MM') <= p.as_of_date
GROUP BY `门店ID`, `月份`
```
- 等价SQL:
```sql
WITH params AS (
  SELECT DATE '2026-06-24' AS as_of_date -- 生产由调度参数替换
)
SELECT
  `门店ID`,
  `月份`,
  SUM(CASE WHEN `成本科目ID` = 'CST_RAW'  THEN `成本金额` ELSE 0 END) AS `原材料成本`,
  SUM(CASE WHEN `成本科目ID` = 'CST_PKG'  THEN `成本金额` ELSE 0 END) AS `包材成本`,
  SUM(CASE WHEN `成本科目ID` = 'CST_PLAT' THEN `成本金额` ELSE 0 END) AS `平台抽佣`,
  SUM(CASE WHEN `成本科目ID` = 'CST_LBR'  THEN `成本金额` ELSE 0 END) AS `人工成本`,
  SUM(CASE WHEN `成本科目ID` = 'CST_RENT' THEN `成本金额` ELSE 0 END) AS `房租物业`,
  SUM(CASE WHEN `成本科目ID` = 'CST_UTL'  THEN `成本金额` ELSE 0 END) AS `能耗水电`,
  SUM(CASE WHEN `成本科目ID` = 'CST_DEP'  THEN `成本金额` ELSE 0 END) AS `设备折旧`,
  SUM(CASE WHEN `成本科目ID` = 'CST_HQ'   THEN `成本金额` ELSE 0 END) AS `总部分摊`,
  SUM(CASE WHEN `成本大类` = '变动成本'   THEN `成本金额` ELSE 0 END) AS `变动成本合计`,
  SUM(CASE WHEN `成本大类` = '半固定成本' THEN `成本金额` ELSE 0 END) AS `半固定成本合计`,
  SUM(CASE WHEN `成本大类` = '固定成本'   THEN `成本金额` ELSE 0 END) AS `固定成本合计`,
  SUM(`成本金额`) AS `成本总计`
FROM input1
CROSS JOIN params p
WHERE TRUNC(CAST(`月份` AS DATE), 'MM') <= p.as_of_date
GROUP BY `门店ID`, `月份`
```


### 节点11
- Id: id_1779345679925
- Name: 营业门店月份骨架+营收成本
- Type: SQL_SCRIPT
- **Sources (Inputs):**
  - id_1779345679922 (门店月营收聚合)
  - id_1779345679924 (成本透视(8 大科目))
  - id_1779345679926 (dim_门店主档)

- **Used By (Outputs):**
  - id_1779345679927 (关联门店维度)
- Position: (1043,64)
- SqlScript:
```sql
WITH params AS (
  SELECT DATE '2026-06-24' AS `as_of_date` -- 生产由调度参数替换
),
store_current AS (
  -- 当前版本只用于取得每家门店的开闭店边界；历史属性在下一节点按月份命中 SCD2。
  SELECT
    s.`门店ID`, s.`开业日期`,
    CASE WHEN s.`闭店日期` IS NULL OR TRIM(s.`闭店日期`) = '' OR LOWER(TRIM(s.`闭店日期`)) = 'null'
         THEN NULL ELSE TO_DATE(s.`闭店日期`) END AS `闭店日期`
  FROM input3 s
  WHERE s.`当前版本标记` = 1
),
store_months AS (
  SELECT
    s.`门店ID`,
    EXPLODE(SEQUENCE(
      TRUNC(s.`开业日期`, 'MM'),
      TRUNC(LEAST(p.`as_of_date`, COALESCE(s.`闭店日期`, p.`as_of_date`)), 'MM'),
      INTERVAL 1 MONTH
    )) AS `月份日期`,
    p.`as_of_date` AS `数据快照日期`
  FROM store_current s
  CROSS JOIN params p
  WHERE s.`开业日期` IS NOT NULL
    AND s.`开业日期` <= p.`as_of_date`
    AND COALESCE(s.`闭店日期`, p.`as_of_date`) >= s.`开业日期`
),
revenue AS (
  SELECT
    `门店ID`, TO_DATE(CONCAT(SUBSTR(CAST(`月份` AS STRING), 1, 7), '-01')) AS `月份日期`,
    `实付金额`, `堂食营收`, `外卖营收`, `订单计数`
  FROM input1
),
cost AS (
  SELECT
    `门店ID`, TRUNC(CAST(`月份` AS DATE), 'MM') AS `月份日期`,
    `原材料成本`, `包材成本`, `平台抽佣`, `人工成本`, `房租物业`,
    `能耗水电`, `设备折旧`, `总部分摊`, `变动成本合计`,
    `半固定成本合计`, `固定成本合计`, `成本总计`
  FROM input2
)
SELECT
  b.`门店ID`, DATE_FORMAT(b.`月份日期`, 'yyyy-MM') AS `月份`,
  LEAST(LAST_DAY(b.`月份日期`), b.`数据快照日期`) AS `维度命中日期`,
  COALESCE(r.`实付金额`, 0) AS `月营收`,
  COALESCE(r.`堂食营收`, 0) AS `堂食营收`,
  COALESCE(r.`外卖营收`, 0) AS `外卖营收`,
  COALESCE(r.`订单计数`, 0) AS `订单数`,
  COALESCE(c.`原材料成本`, 0) AS `原材料成本`,
  COALESCE(c.`包材成本`, 0)   AS `包材成本`,
  COALESCE(c.`平台抽佣`, 0)   AS `平台抽佣`,
  COALESCE(c.`人工成本`, 0)   AS `人工成本`,
  COALESCE(c.`房租物业`, 0)   AS `房租物业`,
  COALESCE(c.`能耗水电`, 0)   AS `能耗水电`,
  COALESCE(c.`设备折旧`, 0)   AS `设备折旧`,
  COALESCE(c.`总部分摊`, 0)   AS `总部分摊`,
  COALESCE(c.`变动成本合计`, 0) AS `变动成本合计`,
  COALESCE(c.`半固定成本合计`, 0) AS `半固定成本合计`,
  COALESCE(c.`固定成本合计`, 0)   AS `固定成本合计`,
  COALESCE(c.`成本总计`, 0) AS `成本总计`,
  b.`数据快照日期`
FROM store_months b
LEFT JOIN revenue r
  ON b.`门店ID` = r.`门店ID` AND b.`月份日期` = r.`月份日期`
LEFT JOIN cost c
  ON b.`门店ID` = c.`门店ID` AND b.`月份日期` = c.`月份日期`
```
- 等价SQL:
```sql
WITH params AS (
  SELECT DATE '2026-06-24' AS `as_of_date` -- 生产由调度参数替换
),
store_current AS (
  -- 当前版本只用于取得每家门店的开闭店边界；历史属性在下一节点按月份命中 SCD2。
  SELECT
    s.`门店ID`, s.`开业日期`,
    CASE WHEN s.`闭店日期` IS NULL OR TRIM(s.`闭店日期`) = '' OR LOWER(TRIM(s.`闭店日期`)) = 'null'
         THEN NULL ELSE TO_DATE(s.`闭店日期`) END AS `闭店日期`
  FROM input3 s
  WHERE s.`当前版本标记` = 1
),
store_months AS (
  SELECT
    s.`门店ID`,
    EXPLODE(SEQUENCE(
      TRUNC(s.`开业日期`, 'MM'),
      TRUNC(LEAST(p.`as_of_date`, COALESCE(s.`闭店日期`, p.`as_of_date`)), 'MM'),
      INTERVAL 1 MONTH
    )) AS `月份日期`,
    p.`as_of_date` AS `数据快照日期`
  FROM store_current s
  CROSS JOIN params p
  WHERE s.`开业日期` IS NOT NULL
    AND s.`开业日期` <= p.`as_of_date`
    AND COALESCE(s.`闭店日期`, p.`as_of_date`) >= s.`开业日期`
),
revenue AS (
  SELECT
    `门店ID`, TO_DATE(CONCAT(SUBSTR(CAST(`月份` AS STRING), 1, 7), '-01')) AS `月份日期`,
    `实付金额`, `堂食营收`, `外卖营收`, `订单计数`
  FROM input1
),
cost AS (
  SELECT
    `门店ID`, TRUNC(CAST(`月份` AS DATE), 'MM') AS `月份日期`,
    `原材料成本`, `包材成本`, `平台抽佣`, `人工成本`, `房租物业`,
    `能耗水电`, `设备折旧`, `总部分摊`, `变动成本合计`,
    `半固定成本合计`, `固定成本合计`, `成本总计`
  FROM input2
)
SELECT
  b.`门店ID`, DATE_FORMAT(b.`月份日期`, 'yyyy-MM') AS `月份`,
  LEAST(LAST_DAY(b.`月份日期`), b.`数据快照日期`) AS `维度命中日期`,
  COALESCE(r.`实付金额`, 0) AS `月营收`,
  COALESCE(r.`堂食营收`, 0) AS `堂食营收`,
  COALESCE(r.`外卖营收`, 0) AS `外卖营收`,
  COALESCE(r.`订单计数`, 0) AS `订单数`,
  COALESCE(c.`原材料成本`, 0) AS `原材料成本`,
  COALESCE(c.`包材成本`, 0)   AS `包材成本`,
  COALESCE(c.`平台抽佣`, 0)   AS `平台抽佣`,
  COALESCE(c.`人工成本`, 0)   AS `人工成本`,
  COALESCE(c.`房租物业`, 0)   AS `房租物业`,
  COALESCE(c.`能耗水电`, 0)   AS `能耗水电`,
  COALESCE(c.`设备折旧`, 0)   AS `设备折旧`,
  COALESCE(c.`总部分摊`, 0)   AS `总部分摊`,
  COALESCE(c.`变动成本合计`, 0) AS `变动成本合计`,
  COALESCE(c.`半固定成本合计`, 0) AS `半固定成本合计`,
  COALESCE(c.`固定成本合计`, 0)   AS `固定成本合计`,
  COALESCE(c.`成本总计`, 0) AS `成本总计`,
  b.`数据快照日期`
FROM store_months b
LEFT JOIN revenue r
  ON b.`门店ID` = r.`门店ID` AND b.`月份日期` = r.`月份日期`
LEFT JOIN cost c
  ON b.`门店ID` = c.`门店ID` AND b.`月份日期` = c.`月份日期`
```


---

## 血缘关系

### 上游资源 (3)
- **dwd_门店成本明细** (DATA_SET_FILE)
  - ID: ff7b4cae808ca4ecab894f53
- **dwd_订单** (DATA_SET_FILE)
  - ID: j23ea7e60564e47458b71d82
- **dim_门店主档** (DATA_SET_FILE)
  - ID: sedfdd84abacc4cb496c15e7

### 下游资源 (1)
- **dws_单店利润月汇总** (DATA_SET_ETL)
  - ID: l6ee75fc812be413583215e4
