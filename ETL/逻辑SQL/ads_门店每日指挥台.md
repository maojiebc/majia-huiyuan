你是一个ETL专家, 正在查看如下ETL的定义(注意: 这里的所有节点都会运行在 Apache Spark 3.4上, 所有的SQL语法都是Spark的语法, 当用户让优化性能时, 不要给出建索引等通用的建议, 因为Spark不能建立索引, 并且这个ETL只能使用Spark SQL(不能使用DataFrame API), 主要给出可以"通过优化ETL节点的写法来优化性能"这种优化建议):

## 基本信息
- UniformResourceType: DATA_PROCESS_ETL
---
## ETL 流程摘要

- **总节点数:** 5
- **节点类型分布:**
  - INPUT_DATASET: 3
  - OUTPUT_DATASET: 1
  - SQL_SCRIPT: 1
- **数据输入源:**
  - sedfdd84abacc4cb496c15e7 (dim_门店主档)
  - af8234caa4e90486793eaab8 (dwd_评价)
  - j23ea7e60564e47458b71d82 (dwd_订单)
- **数据输出目标:**
  - ads_门店每日指挥台 (目录: 马甲的demo-0523)
---
## ETL 节点详细信息


### 节点1
- Id: id_1779326818730
- Name: dim_门店主档
- Type: INPUT_DATASET
- **Used By (Outputs):**
  - id_1779326818732 (营业日历骨架+日指标+自然日基线)
- Position: (200,250)
- InputDsId: sedfdd84abacc4cb496c15e7
- DisplayType: CSV
- PreviewScope: ALL
- 等价SQL:
```sql
SELECT * FROM input
```


### 节点2
- Id: id_1779326818731
- Name: dwd_评价
- Type: INPUT_DATASET
- **Used By (Outputs):**
  - id_1779326818732 (营业日历骨架+日指标+自然日基线)
- Position: (200,400)
- InputDsId: af8234caa4e90486793eaab8
- DisplayType: CSV
- PreviewScope: ALL
- 等价SQL:
```sql
SELECT * FROM input
```


### 节点3
- Id: id_1779326818733
- Name: ads_门店每日指挥台
- Type: OUTPUT_DATASET
- **Sources (Inputs):**
  - id_1779326818732 (营业日历骨架+日指标+自然日基线)
- Position: (800,100)
- OutputDsName: ads_门店每日指挥台
- ParentDirId: v2b6bde3d41444cfd9e6d7ef
- ParentDirName: 马甲的demo-0523
- DataSourceDsId: nd177a0ac0eda44ac98c75bc
- DataSourceCreated: true
- DirPath: 根目录 > 马甲的demo-0523
- 等价SQL:
```sql
SELECT * FROM input1
```


### 节点4
- Id: id_1779326818732
- Name: 营业日历骨架+日指标+自然日基线
- Type: SQL_SCRIPT
- **Sources (Inputs):**
  - id_1779326818729 (dwd_订单)
  - id_1779326818730 (dim_门店主档)
  - id_1779326818731 (dwd_评价)

- **Used By (Outputs):**
  - id_1779326818733 (ads_门店每日指挥台)
- Position: (500,100)
- SqlScript:
```sql
WITH params AS (
  SELECT DATE '2026-06-24' AS `as_of_date` -- 生产由调度参数替换
),
store_current AS (
  -- 当前版本只负责提供每家门店唯一的开闭店边界。
  SELECT
    s.`门店ID`, s.`开业日期`,
    CASE WHEN s.`闭店日期` IS NULL OR TRIM(s.`闭店日期`) = '' OR LOWER(TRIM(s.`闭店日期`)) = 'null'
         THEN NULL ELSE TO_DATE(s.`闭店日期`) END AS `闭店日期`
  FROM input2 s
  WHERE s.`当前版本标记` = 1
),
store_bounds AS (
  SELECT s.`门店ID`, s.`开业日期`, COALESCE(s.`闭店日期`, p.`as_of_date`) AS `营业截止日期`, p.`as_of_date`
  FROM store_current s
  CROSS JOIN params p
  WHERE s.`开业日期` IS NOT NULL
    AND s.`开业日期` <= p.`as_of_date`
    AND COALESCE(s.`闭店日期`, p.`as_of_date`) >= s.`开业日期`
),
store_calendar AS (
  -- 门店×自然营业日期骨架：即使当天 0 单也会有一行。
  SELECT s.`门店ID`, d.`业务日期`, s.`as_of_date` AS `数据快照日期`
  FROM store_bounds s
  LATERAL VIEW EXPLODE(SEQUENCE(
    s.`开业日期`,
    LEAST(s.`as_of_date`, s.`营业截止日期`),
    INTERVAL 1 DAY
  )) d AS `业务日期`
),
order_daily AS (
  SELECT
    o.`门店ID`, o.`业务日期`,
    COUNT(DISTINCT o.`订单ID`) AS `订单数`,
    SUM(o.`实付金额`) AS `销售额`,
    COUNT(DISTINCT CASE WHEN o.`会员ID` IS NOT NULL AND o.`会员ID` <> '' THEN o.`订单ID` END) AS `会员订单数`,
    COUNT(DISTINCT CASE WHEN o.`是否到店` = 1 THEN o.`订单ID` END) AS `到店订单数`,
    COUNT(DISTINCT CASE WHEN o.`是否到店` = 0 THEN o.`订单ID` END) AS `外卖订单数`,
    SUM(o.`折扣金额`) AS `折扣总额`,
    SUM(o.`原价金额`) AS `原价总额`
  FROM input1 o
  CROSS JOIN params p
  WHERE o.`订单状态` = '已完成' AND o.`业务日期` <= p.`as_of_date`
  GROUP BY o.`门店ID`, o.`业务日期`
),
review_daily AS (
  SELECT
    r.`门店ID`, r.`评价日期` AS `业务日期`,
    AVG(r.`评分`) AS `当日评分`,
    COUNT(*) AS `当日评价数`,
    SUM(CASE WHEN r.`评分` <= 2 AND r.`回复状态` = '未回复' THEN 1 ELSE 0 END) AS `未回复负评数`
  FROM input3 r
  CROSS JOIN params p
  WHERE r.`评价日期` <= p.`as_of_date`
  GROUP BY r.`门店ID`, r.`评价日期`
),
joined AS (
  SELECT
    b.`门店ID`, s.`门店名称`, s.`省份`, s.`城市`, s.`城市层级`, s.`店型`, s.`门店类型`, s.`商圈`,
    CASE WHEN DATEDIFF(b.`业务日期`, s.`开业日期`) BETWEEN 0 AND 89 THEN 'TRUE' ELSE 'FALSE' END AS `是否90天内新店`,
    CASE WHEN DATEDIFF(b.`业务日期`, s.`开业日期`) BETWEEN 0 AND 89 THEN '90天新店' ELSE '成熟店' END AS `新店标签`,
    b.`业务日期`, COALESCE(o.`订单数`, 0) AS `订单数`, COALESCE(o.`销售额`, 0) AS `销售额`,
    CASE WHEN COALESCE(o.`订单数`, 0) > 0 THEN o.`销售额` / o.`订单数` ELSE NULL END AS `平均客单价_基线口径`,
    COALESCE(o.`会员订单数`, 0) AS `会员订单数`, COALESCE(o.`到店订单数`, 0) AS `到店订单数`,
    COALESCE(o.`外卖订单数`, 0) AS `外卖订单数`,
    CASE WHEN COALESCE(o.`订单数`, 0) > 0 THEN o.`会员订单数` * 1.0 / o.`订单数` ELSE 0 END AS `会员订单占比`,
    CASE WHEN COALESCE(o.`订单数`, 0) > 0 THEN o.`到店订单数` * 1.0 / o.`订单数` ELSE 0 END AS `到店占比`,
    CASE WHEN COALESCE(o.`原价总额`, 0) > 0 THEN o.`折扣总额` / o.`原价总额` ELSE 0 END AS `折扣率`,
    COALESCE(r.`当日评分`, 5) AS `当日评分`, COALESCE(r.`当日评价数`, 0) AS `当日评价数`,
    COALESCE(r.`未回复负评数`, 0) AS `未回复负评数`,
    COALESCE(o.`折扣总额`, 0) AS `折扣总额`, COALESCE(o.`原价总额`, 0) AS `原价总额`,
    b.`数据快照日期`
  FROM store_calendar b
  LEFT JOIN input2 s
    ON b.`门店ID` = s.`门店ID`
   AND b.`业务日期` >= s.`生效起始日期`
   AND b.`业务日期` <= COALESCE(s.`生效截止日期`, DATE '9999-12-31')
  LEFT JOIN order_daily o ON b.`门店ID` = o.`门店ID` AND b.`业务日期` = o.`业务日期`
  LEFT JOIN review_daily r ON b.`门店ID` = r.`门店ID` AND b.`业务日期` = r.`业务日期`
),
with_baseline AS (
  SELECT *,
    -- 因骨架每天一行，ROWS 14 PRECEDING 就是前 14 个自然日；AVG 自动忽略零订单日的 NULL 客单价。
    AVG(`平均客单价_基线口径`) OVER (
      PARTITION BY `门店ID` ORDER BY `业务日期` ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING
    ) AS `客单价基线`
  FROM joined
)
SELECT
  `门店ID`, `门店名称`, `省份`, `城市`, `城市层级`, `店型`, `门店类型`,
  `商圈`, `是否90天内新店`, `新店标签`,
  `业务日期`, `订单数`, `销售额`, COALESCE(`平均客单价_基线口径`, 0) AS `平均客单价`,
  `会员订单数`, `到店订单数`, `外卖订单数`, `会员订单占比`, `到店占比`, `折扣率`,
  `当日评分`, `未回复负评数`,
  CASE
    WHEN `订单数` = 0 THEN '客流异常' -- 覆盖闭店、断数、POS 未上传等最严重场景
    WHEN `订单数` < 5 THEN '客流异常'
    WHEN `未回复负评数` >= 1 THEN '口碑异常'
    WHEN `当日评价数` >= 2 AND `当日评分` <= 3.0 THEN '评分滑坡'
    WHEN `原价总额` > 0 AND `折扣总额` / `原价总额` > 0.30 THEN '折扣过高'
    WHEN `订单数` >= 10 AND `会员订单数` * 1.0 / `订单数` < 0.10 THEN '会员占比异常'
    WHEN `订单数` >= 10 AND `客单价基线` IS NOT NULL
         AND `平均客单价_基线口径` < 0.4 * `客单价基线` THEN '客单价异常'
    ELSE '正常'
  END AS `今日异常`,
  `数据快照日期`
FROM with_baseline

```
- 等价SQL:
```sql
WITH params AS (
  SELECT DATE '2026-06-24' AS `as_of_date` -- 生产由调度参数替换
),
store_current AS (
  -- 当前版本只负责提供每家门店唯一的开闭店边界。
  SELECT
    s.`门店ID`, s.`开业日期`,
    CASE WHEN s.`闭店日期` IS NULL OR TRIM(s.`闭店日期`) = '' OR LOWER(TRIM(s.`闭店日期`)) = 'null'
         THEN NULL ELSE TO_DATE(s.`闭店日期`) END AS `闭店日期`
  FROM input2 s
  WHERE s.`当前版本标记` = 1
),
store_bounds AS (
  SELECT s.`门店ID`, s.`开业日期`, COALESCE(s.`闭店日期`, p.`as_of_date`) AS `营业截止日期`, p.`as_of_date`
  FROM store_current s
  CROSS JOIN params p
  WHERE s.`开业日期` IS NOT NULL
    AND s.`开业日期` <= p.`as_of_date`
    AND COALESCE(s.`闭店日期`, p.`as_of_date`) >= s.`开业日期`
),
store_calendar AS (
  -- 门店×自然营业日期骨架：即使当天 0 单也会有一行。
  SELECT s.`门店ID`, d.`业务日期`, s.`as_of_date` AS `数据快照日期`
  FROM store_bounds s
  LATERAL VIEW EXPLODE(SEQUENCE(
    s.`开业日期`,
    LEAST(s.`as_of_date`, s.`营业截止日期`),
    INTERVAL 1 DAY
  )) d AS `业务日期`
),
order_daily AS (
  SELECT
    o.`门店ID`, o.`业务日期`,
    COUNT(DISTINCT o.`订单ID`) AS `订单数`,
    SUM(o.`实付金额`) AS `销售额`,
    COUNT(DISTINCT CASE WHEN o.`会员ID` IS NOT NULL AND o.`会员ID` <> '' THEN o.`订单ID` END) AS `会员订单数`,
    COUNT(DISTINCT CASE WHEN o.`是否到店` = 1 THEN o.`订单ID` END) AS `到店订单数`,
    COUNT(DISTINCT CASE WHEN o.`是否到店` = 0 THEN o.`订单ID` END) AS `外卖订单数`,
    SUM(o.`折扣金额`) AS `折扣总额`,
    SUM(o.`原价金额`) AS `原价总额`
  FROM input1 o
  CROSS JOIN params p
  WHERE o.`订单状态` = '已完成' AND o.`业务日期` <= p.`as_of_date`
  GROUP BY o.`门店ID`, o.`业务日期`
),
review_daily AS (
  SELECT
    r.`门店ID`, r.`评价日期` AS `业务日期`,
    AVG(r.`评分`) AS `当日评分`,
    COUNT(*) AS `当日评价数`,
    SUM(CASE WHEN r.`评分` <= 2 AND r.`回复状态` = '未回复' THEN 1 ELSE 0 END) AS `未回复负评数`
  FROM input3 r
  CROSS JOIN params p
  WHERE r.`评价日期` <= p.`as_of_date`
  GROUP BY r.`门店ID`, r.`评价日期`
),
joined AS (
  SELECT
    b.`门店ID`, s.`门店名称`, s.`省份`, s.`城市`, s.`城市层级`, s.`店型`, s.`门店类型`, s.`商圈`,
    CASE WHEN DATEDIFF(b.`业务日期`, s.`开业日期`) BETWEEN 0 AND 89 THEN 'TRUE' ELSE 'FALSE' END AS `是否90天内新店`,
    CASE WHEN DATEDIFF(b.`业务日期`, s.`开业日期`) BETWEEN 0 AND 89 THEN '90天新店' ELSE '成熟店' END AS `新店标签`,
    b.`业务日期`, COALESCE(o.`订单数`, 0) AS `订单数`, COALESCE(o.`销售额`, 0) AS `销售额`,
    CASE WHEN COALESCE(o.`订单数`, 0) > 0 THEN o.`销售额` / o.`订单数` ELSE NULL END AS `平均客单价_基线口径`,
    COALESCE(o.`会员订单数`, 0) AS `会员订单数`, COALESCE(o.`到店订单数`, 0) AS `到店订单数`,
    COALESCE(o.`外卖订单数`, 0) AS `外卖订单数`,
    CASE WHEN COALESCE(o.`订单数`, 0) > 0 THEN o.`会员订单数` * 1.0 / o.`订单数` ELSE 0 END AS `会员订单占比`,
    CASE WHEN COALESCE(o.`订单数`, 0) > 0 THEN o.`到店订单数` * 1.0 / o.`订单数` ELSE 0 END AS `到店占比`,
    CASE WHEN COALESCE(o.`原价总额`, 0) > 0 THEN o.`折扣总额` / o.`原价总额` ELSE 0 END AS `折扣率`,
    COALESCE(r.`当日评分`, 5) AS `当日评分`, COALESCE(r.`当日评价数`, 0) AS `当日评价数`,
    COALESCE(r.`未回复负评数`, 0) AS `未回复负评数`,
    COALESCE(o.`折扣总额`, 0) AS `折扣总额`, COALESCE(o.`原价总额`, 0) AS `原价总额`,
    b.`数据快照日期`
  FROM store_calendar b
  LEFT JOIN input2 s
    ON b.`门店ID` = s.`门店ID`
   AND b.`业务日期` >= s.`生效起始日期`
   AND b.`业务日期` <= COALESCE(s.`生效截止日期`, DATE '9999-12-31')
  LEFT JOIN order_daily o ON b.`门店ID` = o.`门店ID` AND b.`业务日期` = o.`业务日期`
  LEFT JOIN review_daily r ON b.`门店ID` = r.`门店ID` AND b.`业务日期` = r.`业务日期`
),
with_baseline AS (
  SELECT *,
    -- 因骨架每天一行，ROWS 14 PRECEDING 就是前 14 个自然日；AVG 自动忽略零订单日的 NULL 客单价。
    AVG(`平均客单价_基线口径`) OVER (
      PARTITION BY `门店ID` ORDER BY `业务日期` ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING
    ) AS `客单价基线`
  FROM joined
)
SELECT
  `门店ID`, `门店名称`, `省份`, `城市`, `城市层级`, `店型`, `门店类型`,
  `商圈`, `是否90天内新店`, `新店标签`,
  `业务日期`, `订单数`, `销售额`, COALESCE(`平均客单价_基线口径`, 0) AS `平均客单价`,
  `会员订单数`, `到店订单数`, `外卖订单数`, `会员订单占比`, `到店占比`, `折扣率`,
  `当日评分`, `未回复负评数`,
  CASE
    WHEN `订单数` = 0 THEN '客流异常' -- 覆盖闭店、断数、POS 未上传等最严重场景
    WHEN `订单数` < 5 THEN '客流异常'
    WHEN `未回复负评数` >= 1 THEN '口碑异常'
    WHEN `当日评价数` >= 2 AND `当日评分` <= 3.0 THEN '评分滑坡'
    WHEN `原价总额` > 0 AND `折扣总额` / `原价总额` > 0.30 THEN '折扣过高'
    WHEN `订单数` >= 10 AND `会员订单数` * 1.0 / `订单数` < 0.10 THEN '会员占比异常'
    WHEN `订单数` >= 10 AND `客单价基线` IS NOT NULL
         AND `平均客单价_基线口径` < 0.4 * `客单价基线` THEN '客单价异常'
    ELSE '正常'
  END AS `今日异常`,
  `数据快照日期`
FROM with_baseline

```


### 节点5
- Id: id_1779326818729
- Name: dwd_订单
- Type: INPUT_DATASET
- **Used By (Outputs):**
  - id_1779326818732 (营业日历骨架+日指标+自然日基线)
- Position: (200,100)
- InputDsId: j23ea7e60564e47458b71d82
- DisplayType: CSV
- PreviewScope: ALL
- 等价SQL:
```sql
SELECT * FROM input
```


---

## 血缘关系

### 上游资源 (3)
- **dwd_订单** (DATA_SET_FILE)
  - ID: j23ea7e60564e47458b71d82
- **dim_门店主档** (DATA_SET_FILE)
  - ID: sedfdd84abacc4cb496c15e7
- **dwd_评价** (DATA_SET_FILE)
  - ID: af8234caa4e90486793eaab8

### 下游资源 (1)
- **ads_门店每日指挥台** (DATA_SET_ETL)
  - ID: nd177a0ac0eda44ac98c75bc
