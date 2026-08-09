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
  - af8234caa4e90486793eaab8 (dwd_评价)
  - l1b7c38276d9d483b9e1f712 (dwd_投诉)
  - sedfdd84abacc4cb496c15e7 (dim_门店主档)
- **数据输出目标:**
  - dws_体验口碑汇总 (目录: 马甲的demo-0523)
---
## ETL 节点详细信息


### 节点1
- Id: id_1779326818726
- Name: dwd_评价
- Type: INPUT_DATASET
- **Used By (Outputs):**
  - id_1779326818729 (评价投诉事件日全量合并+SCD2关联)
- Position: (200,100)
- InputDsId: af8234caa4e90486793eaab8
- DisplayType: EXCEL
- PreviewScope: ALL
- 等价SQL:
```sql
SELECT * FROM input
```


### 节点2
- Id: id_1779326818727
- Name: dwd_投诉
- Type: INPUT_DATASET
- **Used By (Outputs):**
  - id_1779326818729 (评价投诉事件日全量合并+SCD2关联)
- Position: (200,250)
- InputDsId: l1b7c38276d9d483b9e1f712
- DisplayType: EXCEL
- PreviewScope: ALL
- 等价SQL:
```sql
SELECT * FROM input
```


### 节点3
- Id: id_1779326818728
- Name: dim_门店主档
- Type: INPUT_DATASET
- **Used By (Outputs):**
  - id_1779326818729 (评价投诉事件日全量合并+SCD2关联)
- Position: (200,400)
- InputDsId: sedfdd84abacc4cb496c15e7
- DisplayType: EXCEL
- PreviewScope: ALL
- 等价SQL:
```sql
SELECT * FROM input
```


### 节点4
- Id: id_1779326818729
- Name: 评价投诉事件日全量合并+SCD2关联
- Type: SQL_SCRIPT
- **Sources (Inputs):**
  - id_1779326818726 (dwd_评价)
  - id_1779326818727 (dwd_投诉)
  - id_1779326818728 (dim_门店主档)

- **Used By (Outputs):**
  - id_1779326818730 (dws_体验口碑汇总)
- Position: (500,100)
- SqlScript:
```sql
WITH params AS (
  SELECT DATE '2026-06-24' AS `as_of_date` -- 生产由调度参数替换
),
review_agg AS (
  SELECT
    r.`门店ID`, r.`评价日期` AS `业务日期`,
    COUNT(DISTINCT r.`评价ID`) AS `评价数`,
    AVG(r.`评分`) AS `平均评分`,
    SUM(CASE WHEN r.`评分` <= 2 THEN 1 ELSE 0 END) AS `负评数`,
    SUM(CASE WHEN r.`评分` = 5 THEN 1 ELSE 0 END) AS `好评数`,
    SUM(CASE WHEN r.`评分` <= 2 AND r.`回复状态` = '未回复' THEN 1 ELSE 0 END) AS `未回复负评数`
  FROM input1 r
  CROSS JOIN params p
  WHERE r.`评价日期` <= p.`as_of_date`
  GROUP BY r.`门店ID`, r.`评价日期`
),
complain_agg AS (
  SELECT
    c.`门店ID`, DATE(c.`投诉时间`) AS `业务日期`,
    COUNT(DISTINCT c.`投诉ID`) AS `投诉数`,
    SUM(CASE WHEN c.`状态` = '待处理' THEN 1 ELSE 0 END) AS `待处理投诉`,
    AVG(c.`处理时长_小时`) AS `平均处理时长`
  FROM input2 c
  CROSS JOIN params p
  WHERE DATE(c.`投诉时间`) <= p.`as_of_date`
  GROUP BY c.`门店ID`, DATE(c.`投诉时间`)
),
event_day AS (
  -- 先做事件日 FULL OUTER JOIN，投诉-only / 评价-only 日期都保留。
  SELECT
    COALESCE(r.`门店ID`, c.`门店ID`) AS `门店ID`,
    COALESCE(r.`业务日期`, c.`业务日期`) AS `业务日期`,
    r.`评价数`, r.`平均评分`, r.`负评数`, r.`好评数`, r.`未回复负评数`,
    c.`投诉数`, c.`待处理投诉`, c.`平均处理时长`
  FROM review_agg r
  FULL OUTER JOIN complain_agg c
    ON r.`门店ID` = c.`门店ID` AND r.`业务日期` = c.`业务日期`
)
SELECT
  e.`门店ID`, s.`门店名称`, s.`省份`, s.`城市`, s.`城市层级`, s.`店型`, s.`门店类型`,
  e.`业务日期`,
  COALESCE(e.`评价数`, 0) AS `评价数`,
  e.`平均评分`,
  COALESCE(e.`负评数`, 0) AS `负评数`,
  COALESCE(e.`好评数`, 0) AS `好评数`,
  COALESCE(e.`未回复负评数`, 0) AS `未回复负评数`,
  CASE WHEN COALESCE(e.`评价数`, 0) > 0 THEN e.`负评数` * 1.0 / e.`评价数` ELSE 0 END AS `负评率`,
  COALESCE(e.`投诉数`, 0) AS `投诉数`,
  COALESCE(e.`待处理投诉`, 0) AS `待处理投诉`,
  e.`平均处理时长`,
  CASE
    WHEN COALESCE(e.`待处理投诉`, 0) > 0 OR COALESCE(e.`平均评分`, 5) < 4.0 THEN '高风险'
    WHEN COALESCE(e.`投诉数`, 0) > 0 OR COALESCE(e.`平均评分`, 5) < 4.5 THEN '中风险'
    ELSE '正常'
  END AS `体验风险等级`,
  p.`as_of_date` AS `数据快照日期`
FROM event_day e
LEFT JOIN input3 s
  ON e.`门店ID` = s.`门店ID`
 AND e.`业务日期` >= s.`生效起始日期`
 AND e.`业务日期` <= COALESCE(s.`生效截止日期`, DATE '9999-12-31')
CROSS JOIN params p
```
- 等价SQL:
```sql
WITH params AS (
  SELECT DATE '2026-06-24' AS `as_of_date` -- 生产由调度参数替换
),
review_agg AS (
  SELECT
    r.`门店ID`, r.`评价日期` AS `业务日期`,
    COUNT(DISTINCT r.`评价ID`) AS `评价数`,
    AVG(r.`评分`) AS `平均评分`,
    SUM(CASE WHEN r.`评分` <= 2 THEN 1 ELSE 0 END) AS `负评数`,
    SUM(CASE WHEN r.`评分` = 5 THEN 1 ELSE 0 END) AS `好评数`,
    SUM(CASE WHEN r.`评分` <= 2 AND r.`回复状态` = '未回复' THEN 1 ELSE 0 END) AS `未回复负评数`
  FROM input1 r
  CROSS JOIN params p
  WHERE r.`评价日期` <= p.`as_of_date`
  GROUP BY r.`门店ID`, r.`评价日期`
),
complain_agg AS (
  SELECT
    c.`门店ID`, DATE(c.`投诉时间`) AS `业务日期`,
    COUNT(DISTINCT c.`投诉ID`) AS `投诉数`,
    SUM(CASE WHEN c.`状态` = '待处理' THEN 1 ELSE 0 END) AS `待处理投诉`,
    AVG(c.`处理时长_小时`) AS `平均处理时长`
  FROM input2 c
  CROSS JOIN params p
  WHERE DATE(c.`投诉时间`) <= p.`as_of_date`
  GROUP BY c.`门店ID`, DATE(c.`投诉时间`)
),
event_day AS (
  -- 先做事件日 FULL OUTER JOIN，投诉-only / 评价-only 日期都保留。
  SELECT
    COALESCE(r.`门店ID`, c.`门店ID`) AS `门店ID`,
    COALESCE(r.`业务日期`, c.`业务日期`) AS `业务日期`,
    r.`评价数`, r.`平均评分`, r.`负评数`, r.`好评数`, r.`未回复负评数`,
    c.`投诉数`, c.`待处理投诉`, c.`平均处理时长`
  FROM review_agg r
  FULL OUTER JOIN complain_agg c
    ON r.`门店ID` = c.`门店ID` AND r.`业务日期` = c.`业务日期`
)
SELECT
  e.`门店ID`, s.`门店名称`, s.`省份`, s.`城市`, s.`城市层级`, s.`店型`, s.`门店类型`,
  e.`业务日期`,
  COALESCE(e.`评价数`, 0) AS `评价数`,
  e.`平均评分`,
  COALESCE(e.`负评数`, 0) AS `负评数`,
  COALESCE(e.`好评数`, 0) AS `好评数`,
  COALESCE(e.`未回复负评数`, 0) AS `未回复负评数`,
  CASE WHEN COALESCE(e.`评价数`, 0) > 0 THEN e.`负评数` * 1.0 / e.`评价数` ELSE 0 END AS `负评率`,
  COALESCE(e.`投诉数`, 0) AS `投诉数`,
  COALESCE(e.`待处理投诉`, 0) AS `待处理投诉`,
  e.`平均处理时长`,
  CASE
    WHEN COALESCE(e.`待处理投诉`, 0) > 0 OR COALESCE(e.`平均评分`, 5) < 4.0 THEN '高风险'
    WHEN COALESCE(e.`投诉数`, 0) > 0 OR COALESCE(e.`平均评分`, 5) < 4.5 THEN '中风险'
    ELSE '正常'
  END AS `体验风险等级`,
  p.`as_of_date` AS `数据快照日期`
FROM event_day e
LEFT JOIN input3 s
  ON e.`门店ID` = s.`门店ID`
 AND e.`业务日期` >= s.`生效起始日期`
 AND e.`业务日期` <= COALESCE(s.`生效截止日期`, DATE '9999-12-31')
CROSS JOIN params p
```


### 节点5
- Id: id_1779326818730
- Name: dws_体验口碑汇总
- Type: OUTPUT_DATASET
- **Sources (Inputs):**
  - id_1779326818729 (评价投诉事件日全量合并+SCD2关联)
- Position: (800,100)
- OutputDsName: dws_体验口碑汇总
- ParentDirId: v2b6bde3d41444cfd9e6d7ef
- ParentDirName: 马甲的demo-0523
- DataSourceDsId: g52a667122e214eefb542bf6
- DataSourceCreated: true
- DirPath: 根目录 > 马甲的demo-0523
- 等价SQL:
```sql
SELECT * FROM input1
```


---

## 血缘关系

### 上游资源 (3)
- **dim_门店主档** (DATA_SET_FILE)
  - ID: sedfdd84abacc4cb496c15e7
- **dwd_评价** (DATA_SET_FILE)
  - ID: af8234caa4e90486793eaab8
- **dwd_投诉** (DATA_SET_FILE)
  - ID: l1b7c38276d9d483b9e1f712

### 下游资源 (1)
- **dws_体验口碑汇总** (DATA_SET_ETL)
  - ID: g52a667122e214eefb542bf6
