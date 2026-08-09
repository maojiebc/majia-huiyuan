你是一个 ETL 专家，正在查看如下 ETL 的定义。所有节点运行在 Apache Spark 3.4，SQL 只能使用 Spark SQL。

## 基本信息

- UniformResourceType: DATA_PROCESS_ETL
- 版本口径: v1.4.1

## ETL 流程摘要

- **数据输入源:**
  - `dwd_订单`
  - `dwd_会员触达`
- **数据输出目标:** `ads_高层经营驾驶舱`
- **运行参数:** `as_of_date`，必填，格式 `yyyy-MM-dd`；调度器在运行前替换 `${as_of_date}`。
- **归因窗口:** 触达时间起（含）至触达时间后 8×24 小时（不含），即滚动 0–7 天。

## v1.4.1 业务口径

1. 总销售、会员销售、到店销售和关联销售全部按订单发生日汇总，分子分母处于同一天。
2. 私域订单只允许归属于下单前最近一次有效触达；用 `ROW_NUMBER` 将同一订单的归因优先级固定为 1，杜绝重复归因。
3. 归因桥保留 `订单ID、触达ID、触达时间、下单时间、归因规则、归因优先级`，便于 DQC 逐单审计。
4. 没有对照组时，只能称“触达后关联销售”，不能称“私域贡献销售”或增量收入。
5. 原字段“到店订单占比”实际按销售额计算，v1.4.1 更名为“到店销售额占比”。

## 核心 SQL

输入顺序：`input1 = dwd_订单`，`input2 = dwd_会员触达`。

```sql
WITH params AS (
  SELECT
    CAST('${as_of_date}' AS DATE) AS `as_of_date`,
    7 AS `归因窗口天数`
),
valid_order AS (
  SELECT o.*
  FROM input1 o
  CROSS JOIN params p
  WHERE o.`订单状态` = '已完成'
    AND o.`业务日期` <= p.`as_of_date`
),
valid_touch AS (
  SELECT t.*
  FROM input2 t
  CROSS JOIN params p
  WHERE t.`触达状态` = '已发送'
    AND t.`会员ID` IS NOT NULL AND t.`会员ID` <> ''
    AND t.`触达日期` <= p.`as_of_date`
),
order_daily AS (
  SELECT
    o.`业务日期`,
    SUM(o.`实付金额`) AS `总销售`,
    SUM(CASE
      WHEN o.`会员ID` IS NOT NULL AND o.`会员ID` <> '' THEN o.`实付金额`
      ELSE 0
    END) AS `会员销售`,
    SUM(CASE WHEN o.`是否到店` = 1 THEN o.`实付金额` ELSE 0 END) AS `到店销售`,
    COUNT(DISTINCT o.`订单ID`) AS `总订单数`,
    COUNT(DISTINCT o.`会员ID`) AS `活跃会员数`,
    COUNT(DISTINCT CASE WHEN o.`是否会员首单` = 1 THEN o.`会员ID` END) AS `新增会员数`
  FROM valid_order o
  GROUP BY o.`业务日期`
),
touch_order_candidates AS (
  SELECT
    o.`订单ID`,
    t.`触达ID`,
    t.`触达时间`,
    o.`下单时间`,
    o.`业务日期` AS `订单发生日期`,
    o.`实付金额`,
    '下单前最近一次有效触达' AS `归因规则`,
    ROW_NUMBER() OVER (
      PARTITION BY o.`订单ID`
      ORDER BY t.`触达时间` DESC, t.`触达ID` DESC
    ) AS `归因优先级`
  FROM valid_order o
  JOIN valid_touch t
    ON o.`会员ID` = t.`会员ID`
   AND o.`会员ID` IS NOT NULL AND o.`会员ID` <> ''
   AND o.`下单时间` >= t.`触达时间`
   AND o.`下单时间` < t.`触达时间` + INTERVAL 8 DAYS
),
touch_order_bridge AS (
  SELECT
    `订单ID`, `触达ID`, `触达时间`, `下单时间`, `订单发生日期`,
    `实付金额`, `归因规则`, `归因优先级`
  FROM touch_order_candidates
  WHERE `归因优先级` = 1
),
touch_order_daily AS (
  SELECT
    b.`订单发生日期` AS `业务日期`,
    COUNT(DISTINCT b.`订单ID`) AS `触达后关联订单数`,
    SUM(b.`实付金额`) AS `触达后关联销售`
  FROM touch_order_bridge b
  GROUP BY b.`订单发生日期`
)
SELECT
  d.`业务日期`,
  d.`总销售`,
  d.`会员销售`,
  d.`到店销售`,
  d.`总订单数`,
  d.`活跃会员数`,
  d.`新增会员数`,
  COALESCE(a.`触达后关联订单数`, 0) AS `触达后关联订单数`,
  COALESCE(a.`触达后关联销售`, 0.0) AS `触达后关联销售`,
  CASE WHEN d.`总销售` > 0 THEN d.`会员销售` / d.`总销售` ELSE 0 END AS `会员销售占比`,
  CASE WHEN d.`总销售` > 0 THEN d.`到店销售` / d.`总销售` ELSE 0 END AS `到店销售额占比`,
  CASE
    WHEN d.`总销售` > 0 THEN COALESCE(a.`触达后关联销售`, 0.0) / d.`总销售`
    ELSE 0
  END AS `触达后关联销售占比`,
  p.`归因窗口天数`,
  '下单前最近一次有效触达；每订单唯一' AS `归因规则`,
  p.`as_of_date` AS `数据快照日期`
FROM order_daily d
LEFT JOIN touch_order_daily a
  ON d.`业务日期` = a.`业务日期`
CROSS JOIN params p
```

## 验收约束

- `touch_order_bridge` 中 `订单ID` 必须唯一。
- 每日 `触达后关联订单数 <= 总订单数`。
- 每日 `触达后关联销售 <= 总销售`，允许因退款冲销或负金额数据触发例外并进入 DQC 人工核验。
- `会员销售占比`、`到店销售额占比`、`触达后关联销售占比` 在正常非负订单口径下均应处于 `[0, 1]`。
- 所有业务日期不得晚于 `as_of_date`。

## 血缘关系

- 上游：`dwd_订单`、`dwd_会员触达`
- 下游：`ads_高层经营驾驶舱`
