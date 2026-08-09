你是一个 ETL 专家，正在查看如下 ETL 的定义。所有节点运行在 Apache Spark 3.4，SQL 只能使用 Spark SQL。

## 基本信息

- UniformResourceType: DATA_PROCESS_ETL
- 版本口径: v1.4.1
- 历史文件名保留原节点描述；v1.4.1 将核心归因集中为可审计的 SQL 节点。

## ETL 流程摘要

- **数据输入源:**
  - `dwd_会员触达`
  - `dwd_订单`
  - `dim_活动主档`
- **数据输出目标:** `dws_私域转化漏斗`
- **运行参数:** `as_of_date`，必填，格式 `yyyy-MM-dd`；调度器在运行前替换 `${as_of_date}`。
- **归因窗口:** 触达时间起（含）至触达时间后 8×24 小时（不含），即滚动 0–7 天。

## v1.4.1 业务口径

1. 只有 `触达状态 = '已发送'`、会员与活动均非空、且不晚于 `as_of_date` 的记录才是有效触达。
2. 订单必须为已完成、会员非空、且订单业务日期不晚于 `as_of_date`。
3. 候选触达必须发生在下单前；同一订单命中多次触达时，只保留下单前最近一次有效触达，并以 `触达ID` 作为稳定并列裁决键。
4. 归因桥在明细层保留 `订单ID、触达ID、触达时间、下单时间、归因规则、归因优先级`。`归因优先级 = 1` 才能进入汇总，因此一笔订单最多归属一次触达。
5. 触达与查看指标按触达发生日汇总；关联订单指标按订单发生日汇总。`业务日期` 始终是指标实际发生日，不能把未来 7 天订单塞回触达日。
6. 本表没有对照组，只输出“触达后关联”结果，不把关联 GMV 称为贡献、增量或 ROI。由于顶部漏斗和订单指标采用各自事件发生日，本表不再输出会错配分子分母的“整体转化率/查看转化率”。

## 核心 SQL

输入顺序：`input1 = dwd_会员触达`，`input2 = dwd_订单`，`input3 = dim_活动主档`。

```sql
WITH params AS (
  SELECT
    CAST('${as_of_date}' AS DATE) AS `as_of_date`,
    7 AS `归因窗口天数`
),
valid_touch AS (
  SELECT
    t.`触达ID`,
    t.`触达时间`,
    t.`触达日期`,
    t.`会员ID`,
    t.`活动ID`,
    t.`触达渠道`,
    t.`是否查看`
  FROM input1 t
  CROSS JOIN params p
  WHERE t.`触达状态` = '已发送'
    AND t.`会员ID` IS NOT NULL AND t.`会员ID` <> ''
    AND t.`活动ID` IS NOT NULL AND t.`活动ID` <> ''
    AND t.`触达日期` <= p.`as_of_date`
),
valid_order AS (
  SELECT
    o.`订单ID`,
    o.`会员ID`,
    o.`下单时间`,
    o.`业务日期`,
    o.`实付金额`
  FROM input2 o
  CROSS JOIN params p
  WHERE o.`订单状态` = '已完成'
    AND o.`会员ID` IS NOT NULL AND o.`会员ID` <> ''
    AND o.`业务日期` <= p.`as_of_date`
),
touch_daily AS (
  SELECT
    t.`活动ID`,
    t.`触达日期` AS `业务日期`,
    t.`触达渠道`,
    COUNT(*) AS `触达人次`,
    SUM(CASE WHEN t.`是否查看` = 1 THEN 1 ELSE 0 END) AS `查看人次`,
    COUNT(DISTINCT t.`会员ID`) AS `触达人数`
  FROM valid_touch t
  GROUP BY t.`活动ID`, t.`触达日期`, t.`触达渠道`
),
touch_order_candidates AS (
  SELECT
    o.`订单ID`,
    t.`触达ID`,
    t.`触达时间`,
    o.`下单时间`,
    o.`业务日期` AS `订单发生日期`,
    o.`会员ID`,
    o.`实付金额`,
    t.`活动ID`,
    t.`触达渠道`,
    '下单前最近一次有效触达' AS `归因规则`,
    ROW_NUMBER() OVER (
      PARTITION BY o.`订单ID`
      ORDER BY t.`触达时间` DESC, t.`触达ID` DESC
    ) AS `归因优先级`
  FROM valid_order o
  JOIN valid_touch t
    ON o.`会员ID` = t.`会员ID`
   AND o.`下单时间` >= t.`触达时间`
   AND o.`下单时间` < t.`触达时间` + INTERVAL 8 DAYS
),
touch_order_bridge AS (
  SELECT
    `订单ID`, `触达ID`, `触达时间`, `下单时间`, `订单发生日期`,
    `会员ID`, `实付金额`, `活动ID`, `触达渠道`, `归因规则`, `归因优先级`
  FROM touch_order_candidates
  WHERE `归因优先级` = 1
),
order_daily AS (
  SELECT
    b.`活动ID`,
    b.`订单发生日期` AS `业务日期`,
    b.`触达渠道`,
    COUNT(DISTINCT b.`会员ID`) AS `触达后关联下单人数`,
    COUNT(DISTINCT b.`订单ID`) AS `触达后关联订单数`,
    SUM(b.`实付金额`) AS `触达后关联GMV`
  FROM touch_order_bridge b
  GROUP BY b.`活动ID`, b.`订单发生日期`, b.`触达渠道`
),
date_keys AS (
  SELECT `活动ID`, `业务日期`, `触达渠道` FROM touch_daily
  UNION
  SELECT `活动ID`, `业务日期`, `触达渠道` FROM order_daily
)
SELECT
  k.`活动ID`,
  a.`活动名称`,
  a.`活动类型`,
  k.`业务日期`,
  k.`触达渠道`,
  COALESCE(t.`触达人次`, 0) AS `触达人次`,
  COALESCE(t.`查看人次`, 0) AS `查看人次`,
  COALESCE(t.`触达人数`, 0) AS `触达人数`,
  CASE
    WHEN COALESCE(t.`触达人次`, 0) > 0
      THEN t.`查看人次` * 1.0 / t.`触达人次`
    ELSE 0
  END AS `打开率`,
  COALESCE(o.`触达后关联下单人数`, 0) AS `触达后关联下单人数`,
  COALESCE(o.`触达后关联订单数`, 0) AS `触达后关联订单数`,
  COALESCE(o.`触达后关联GMV`, 0.0) AS `触达后关联GMV`,
  p.`归因窗口天数`,
  '下单前最近一次有效触达；每订单唯一' AS `归因规则`,
  p.`as_of_date` AS `数据快照日期`
FROM date_keys k
LEFT JOIN touch_daily t
  ON k.`活动ID` = t.`活动ID`
 AND k.`业务日期` = t.`业务日期`
 AND k.`触达渠道` = t.`触达渠道`
LEFT JOIN order_daily o
  ON k.`活动ID` = o.`活动ID`
 AND k.`业务日期` = o.`业务日期`
 AND k.`触达渠道` = o.`触达渠道`
LEFT JOIN input3 a
  ON k.`活动ID` = a.`活动ID`
CROSS JOIN params p
```

## 验收约束

- `touch_order_bridge` 中 `订单ID` 必须唯一。
- `触达后关联GMV` 按订单发生日期汇总，同口径下不得大于同期符合条件的已完成会员订单 GMV。
- `触达后关联下单人数 <= 触达后关联订单数`。
- 所有 `业务日期` 与 `数据快照日期` 均不得晚于 `as_of_date`。

## 血缘关系

- 上游：`dwd_会员触达`、`dwd_订单`、`dim_活动主档`
- 下游：`dws_私域转化漏斗`
