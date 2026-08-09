你是一个 ETL 专家，正在查看如下 ETL 的定义。所有节点运行在 Apache Spark 3.4，SQL 只能使用 Spark SQL。

## 基本信息

- UniformResourceType: DATA_PROCESS_ETL
- 版本口径: v1.4.1
- 历史文件名保留原节点描述；v1.4.1 将核心归因集中为可审计的 SQL 节点。

## ETL 流程摘要

- **数据输入源:**
  - `dwd_券事件`
  - `dwd_会员触达`
  - `dwd_订单`
  - `dim_活动主档`
  - `dwd_活动参与`
- **数据输出目标:** `ads_活动权益复盘`
- **运行参数:** `as_of_date`，必填，格式 `yyyy-MM-dd`；调度器在运行前替换 `${as_of_date}`。
- **归因窗口:** 触达/参与时间起（含）至事件时间后 8×24 小时（不含），即滚动 0–7 天。

## v1.4.1 业务口径

1. 券链按 `券ID -> 订单ID` 建立券实例—核销订单桥，不再按发放日、核销日或订单日做同日连接。只有核销日期位于发放日至失效日内的券才是有效核销；同一订单异常命中多张券时，只保留优惠金额最大的券实例。
2. 触达链先筛选有效触达，再为每笔已完成订单选择下单前最近一次触达；触达晚于下单的记录不能归因。
3. 活动参与链以成功的 `参与ID` 为归因事件，为每笔订单选择下单前最近一次成功参与。
4. 触达归因与活动参与归因是两个不同口径：同一订单在每个口径内最多命中一次，不把两条链的 GMV 相加成“总贡献”。
5. 指标分层展示：`核销订单GMV` 是券实例直连结果；`触达后关联GMV` / `参与后关联GMV` 是有限窗口关联结果；只有对照实验或准实验才能产生 `增量GMV`。
6. 当前数据没有对照组标记，也没有完整的触达、渠道等成本，因此 `增量GMV`、`增量ROI` 必须为 `NULL`。`核销GMV成本比` 只是描述性比值，不叫 ROI。

## 核心 SQL

输入顺序：`input1 = dwd_券事件`，`input2 = dwd_会员触达`，`input3 = dwd_订单`，`input4 = dim_活动主档`，`input5 = dwd_活动参与`。

```sql
WITH params AS (
  SELECT
    CAST('${as_of_date}' AS DATE) AS `as_of_date`,
    7 AS `归因窗口天数`
),
valid_order AS (
  SELECT o.*
  FROM input3 o
  CROSS JOIN params p
  WHERE o.`订单状态` = '已完成'
    AND o.`业务日期` <= p.`as_of_date`
),
coupon_instance_ranked AS (
  SELECT
    c.*,
    ROW_NUMBER() OVER (
      PARTITION BY c.`券ID`
      ORDER BY COALESCE(c.`核销日期`, c.`发放日期`) DESC,
               COALESCE(c.`订单ID`, '') DESC
    ) AS `券实例版本优先级`
  FROM input1 c
  CROSS JOIN params p
  WHERE c.`券ID` IS NOT NULL AND c.`券ID` <> ''
    AND c.`来源活动ID` IS NOT NULL AND c.`来源活动ID` <> ''
    AND c.`发放日期` <= p.`as_of_date`
),
coupon_instance AS (
  SELECT *
  FROM coupon_instance_ranked
  WHERE `券实例版本优先级` = 1
),
coupon_activity_agg AS (
  SELECT
    c.`来源活动ID` AS `活动ID`,
    COUNT(DISTINCT c.`券ID`) AS `券发放数`,
    COUNT(DISTINCT CASE
      WHEN c.`核销日期` BETWEEN c.`发放日期`
                              AND COALESCE(c.`失效日期`, DATE '9999-12-31')
       AND c.`核销日期` <= p.`as_of_date` THEN c.`券ID`
    END) AS `券核销数`,
    SUM(CASE
      WHEN c.`核销日期` BETWEEN c.`发放日期`
                              AND COALESCE(c.`失效日期`, DATE '9999-12-31')
       AND c.`核销日期` <= p.`as_of_date`
        THEN COALESCE(c.`折扣金额`, 0.0)
      ELSE 0.0
    END) AS `已记录权益成本`
  FROM coupon_instance c
  CROSS JOIN params p
  GROUP BY c.`来源活动ID`
),
coupon_order_candidates AS (
  SELECT
    c.`券ID`,
    c.`来源活动ID` AS `活动ID`,
    o.`订单ID`,
    o.`业务日期` AS `订单发生日期`,
    o.`实付金额`,
    '券实例订单ID直连；多券同单取优惠金额最大，券ID并列裁决' AS `归因规则`,
    ROW_NUMBER() OVER (
      PARTITION BY o.`订单ID`
      ORDER BY COALESCE(c.`折扣金额`, 0.0) DESC, c.`券ID` ASC
    ) AS `归因优先级`
  FROM coupon_instance c
  CROSS JOIN params p
  JOIN valid_order o
    ON c.`订单ID` = o.`订单ID`
   AND c.`会员ID` <=> o.`会员ID`
  WHERE c.`核销日期` BETWEEN c.`发放日期`
                          AND COALESCE(c.`失效日期`, DATE '9999-12-31')
    AND c.`核销日期` <= p.`as_of_date`
),
coupon_order_bridge AS (
  SELECT
    `券ID`, `活动ID`, `订单ID`, `订单发生日期`, `实付金额`, `归因规则`, `归因优先级`
  FROM coupon_order_candidates
  WHERE `归因优先级` = 1
),
coupon_order_agg AS (
  SELECT
    b.`活动ID`,
    COUNT(DISTINCT b.`订单ID`) AS `核销订单数`,
    SUM(b.`实付金额`) AS `核销订单GMV`
  FROM coupon_order_bridge b
  GROUP BY b.`活动ID`
),
valid_touch AS (
  SELECT
    t.`触达ID`, t.`触达时间`, t.`会员ID`, t.`活动ID`, t.`是否查看`
  FROM input2 t
  CROSS JOIN params p
  WHERE t.`触达状态` = '已发送'
    AND t.`会员ID` IS NOT NULL AND t.`会员ID` <> ''
    AND t.`活动ID` IS NOT NULL AND t.`活动ID` <> ''
    AND t.`触达日期` <= p.`as_of_date`
),
touch_agg AS (
  SELECT
    t.`活动ID`,
    COUNT(DISTINCT t.`会员ID`) AS `触达人数`,
    COUNT(DISTINCT CASE WHEN t.`是否查看` = 1 THEN t.`会员ID` END) AS `查看人数`
  FROM valid_touch t
  GROUP BY t.`活动ID`
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
    `会员ID`, `实付金额`, `活动ID`, `归因规则`, `归因优先级`
  FROM touch_order_candidates
  WHERE `归因优先级` = 1
),
touch_order_agg AS (
  SELECT
    b.`活动ID`,
    COUNT(DISTINCT b.`会员ID`) AS `触达后关联下单人数`,
    COUNT(DISTINCT b.`订单ID`) AS `触达后关联订单数`,
    SUM(b.`实付金额`) AS `触达后关联GMV`
  FROM touch_order_bridge b
  GROUP BY b.`活动ID`
),
valid_participation AS (
  SELECT
    a.`参与ID`, a.`参与时间`, a.`会员ID`, a.`活动ID`
  FROM input5 a
  CROSS JOIN params p
  WHERE a.`结果` = '成功'
    AND a.`会员ID` IS NOT NULL AND a.`会员ID` <> ''
    AND a.`活动ID` IS NOT NULL AND a.`活动ID` <> ''
    AND a.`参与日期` <= p.`as_of_date`
),
participation_agg AS (
  SELECT
    a.`活动ID`,
    COUNT(DISTINCT a.`会员ID`) AS `活动参与人数`
  FROM valid_participation a
  GROUP BY a.`活动ID`
),
participation_order_candidates AS (
  SELECT
    o.`订单ID`,
    a.`参与ID`,
    a.`参与时间`,
    o.`下单时间`,
    o.`业务日期` AS `订单发生日期`,
    o.`会员ID`,
    o.`实付金额`,
    a.`活动ID`,
    '下单前最近一次成功活动参与' AS `归因规则`,
    ROW_NUMBER() OVER (
      PARTITION BY o.`订单ID`
      ORDER BY a.`参与时间` DESC, a.`参与ID` DESC
    ) AS `归因优先级`
  FROM valid_order o
  JOIN valid_participation a
    ON o.`会员ID` = a.`会员ID`
   AND o.`下单时间` >= a.`参与时间`
   AND o.`下单时间` < a.`参与时间` + INTERVAL 8 DAYS
),
participation_order_bridge AS (
  SELECT
    `订单ID`, `参与ID`, `参与时间`, `下单时间`, `订单发生日期`,
    `会员ID`, `实付金额`, `活动ID`, `归因规则`, `归因优先级`
  FROM participation_order_candidates
  WHERE `归因优先级` = 1
),
participation_order_agg AS (
  SELECT
    b.`活动ID`,
    COUNT(DISTINCT b.`会员ID`) AS `参与后关联下单人数`,
    COUNT(DISTINCT b.`订单ID`) AS `参与后关联订单数`,
    SUM(b.`实付金额`) AS `参与后关联GMV`
  FROM participation_order_bridge b
  GROUP BY b.`活动ID`
)
SELECT
  a.`活动ID`,
  a.`活动名称`,
  a.`活动类型`,
  a.`活动渠道`,
  a.`开始日期`,
  a.`结束日期`,
  a.`预算`,
  COALESCE(c.`券发放数`, 0) AS `券发放数`,
  COALESCE(c.`券核销数`, 0) AS `券核销数`,
  COALESCE(c.`已记录权益成本`, 0.0) AS `已记录权益成本`,
  COALESCE(co.`核销订单数`, 0) AS `核销订单数`,
  COALESCE(co.`核销订单GMV`, 0.0) AS `核销订单GMV`,
  COALESCE(t.`触达人数`, 0) AS `触达人数`,
  COALESCE(t.`查看人数`, 0) AS `查看人数`,
  COALESCE(ap.`活动参与人数`, 0) AS `活动参与人数`,
  COALESCE(ta.`触达后关联下单人数`, 0) AS `触达后关联下单人数`,
  COALESCE(ta.`触达后关联订单数`, 0) AS `触达后关联订单数`,
  COALESCE(ta.`触达后关联GMV`, 0.0) AS `触达后关联GMV`,
  COALESCE(pa.`参与后关联下单人数`, 0) AS `参与后关联下单人数`,
  COALESCE(pa.`参与后关联订单数`, 0) AS `参与后关联订单数`,
  COALESCE(pa.`参与后关联GMV`, 0.0) AS `参与后关联GMV`,
  CASE
    WHEN COALESCE(c.`券发放数`, 0) > 0 THEN c.`券核销数` * 1.0 / c.`券发放数`
    ELSE 0
  END AS `券核销率`,
  CASE
    WHEN COALESCE(t.`触达人数`, 0) > 0 THEN t.`查看人数` * 1.0 / t.`触达人数`
    ELSE 0
  END AS `打开率`,
  CASE
    WHEN COALESCE(t.`触达人数`, 0) > 0
      THEN COALESCE(ta.`触达后关联下单人数`, 0) * 1.0 / t.`触达人数`
    ELSE 0
  END AS `触达后关联下单率`,
  CASE
    WHEN COALESCE(ap.`活动参与人数`, 0) > 0
      THEN COALESCE(pa.`参与后关联下单人数`, 0) * 1.0 / ap.`活动参与人数`
    ELSE 0
  END AS `参与后关联下单率`,
  CASE
    WHEN COALESCE(c.`已记录权益成本`, 0.0) > 0
      THEN COALESCE(co.`核销订单GMV`, 0.0) / c.`已记录权益成本`
    ELSE NULL
  END AS `核销GMV成本比`,
  CAST(NULL AS DOUBLE) AS `增量GMV`,
  CAST(NULL AS DOUBLE) AS `增量ROI`,
  '未接入对照组及完整成本；关联GMV不等于增量GMV' AS `增量测算状态`,
  p.`归因窗口天数`,
  '下单前最近一次有效触达；每订单唯一' AS `触达归因规则`,
  '下单前最近一次成功活动参与；每订单唯一' AS `活动参与归因规则`,
  p.`as_of_date` AS `数据快照日期`
FROM input4 a
CROSS JOIN params p
LEFT JOIN coupon_activity_agg c ON a.`活动ID` = c.`活动ID`
LEFT JOIN coupon_order_agg co ON a.`活动ID` = co.`活动ID`
LEFT JOIN touch_agg t ON a.`活动ID` = t.`活动ID`
LEFT JOIN touch_order_agg ta ON a.`活动ID` = ta.`活动ID`
LEFT JOIN participation_agg ap ON a.`活动ID` = ap.`活动ID`
LEFT JOIN participation_order_agg pa ON a.`活动ID` = pa.`活动ID`
WHERE a.`开始日期` <= p.`as_of_date`
```

## 验收约束

- `coupon_order_bridge`、`touch_order_bridge`、`participation_order_bridge` 各自的 `订单ID` 必须唯一。
- 每条桥上的 `归因优先级` 必须恒等于 1，归因事件时间不得晚于下单时间。
- `券核销数 <= 券发放数`，且 `核销订单数 <= 券核销数`。
- `触达后关联下单人数 <= 触达人数`，`参与后关联下单人数 <= 活动参与人数`。
- `增量GMV`、`增量ROI` 在没有对照组时必须为 `NULL`。
- 所有事实日期不得晚于 `as_of_date`。

## 血缘关系

- 上游：`dwd_券事件`、`dwd_会员触达`、`dwd_订单`、`dim_活动主档`、`dwd_活动参与`
- 下游：`ads_活动权益复盘`
