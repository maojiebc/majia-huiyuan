你是一个 ETL 专家，正在查看如下 ETL 的定义。所有节点运行在 Apache Spark 3.4，SQL 只能使用 Spark SQL。

## 基本信息

- UniformResourceType: DATA_PROCESS_ETL
- 版本口径: v1.4.1

## ETL 流程摘要

- **数据输入源:**
  - `dwd_券事件`
  - `dwd_订单`
  - `dim_券模板`
- **数据输出目标:** `dws_券效益分析`
- **运行参数:** `as_of_date`，必填，格式 `yyyy-MM-dd`；调度器在运行前替换 `${as_of_date}`。
- **统计粒度:** `券模板ID × 发放日期`，即发券同期群口径。

## v1.4.1 业务口径

1. `券ID` 是券实例主键。先按 `券ID` 去重，再通过券实例上记录的 `订单ID` 连接已完成订单；严禁用“发放日期 = 核销日期/订单日期”连接。
2. 一张券只有在 `发放日期 <= 核销日期 <= 失效日期` 且 `核销日期 <= as_of_date` 时才计入有效核销。券实例缺少订单、订单未完成或订单晚于快照日时，可以计入有效核销数，但不能计入核销订单 GMV。
3. 若同一订单异常关联多张券实例，GMV 只归给优惠金额最大的券；并列时按 `券ID` 排序。归因桥保留 `券ID、订单ID、归因规则、归因优先级`，一笔订单在券 GMV 口径下最多出现一次。
4. `核销订单GMV / 已核销优惠成本` 只叫“核销GMV成本比”，它不扣自然消费，不是 ROI。
5. 当前上游没有对照组、对照标记和完整触达/渠道成本，因此 `增量GMV`、`增量ROI` 必须为 `NULL`，并输出未测算状态，不能用核销 GMV 冒充增量。

## 核心 SQL

输入顺序：`input1 = dwd_券事件`，`input2 = dwd_订单`，`input3 = dim_券模板`。

```sql
WITH params AS (
  SELECT CAST('${as_of_date}' AS DATE) AS `as_of_date`
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
    AND c.`发放日期` <= p.`as_of_date`
),
coupon_instance AS (
  SELECT *
  FROM coupon_instance_ranked
  WHERE `券实例版本优先级` = 1
),
valid_order AS (
  SELECT o.*
  FROM input2 o
  CROSS JOIN params p
  WHERE o.`订单状态` = '已完成'
    AND o.`业务日期` <= p.`as_of_date`
),
issue_agg AS (
  SELECT
    c.`券模板ID`,
    c.`发放日期`,
    COUNT(DISTINCT c.`券ID`) AS `发放数`,
    COUNT(DISTINCT CASE
      WHEN c.`核销日期` BETWEEN c.`发放日期`
                              AND COALESCE(c.`失效日期`, DATE '9999-12-31')
       AND c.`核销日期` <= p.`as_of_date` THEN c.`券ID`
    END) AS `核销数`,
    SUM(CASE
      WHEN c.`核销日期` BETWEEN c.`发放日期`
                              AND COALESCE(c.`失效日期`, DATE '9999-12-31')
       AND c.`核销日期` <= p.`as_of_date`
        THEN COALESCE(c.`折扣金额`, 0.0)
      ELSE 0.0
    END) AS `已核销优惠成本`
  FROM coupon_instance c
  CROSS JOIN params p
  GROUP BY c.`券模板ID`, c.`发放日期`
),
coupon_order_candidates AS (
  SELECT
    c.`券ID`,
    c.`券模板ID`,
    c.`发放日期`,
    c.`核销日期`,
    c.`折扣金额`,
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
    `券ID`, `券模板ID`, `发放日期`, `核销日期`, `折扣金额`,
    `订单ID`, `订单发生日期`, `实付金额`, `归因规则`, `归因优先级`
  FROM coupon_order_candidates
  WHERE `归因优先级` = 1
),
redemption_order_agg AS (
  SELECT
    b.`券模板ID`,
    b.`发放日期`,
    COUNT(DISTINCT b.`订单ID`) AS `核销订单数`,
    SUM(b.`实付金额`) AS `核销订单GMV`
  FROM coupon_order_bridge b
  GROUP BY b.`券模板ID`, b.`发放日期`
)
SELECT
  i.`券模板ID`,
  t.`券名称`,
  t.`券类型`,
  t.`优惠形式`,
  i.`发放日期`,
  i.`发放数`,
  i.`核销数`,
  CASE WHEN i.`发放数` > 0 THEN i.`核销数` * 1.0 / i.`发放数` ELSE 0 END AS `核销率`,
  i.`已核销优惠成本`,
  COALESCE(r.`核销订单GMV`, 0.0) AS `核销订单GMV`,
  COALESCE(r.`核销订单数`, 0) AS `核销订单数`,
  CASE
    WHEN i.`已核销优惠成本` > 0
      THEN COALESCE(r.`核销订单GMV`, 0.0) / i.`已核销优惠成本`
    ELSE NULL
  END AS `核销GMV成本比`,
  CAST(NULL AS DOUBLE) AS `增量GMV`,
  CAST(NULL AS DOUBLE) AS `增量ROI`,
  '未接入对照组；核销订单GMV不等于增量GMV' AS `增量测算状态`,
  '券实例订单ID直连；多券同单只归一次' AS `订单归因规则`,
  p.`as_of_date` AS `数据快照日期`
FROM issue_agg i
LEFT JOIN redemption_order_agg r
  ON i.`券模板ID` = r.`券模板ID`
 AND i.`发放日期` = r.`发放日期`
LEFT JOIN input3 t
  ON i.`券模板ID` = t.`券模板ID`
CROSS JOIN params p
```

## 验收约束

- `coupon_instance` 中 `券ID` 必须唯一。
- `coupon_order_bridge` 中 `订单ID` 必须唯一，且每行必须能回溯到唯一 `券ID`。
- `核销数 <= 发放数`，`核销订单数 <= 核销数`；不满足时进入 DQC 核验券状态回滚或上游重复。
- `增量GMV`、`增量ROI` 在没有对照组时必须为 `NULL`，不得填 0 或用核销 GMV 替代。
- 发放日期、核销日期和订单发生日期均不得晚于 `as_of_date`。

## 血缘关系

- 上游：`dwd_券事件`、`dwd_订单`、`dim_券模板`
- 下游：`dws_券效益分析`
