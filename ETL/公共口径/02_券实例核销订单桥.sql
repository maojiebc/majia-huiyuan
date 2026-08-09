-- Spark 3.4；券效益必须按券实例ID/核销订单ID连接，不能按“发放日=核销日”拼接。
-- 输出粒度：一行 = 一笔使用券的已完成订单；每个订单最多保留一个券实例。
WITH params AS (
  SELECT DATE '2026-06-24' AS as_of_date
),
coupon_instance_ranked AS (
  SELECT
    c.*,
    ROW_NUMBER() OVER (
      PARTITION BY c.`券ID`
      ORDER BY COALESCE(c.`核销日期`, c.`发放日期`) DESC,
               COALESCE(c.`订单ID`, '') DESC
    ) AS instance_rn
  FROM `dwd_券事件` c
  CROSS JOIN params p
  WHERE c.`券ID` IS NOT NULL
    AND c.`发放日期` <= p.as_of_date
),
coupon_instance AS (
  SELECT *
  FROM coupon_instance_ranked
  WHERE instance_rn = 1
),
valid_order AS (
  SELECT o.*
  FROM `dwd_订单` o
  CROSS JOIN params p
  WHERE o.`订单状态` = '已完成'
    AND o.`业务日期` <= p.as_of_date
),
candidates AS (
  SELECT
    c.`券ID`, c.`券模板ID`, c.`来源活动ID`, c.`会员ID`,
    c.`发放日期`, c.`失效日期`, c.`核销日期`, c.`发放渠道`,
    o.`订单ID`, o.`门店ID`, o.`下单时间`, o.`业务日期` AS `订单日期`,
    o.`实付金额` AS `核销订单GMV`, c.`折扣金额` AS `优惠成本`,
    '券实例订单ID直连；多券同单取优惠金额最大，券ID并列裁决' AS `归因规则`,
    ROW_NUMBER() OVER (
      PARTITION BY o.`订单ID`
      ORDER BY COALESCE(c.`折扣金额`, 0.0) DESC, c.`券ID` ASC
    ) AS rn
  FROM coupon_instance c
  JOIN valid_order o
    ON c.`订单ID` = o.`订单ID`
   AND c.`会员ID` <=> o.`会员ID`
  CROSS JOIN params p
  WHERE c.`核销日期` IS NOT NULL
    AND c.`核销日期` BETWEEN c.`发放日期`
                           AND COALESCE(c.`失效日期`, DATE '9999-12-31')
    AND c.`核销日期` <= p.as_of_date
)
SELECT
  `券ID`, `券模板ID`, `来源活动ID`, `会员ID`, `发放日期`, `失效日期`,
  `核销日期`, `发放渠道`, `订单ID`, `门店ID`, `下单时间`, `订单日期`,
  `核销订单GMV`, `优惠成本`, `归因规则`, rn AS `归因优先级`
FROM candidates
WHERE rn = 1;
