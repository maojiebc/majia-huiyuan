-- Spark 3.4；输入表名按目标数仓替换。
-- 输出粒度：一行 = 一笔被归因订单；每个订单最多命中一次最近有效触达。
WITH params AS (
  SELECT DATE '2026-06-24' AS as_of_date
),
valid_touch AS (
  SELECT
    t.`触达ID`, t.`会员ID`, t.`活动ID`, t.`触达渠道`,
    t.`触达时间`, t.`触达日期`, t.`是否查看`
  FROM `dwd_会员触达` t
  CROSS JOIN params p
  WHERE t.`触达状态` = '已发送'
    AND t.`会员ID` IS NOT NULL
    AND t.`会员ID` <> ''
    AND t.`触达日期` <= p.as_of_date
),
valid_order AS (
  SELECT o.*
  FROM `dwd_订单` o
  CROSS JOIN params p
  WHERE o.`订单状态` = '已完成'
    AND o.`会员ID` IS NOT NULL
    AND o.`会员ID` <> ''
    AND o.`业务日期` <= p.as_of_date
),
candidates AS (
  SELECT
    o.`订单ID`, o.`会员ID`, o.`门店ID`, o.`下单时间`, o.`业务日期` AS `订单日期`,
    o.`实付金额`, o.`收入口径金额`, o.`是否到店`,
    t.`触达ID`, t.`活动ID`, t.`触达渠道`, t.`触达时间`, t.`触达日期`,
    t.`是否查看`,
    '下单前最近一次有效触达（0-7天）' AS `归因规则`,
    ROW_NUMBER() OVER (
      PARTITION BY o.`订单ID`
      ORDER BY t.`触达时间` DESC, t.`触达ID` DESC
    ) AS rn
  FROM valid_order o
  JOIN valid_touch t
    ON o.`会员ID` = t.`会员ID`
   AND o.`下单时间` >= t.`触达时间`
  CROSS JOIN params p
  WHERE o.`下单时间` < t.`触达时间` + INTERVAL 8 DAYS
)
SELECT
  `订单ID`, `会员ID`, `门店ID`, `下单时间`, `订单日期`,
  `实付金额`, `收入口径金额`, `是否到店`,
  `触达ID`, `活动ID`, `触达渠道`, `触达时间`, `触达日期`, `是否查看`,
  `归因规则`, rn AS `归因优先级`
FROM candidates
WHERE rn = 1;
