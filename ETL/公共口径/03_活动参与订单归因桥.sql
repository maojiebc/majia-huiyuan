-- Spark 3.4；输出粒度：一行 = 一笔被归因订单。
-- 同一订单命中多个活动参与时，只归给下单前最近一次有效参与。
WITH params AS (
  SELECT DATE '2026-06-24' AS as_of_date
),
valid_participation AS (
  SELECT a.*
  FROM `dwd_活动参与` a
  CROSS JOIN params p
  WHERE a.`结果` = '成功'
    AND a.`会员ID` IS NOT NULL
    AND a.`会员ID` <> ''
    AND a.`参与日期` <= p.as_of_date
),
candidates AS (
  SELECT
    o.`订单ID`, o.`会员ID`, o.`门店ID`, o.`下单时间`, o.`业务日期` AS `订单日期`,
    o.`实付金额`, o.`收入口径金额`,
    a.`参与ID`, a.`活动ID`, a.`参与时间`, a.`参与日期`, a.`行为类型`,
    '下单前最近一次有效活动参与（0-7天）' AS `归因规则`,
    ROW_NUMBER() OVER (
      PARTITION BY o.`订单ID`
      ORDER BY a.`参与时间` DESC, a.`参与ID` DESC
    ) AS rn
  FROM `dwd_订单` o
  JOIN valid_participation a
    ON o.`会员ID` = a.`会员ID`
   AND o.`下单时间` >= a.`参与时间`
  CROSS JOIN params p
  WHERE o.`订单状态` = '已完成'
    AND o.`会员ID` IS NOT NULL
    AND o.`会员ID` <> ''
    AND o.`业务日期` <= p.as_of_date
    AND o.`下单时间` < a.`参与时间` + INTERVAL 8 DAYS
)
SELECT
  `订单ID`, `会员ID`, `门店ID`, `下单时间`, `订单日期`, `实付金额`, `收入口径金额`,
  `参与ID`, `活动ID`, `参与时间`, `参与日期`, `行为类型`, `归因规则`,
  rn AS `归因优先级`
FROM candidates
WHERE rn = 1;
