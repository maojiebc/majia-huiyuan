-- Spark 3.4；建议作为 dqc_归因清单对账 的 v1.4.1 扩展节点。
-- 约定：三条公共事实桥已物化为同名临时视图；每个检查输出异常数，必须为 0。
WITH params AS (
  SELECT DATE '2026-06-24' AS as_of_date
),
attr_dup AS (
  SELECT COUNT(*) AS bad_count
  FROM (
    SELECT `订单ID`
    FROM `bridge_触达订单归因`
    GROUP BY `订单ID`
    HAVING COUNT(*) > 1
  ) x
),
attr_gmv_over AS (
  SELECT COUNT(*) AS bad_count
  FROM (
    SELECT a.`订单日期`
    FROM (
      SELECT `订单日期`, SUM(`实付金额`) AS gmv
      FROM `bridge_触达订单归因`
      GROUP BY `订单日期`
    ) a
    JOIN (
      SELECT `业务日期`, SUM(`实付金额`) AS gmv
      FROM `dwd_订单`
      CROSS JOIN params p
      WHERE `订单状态` = '已完成'
        AND `业务日期` <= p.as_of_date
      GROUP BY `业务日期`
    ) o ON a.`订单日期` = o.`业务日期`
    WHERE a.gmv > o.gmv
  ) x
),
cohort_bad AS (
  SELECT COUNT(*) AS bad_count
  FROM `dws_会员同期群留存`
  WHERE (`留存月份序号` = 'M0' AND `留存人数` <> `同期群人数`)
     OR `留存人数` > `同期群人数`
     OR `留存率` < 0 OR `留存率` > 1
),
zero_sale_cost_missing AS (
  SELECT COUNT(*) AS bad_count
  FROM (
    SELECT DISTINCT c.`门店ID`, TRUNC(CAST(c.`月份` AS DATE), 'MM') AS month_start
    FROM `dwd_门店成本明细` c
    LEFT ANTI JOIN `dws_单店利润月汇总` p
      ON c.`门店ID` = p.`门店ID`
     AND TRUNC(CAST(c.`月份` AS DATE), 'MM') = CAST(CONCAT(p.`月份`, '-01') AS DATE)
    WHERE c.`成本金额` <> 0
  ) x
),
loss_with_previous AS (
  SELECT
    `门店ID`, `月份`, `店面贡献利润`, `连续亏损月数`,
    LAG(`月份`) OVER (PARTITION BY `门店ID` ORDER BY `月份`) AS previous_month,
    LAG(`店面贡献利润`) OVER (PARTITION BY `门店ID` ORDER BY `月份`) AS previous_profit,
    LAG(`连续亏损月数`) OVER (PARTITION BY `门店ID` ORDER BY `月份`) AS previous_streak
  FROM `ads_单店利润健康`
),
loss_streak_bad AS (
  SELECT COUNT(*) AS bad_count
  FROM (
    SELECT *,
      CASE
        WHEN `店面贡献利润` >= 0 THEN 0
        WHEN previous_month = DATE_FORMAT(
               ADD_MONTHS(CAST(CONCAT(`月份`, '-01') AS DATE), -1), 'yyyy-MM'
             )
         AND previous_profit < 0
          THEN previous_streak + 1
        ELSE 1
      END AS expected_streak
    FROM loss_with_previous
  ) x
  WHERE COALESCE(`连续亏损月数`, -1) <> expected_streak
),
payback_bad AS (
  SELECT COUNT(*) AS bad_count
  FROM `dws_加盟回本测算`
  WHERE (`预计完整回本日期` IS NOT NULL AND `预计完整回本日期` < `投资起始日`)
     OR (`回本状态` = '当前不可测算' AND `预计完整回本日期` IS NOT NULL)
     OR `剩余回本月数` < 0
),
scd2_bad AS (
  SELECT COUNT(*) AS bad_count
  FROM (
    SELECT o.`订单ID`
    FROM `dwd_订单` o
    LEFT JOIN `dim_门店主档` s
      ON o.`门店ID` = s.`门店ID`
     AND o.`业务日期` BETWEEN CAST(s.`生效起始日期` AS DATE)
                         AND COALESCE(CAST(s.`生效截止日期` AS DATE), DATE '9999-12-31')
    GROUP BY o.`订单ID`
    HAVING COUNT(s.`门店版本ID`) <> 1
  ) x
),
late_fact_bad AS (
  SELECT COUNT(*) AS bad_count
  FROM (
    SELECT `业务日期` AS fact_date FROM `dwd_订单`
    UNION ALL SELECT `触达日期` FROM `dwd_会员触达`
    UNION ALL SELECT `参与日期` FROM `dwd_活动参与`
    UNION ALL SELECT `核销日期` FROM `dwd_券事件` WHERE `核销日期` IS NOT NULL
  ) f CROSS JOIN params p
  WHERE f.fact_date > p.as_of_date
),
param_bad AS (
  SELECT COUNT(*) AS bad_count
  FROM (
    SELECT m.`会员ID`
    FROM `dim_会员主档` m
    CROSS JOIN params x
    LEFT JOIN `param_会员生命周期阈值` p
      ON p.`业务线` = '全品牌' AND p.`会员类型` = m.`会员等级`
     AND x.as_of_date BETWEEN p.`生效日期` AND COALESCE(p.`失效日期`, DATE '9999-12-31')
    GROUP BY m.`会员ID`
    HAVING COUNT(p.`会员类型`) <> 1
  ) x
)
SELECT '10' AS `序号`, '归因唯一性' AS `检查类别`, '触达归因订单ID不重复' AS `检查项`,
       '0' AS `期望值`, CAST(bad_count AS STRING) AS `实际值`, IF(bad_count = 0, '通过', '异常') AS `状态` FROM attr_dup
UNION ALL SELECT '11','金额护栏','归因GMV不大于同期已完成订单GMV','0',CAST(bad_count AS STRING),IF(bad_count=0,'通过','异常') FROM attr_gmv_over
UNION ALL SELECT '12','留存','M0=同期群人数且Mn不超过M0','0',CAST(bad_count AS STRING),IF(bad_count=0,'通过','异常') FROM cohort_bad
UNION ALL SELECT '13','利润骨架','零销售但有成本的门店月份仍存在','0',CAST(bad_count AS STRING),IF(bad_count=0,'通过','异常') FROM zero_sale_cost_missing
UNION ALL SELECT '14','连续性','连续亏损在盈利月或月份断点后重置','0',CAST(bad_count AS STRING),IF(bad_count=0,'通过','异常') FROM loss_streak_bad
UNION ALL SELECT '15','回本','回本日期/状态/剩余月数合法','0',CAST(bad_count AS STRING),IF(bad_count=0,'通过','异常') FROM payback_bad
UNION ALL SELECT '16','SCD2','事实日期只命中一个门店版本','0',CAST(bad_count AS STRING),IF(bad_count=0,'通过','异常') FROM scd2_bad
UNION ALL SELECT '17','时间','事实日期不晚于统一快照','0',CAST(bad_count AS STRING),IF(bad_count=0,'通过','异常') FROM late_fact_bad
UNION ALL SELECT '18','参数','每个会员在快照日恰好命中一条生命周期参数','0',CAST(bad_count AS STRING),IF(bad_count=0,'通过','异常') FROM param_bad;
