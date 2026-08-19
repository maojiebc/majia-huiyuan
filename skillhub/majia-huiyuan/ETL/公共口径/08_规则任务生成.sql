-- Spark 3.4；输出粒度：一个会员至多一条规则任务。
-- 九类圈选 UNION ALL 之后：营销任务先挡近 7 日触达，负评修复豁免；再按 P0<P1<P2、预计价值降序只留一条。
-- 这是候选任务，不是已分派的 dwd_会员经营任务。预计价值冷启动用历史客单价，不是增量。
-- 营销任务按会员ID数字尾号 10% holdout（尾号 0）；负评修复不进对照。没有对照转化率时仍不得写增量。
WITH params AS (
  SELECT DATE '2026-06-24' AS as_of_date
),
r_流失预警 AS (
  SELECT
    l.`会员ID`, l.`注册门店ID` AS `归属门店ID`,
    '规则生成' AS `任务来源`, 'P0' AS `任务优先级`,
    '流失预警' AS `任务类型`, l.`生命周期阶段` AS `人群标签`,
    '电话回访+召回券' AS `推荐动作`,
    '沉睡唤醒 9 折' AS `推荐权益`,
    CONCAT(CAST(l.`距末单天数` AS STRING), ' 天未消费，接近流失阈值') AS `推荐原因`,
    ROUND(l.`总消费金额` / NULLIF(l.`总订单数`, 0), 2) AS `预计价值`
  FROM `dws_会员生命周期` l
  CROSS JOIN params p
  WHERE l.`会员ID` IS NOT NULL AND l.`会员ID` <> ''
    AND l.`生命周期阶段` = '流失预警'
    AND l.`数据快照日期` = p.as_of_date
),
r_负评修复 AS (
  SELECT
    e.`会员ID`, e.`门店ID` AS `归属门店ID`,
    '规则生成' AS `任务来源`, 'P0' AS `任务优先级`,
    '负评修复' AS `任务类型`,
    COALESCE(NULLIF(e.`负评标签`, ''), '低分') AS `人群标签`,
    '服务补救+客服回访' AS `推荐动作`,
    '免费小食券' AS `推荐权益`,
    CONCAT(e.`评价平台`, ' ', CAST(e.`评分` AS STRING), ' 分') AS `推荐原因`,
    CAST(NULL AS DOUBLE) AS `预计价值`
  FROM (
    SELECT
      x.*,
      ROW_NUMBER() OVER (
        PARTITION BY x.`会员ID`
        ORDER BY x.`评分` ASC, x.`评价日期` DESC, x.`评价ID` DESC
      ) AS review_rn
    FROM `dwd_评价` x
    CROSS JOIN params p
    WHERE x.`会员ID` IS NOT NULL AND x.`会员ID` <> ''
      AND (x.`评分` <= 2 OR (x.`负评标签` IS NOT NULL AND x.`负评标签` <> ''))
      AND x.`回复状态` <> '已回复'
      AND x.`评价日期` BETWEEN DATE_SUB(p.as_of_date, 3) AND p.as_of_date
  ) e
  WHERE e.review_rn = 1
),
r_沉睡召回 AS (
  SELECT
    l.`会员ID`, l.`注册门店ID` AS `归属门店ID`,
    '规则生成' AS `任务来源`, 'P1' AS `任务优先级`,
    '沉睡召回' AS `任务类型`, l.`生命周期阶段` AS `人群标签`,
    '强召回权益+低频触达' AS `推荐动作`,
    '拉新返券' AS `推荐权益`,
    CONCAT(CAST(l.`距末单天数` AS STRING), ' 天未消费') AS `推荐原因`,
    ROUND(l.`总消费金额` / NULLIF(l.`总订单数`, 0), 2) AS `预计价值`
  FROM `dws_会员生命周期` l
  CROSS JOIN params p
  WHERE l.`会员ID` IS NOT NULL AND l.`会员ID` <> ''
    AND l.`生命周期阶段` = '沉睡'
    AND l.`数据快照日期` = p.as_of_date
),
r_新会员首单 AS (
  SELECT
    l.`会员ID`, l.`注册门店ID` AS `归属门店ID`,
    '规则生成' AS `任务来源`, 'P1' AS `任务优先级`,
    '新会员首单' AS `任务类型`, l.`生命周期阶段` AS `人群标签`,
    '发首单券+邀约入群' AS `推荐动作`,
    '新人首单特价' AS `推荐权益`,
    CONCAT('注册 ', CAST(l.`注册天数` AS STRING), ' 天未首单') AS `推荐原因`,
    CAST(NULL AS DOUBLE) AS `预计价值`
  FROM `dws_会员生命周期` l
  CROSS JOIN params p
  WHERE l.`会员ID` IS NOT NULL AND l.`会员ID` <> ''
    AND l.`生命周期阶段` IN ('新客-未首单', '注册未消费')
    AND l.`数据快照日期` = p.as_of_date
),
r_首单后二单 AS (
  SELECT
    l.`会员ID`, l.`注册门店ID` AS `归属门店ID`,
    '规则生成' AS `任务来源`, 'P1' AS `任务优先级`,
    '首单后二单' AS `任务类型`, l.`生命周期阶段` AS `人群标签`,
    '发二单券+推荐爆款' AS `推荐动作`,
    '满 30 减 10' AS `推荐权益`,
    CONCAT('首单后 ', CAST(l.`距末单天数` AS STRING), ' 天未二单') AS `推荐原因`,
    ROUND(l.`总消费金额` / NULLIF(l.`总订单数`, 0), 2) AS `预计价值`
  FROM `dws_会员生命周期` l
  CROSS JOIN params p
  WHERE l.`会员ID` IS NOT NULL AND l.`会员ID` <> ''
    AND l.`生命周期阶段` = '新客-已首单'
    AND l.`总订单数` = 1
    AND l.`数据快照日期` = p.as_of_date
),
r_外卖转到店 AS (
  SELECT
    c.`会员ID`, CAST(NULL AS STRING) AS `归属门店ID`,
    '规则生成' AS `任务来源`, 'P2' AS `任务优先级`,
    '外卖转到店' AS `任务类型`, c.`迁移类型` AS `人群标签`,
    '到店专享+堂食套餐' AS `推荐动作`,
    '下午茶套餐' AS `推荐权益`,
    CONCAT('近30天外卖 ', CAST(c.`近30天外卖` AS STRING), ' 单，堂食 ', CAST(c.`近30天堂食` AS STRING), ' 单') AS `推荐原因`,
    CAST(NULL AS DOUBLE) AS `预计价值`
  FROM `dws_渠道迁移分析` c
  CROSS JOIN params p
  WHERE c.`会员ID` IS NOT NULL AND c.`会员ID` <> ''
    AND c.`迁移类型` IN ('纯外卖', '堂食→外卖迁移')
    AND c.`数据快照日期` = p.as_of_date
),
r_堂食老客召回 AS (
  SELECT
    c.`会员ID`, CAST(NULL AS STRING) AS `归属门店ID`,
    '规则生成' AS `任务来源`, 'P2' AS `任务优先级`,
    '堂食老客召回' AS `任务类型`, c.`迁移类型` AS `人群标签`,
    '新品邀请+同行福利' AS `推荐动作`,
    '新品体验 5 折' AS `推荐权益`,
    CONCAT('纯堂食且 R 分 ', CAST(r.`R分` AS STRING)) AS `推荐原因`,
    ROUND(r.`消费金额` / NULLIF(r.`消费次数`, 0), 2) AS `预计价值`
  FROM `dws_渠道迁移分析` c
  JOIN `dws_会员RFM分层` r ON c.`会员ID` = r.`会员ID`
  CROSS JOIN params p
  WHERE c.`会员ID` IS NOT NULL AND c.`会员ID` <> ''
    AND c.`迁移类型` = '纯堂食'
    AND r.`R分` <= 2
    AND c.`数据快照日期` = p.as_of_date
    AND r.`数据快照日期` = p.as_of_date
),
r_高价值维护 AS (
  SELECT
    r.`会员ID`, CAST(NULL AS STRING) AS `归属门店ID`,
    '规则生成' AS `任务来源`, 'P2' AS `任务优先级`,
    '高价值维护' AS `任务类型`, r.`RFM标签` AS `人群标签`,
    '专属权益+会员日邀约' AS `推荐动作`,
    '高等级专享折扣' AS `推荐权益`,
    'RFM 重要价值客户' AS `推荐原因`,
    ROUND(r.`消费金额` / NULLIF(r.`消费次数`, 0), 2) AS `预计价值`
  FROM `dws_会员RFM分层` r
  CROSS JOIN params p
  WHERE r.`会员ID` IS NOT NULL AND r.`会员ID` <> ''
    AND r.`RFM标签` = '重要价值客户'
    AND r.`数据快照日期` = p.as_of_date
),
r_领券未核销 AS (
  SELECT
    c.`会员ID`, c.`门店ID` AS `归属门店ID`,
    '规则生成' AS `任务来源`, 'P2' AS `任务优先级`,
    '领券未核销' AS `任务类型`, '临近失效' AS `人群标签`,
    '核销提醒' AS `推荐动作`,
    '原券+到期提示' AS `推荐权益`,
    CONCAT('券将于 ', DATE_FORMAT(c.`失效日期`, 'yyyy-MM-dd'), ' 失效') AS `推荐原因`,
    CAST(NULL AS DOUBLE) AS `预计价值`
  FROM (
    SELECT
      x.*,
      ROW_NUMBER() OVER (
        PARTITION BY x.`会员ID`
        ORDER BY x.`失效日期` ASC, x.`券ID` ASC
      ) AS coupon_rn
    FROM `dwd_券事件` x
    CROSS JOIN params p
    WHERE x.`会员ID` IS NOT NULL AND x.`会员ID` <> ''
      AND x.`核销日期` IS NULL
      AND x.`发放日期` <= p.as_of_date
      AND x.`失效日期` BETWEEN p.as_of_date AND DATE_ADD(p.as_of_date, 7)
  ) c
  WHERE c.coupon_rn = 1
),
candidates AS (
  SELECT * FROM r_流失预警
  UNION ALL SELECT * FROM r_负评修复
  UNION ALL SELECT * FROM r_沉睡召回
  UNION ALL SELECT * FROM r_新会员首单
  UNION ALL SELECT * FROM r_首单后二单
  UNION ALL SELECT * FROM r_外卖转到店
  UNION ALL SELECT * FROM r_堂食老客召回
  UNION ALL SELECT * FROM r_高价值维护
  UNION ALL SELECT * FROM r_领券未核销
),
recent_touch AS (
  SELECT DISTINCT t.`会员ID`
  FROM `dwd_会员触达` t
  CROSS JOIN params p
  WHERE t.`会员ID` IS NOT NULL AND t.`会员ID` <> ''
    AND t.`触达日期` BETWEEN DATE_SUB(p.as_of_date, 7) AND p.as_of_date
),
reachable AS (
  SELECT c.*
  FROM candidates c
  LEFT JOIN recent_touch d ON c.`会员ID` = d.`会员ID`
  WHERE d.`会员ID` IS NULL OR c.`任务类型` = '负评修复'
),
ranked AS (
  SELECT
    r.*,
    CASE
      WHEN r.`任务类型` = '负评修复' THEN 0
      WHEN CAST(REGEXP_REPLACE(r.`会员ID`, '[^0-9]', '') AS BIGINT) % 10 = 0 THEN 1
      ELSE 0
    END AS `是否对照组`,
    p.as_of_date AS `数据快照日期`,
    ROW_NUMBER() OVER (
      PARTITION BY r.`会员ID`
      ORDER BY r.`任务优先级` ASC, r.`预计价值` DESC, r.`任务类型` ASC
    ) AS rn
  FROM reachable r
  CROSS JOIN params p
)
SELECT
  `会员ID`, `归属门店ID`, `任务来源`, `任务优先级`, `任务类型`, `人群标签`,
  `推荐动作`, `推荐权益`, `推荐原因`, `预计价值`, `是否对照组`, `数据快照日期`
FROM ranked
WHERE rn = 1;
