-- Spark 3.4；输出粒度：一门店 × 一个自然营业日。
-- 开业日至 min(闭店日, as_of_date) 每天一行，当天 0 单也保留。
-- 开闭店边界只取当前版本；日属性必须再走 07_SCD2 时点关联，禁止直接用当前版本回写历史日。
WITH params AS (
  SELECT DATE '2026-06-24' AS as_of_date
),
store_current AS (
  SELECT
    s.`门店ID`,
    CAST(s.`开业日期` AS DATE) AS `开业日期`,
    CASE
      WHEN s.`闭店日期` IS NULL OR TRIM(s.`闭店日期`) = '' OR LOWER(TRIM(s.`闭店日期`)) = 'null'
        THEN NULL
      ELSE TO_DATE(s.`闭店日期`)
    END AS `闭店日期`
  FROM `dim_门店主档` s
  WHERE s.`当前版本标记` = 1
),
store_bounds AS (
  SELECT
    s.`门店ID`,
    s.`开业日期`,
    LEAST(p.as_of_date, COALESCE(s.`闭店日期`, p.as_of_date)) AS `营业截止日期`,
    p.as_of_date
  FROM store_current s
  CROSS JOIN params p
  WHERE s.`开业日期` IS NOT NULL
    AND s.`开业日期` <= p.as_of_date
    AND COALESCE(s.`闭店日期`, p.as_of_date) >= s.`开业日期`
)
SELECT
  s.`门店ID`,
  d.`业务日期`,
  s.as_of_date AS `数据快照日期`
FROM store_bounds s
LATERAL VIEW EXPLODE(SEQUENCE(
  s.`开业日期`,
  s.`营业截止日期`,
  INTERVAL 1 DAY
)) d AS `业务日期`;
