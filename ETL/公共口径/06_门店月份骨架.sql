-- Spark 3.4；输出粒度：一门店 × 一个营业月份。
-- 从开业月到 min(闭店月, as_of 月) 每月一行，零销售但有成本的月份必须留下。
-- 开闭店边界只取当前版本；月份属性按 `维度命中日期` 走 07_SCD2。
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
store_months AS (
  SELECT
    s.`门店ID`,
    EXPLODE(SEQUENCE(
      TRUNC(s.`开业日期`, 'MM'),
      TRUNC(LEAST(p.as_of_date, COALESCE(s.`闭店日期`, p.as_of_date)), 'MM'),
      INTERVAL 1 MONTH
    )) AS `月份日期`,
    p.as_of_date
  FROM store_current s
  CROSS JOIN params p
  WHERE s.`开业日期` IS NOT NULL
    AND s.`开业日期` <= p.as_of_date
    AND COALESCE(s.`闭店日期`, p.as_of_date) >= s.`开业日期`
)
SELECT
  `门店ID`,
  `月份日期`,
  LEAST(LAST_DAY(`月份日期`), as_of_date) AS `维度命中日期`,
  as_of_date AS `数据快照日期`
FROM store_months;
