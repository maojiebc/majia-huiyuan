-- Spark 3.4；输入 `facts` 必须已有 `门店ID` 与 `事实日期`。
-- 每个 (门店ID, 事实日期) 最多命中一个门店版本；重叠窗口按生效起始日、版本ID 降序只留一行。
-- 关联前后事实行数必须守恒：未命中版本时属性为空，但事实行不得消失。
WITH params AS (
  SELECT DATE '2026-06-24' AS as_of_date
),
matched AS (
  SELECT
    f.*,
    s.`门店版本ID`,
    s.`门店名称`,
    s.`开业日期`,
    ROW_NUMBER() OVER (
      PARTITION BY f.`门店ID`, f.`事实日期`
      ORDER BY s.`生效起始日期` DESC, s.`门店版本ID` DESC
    ) AS scd_rn
  FROM facts f
  LEFT JOIN `dim_门店主档` s
    ON f.`门店ID` = s.`门店ID`
   AND f.`事实日期` BETWEEN CAST(s.`生效起始日期` AS DATE)
                       AND COALESCE(CAST(s.`生效截止日期` AS DATE), DATE '9999-12-31')
  CROSS JOIN params p
  WHERE f.`事实日期` <= p.as_of_date
)
SELECT *
FROM matched
WHERE scd_rn = 1;
