你是一个ETL专家, 正在查看如下ETL的定义(注意: 这里的所有节点都会运行在 Apache Spark 3.4上, 所有的SQL语法都是Spark的语法, 当用户让优化性能时, 不要给出建索引等通用的建议, 因为Spark不能建立索引, 并且这个ETL只能使用Spark SQL(不能使用DataFrame API), 主要给出可以"通过优化ETL节点的写法来优化性能"这种优化建议):

## 基本信息
- UniformResourceType: DATA_PROCESS_ETL
---
## ETL 流程摘要

- **总节点数:** 5
- **节点类型分布:**
  - INPUT_DATASET: 3
  - OUTPUT_DATASET: 1
  - SQL_SCRIPT: 1
- **数据输入源:**
  - j23ea7e60564e47458b71d82 (dwd_订单)
  - h551155a12fc04d88a57d319 (dim_会员主档)
  - v1.4.1 新增 input3 (param_会员生命周期阈值；平台资源 ID 待发布时生成)
- **数据输出目标:**
  - dws_会员生命周期 (目录: 0523-马甲-demo)
---
## ETL 节点详细信息


### 节点1
- Id: id_1779326818717
- Name: dwd_订单
- Type: INPUT_DATASET
- **Used By (Outputs):**
  - id_1779326818719 (SQL处理)
- Position: (200,100)
- InputDsId: j23ea7e60564e47458b71d82
- DisplayType: CSV
- PreviewScope: ALL
- 等价SQL:
```sql
SELECT * FROM input
```


### 节点2
- Id: id_1779326818718
- Name: dim_会员主档
- Type: INPUT_DATASET
- **Used By (Outputs):**
  - id_1779326818719 (SQL处理)
- Position: (200,250)
- InputDsId: h551155a12fc04d88a57d319
- DisplayType: CSV
- PreviewScope: ALL
- 等价SQL:
```sql
SELECT * FROM input
```


### 节点3（v1.4.1 新增输入）
- Id: id_v141_lifecycle_params
- Name: param_会员生命周期阈值
- Type: INPUT_DATASET
- **Used By (Outputs):**
  - id_1779326818719 (SQL处理)
- 说明: 平台当前导出版本仅有两个输入；v1.4.1 发布时必须把该参数表接为 `input3`，不得在 SQL 中回退硬编码阈值。
- 等价SQL:
```sql
SELECT * FROM input
```


### 节点4
- Id: id_1779326818719
- Name: SQL处理
- Type: SQL_SCRIPT
- **Sources (Inputs):**
  - id_1779326818717 (dwd_订单)
  - id_1779326818718 (dim_会员主档)
  - id_v141_lifecycle_params (param_会员生命周期阈值)

- **Used By (Outputs):**
  - id_1779326818720 (dws_会员生命周期)
- Position: (500,100)
- SqlScript:
```sql
WITH params AS (
  -- 统一运行参数：调度到其他快照时只替换此处
  SELECT DATE '2026-06-24' AS as_of_date
),
ranked_lifecycle_params AS (
  SELECT
    p.`业务线`,
    p.`会员类型`,
    CAST(p.`活跃天数上限` AS INT) AS `活跃天数上限`,
    CAST(p.`沉睡天数上限` AS INT) AS `沉睡天数上限`,
    CAST(p.`流失天数上限` AS INT) AS `流失天数上限`,
    CAST(p.`生效日期` AS DATE) AS `参数生效日期`,
    ROW_NUMBER() OVER (
      PARTITION BY p.`业务线`, p.`会员类型`
      ORDER BY CAST(p.`生效日期` AS DATE) DESC,
               COALESCE(CAST(p.`失效日期` AS DATE), DATE '9999-12-31') DESC
    ) AS `参数版本序号`
  FROM input3 p
  CROSS JOIN params run
  WHERE p.`业务线` = '全品牌'
    AND CAST(p.`生效日期` AS DATE) <= run.as_of_date
    AND (
      p.`失效日期` IS NULL
      OR CAST(p.`失效日期` AS DATE) >= run.as_of_date
    )
),
active_lifecycle_params AS (
  SELECT
    `业务线`, `会员类型`,
    `活跃天数上限`, `沉睡天数上限`, `流失天数上限`, `参数生效日期`
  FROM ranked_lifecycle_params
  WHERE `参数版本序号` = 1
),
member_base AS (
  SELECT
    m.`会员ID`, m.`会员等级`, CAST(m.`注册日期` AS DATE) AS `注册日期`,
    m.`注册渠道`, m.`注册门店ID`, m.`城市`,
    lp.`活跃天数上限`, lp.`沉睡天数上限`, lp.`流失天数上限`,
    lp.`参数生效日期`, run.as_of_date,
    CASE
      WHEN lp.`会员类型` IS NULL THEN '待核验'
      WHEN lp.`活跃天数上限` IS NULL
        OR lp.`沉睡天数上限` IS NULL
        OR lp.`流失天数上限` IS NULL
        OR lp.`活跃天数上限` < 0
        OR lp.`活跃天数上限` > lp.`沉睡天数上限`
        OR lp.`沉睡天数上限` > lp.`流失天数上限` THEN '参数异常'
      ELSE '已命中'
    END AS `参数状态`
  FROM input2 m
  CROSS JOIN params run
  LEFT JOIN active_lifecycle_params lp
    ON m.`会员等级` = lp.`会员类型`
  WHERE m.`会员ID` IS NOT NULL
    AND m.`会员ID` <> ''
    AND m.`注册日期` IS NOT NULL
    AND CAST(m.`注册日期` AS DATE) <= run.as_of_date
),
order_stats AS (
  SELECT
    o.`会员ID`,
    MIN(CAST(o.`业务日期` AS DATE)) AS `首单日期`,
    MAX(CAST(o.`业务日期` AS DATE)) AS `末单日期`,
    COUNT(DISTINCT o.`订单ID`) AS `总订单数`,
    SUM(COALESCE(o.`实付金额`, 0)) AS `总消费金额`,
    COUNT(DISTINCT CASE
      WHEN CAST(o.`业务日期` AS DATE) BETWEEN DATE_SUB(m.as_of_date, 29) AND m.as_of_date
      THEN o.`订单ID`
    END) AS `近30天订单`,
    COUNT(DISTINCT CASE
      WHEN CAST(o.`业务日期` AS DATE) BETWEEN DATE_SUB(m.as_of_date, 6) AND m.as_of_date
      THEN o.`订单ID`
    END) AS `近7天订单`
  FROM input1 o
  JOIN member_base m
    ON o.`会员ID` = m.`会员ID`
   AND CAST(o.`业务日期` AS DATE) >= m.`注册日期`
   AND CAST(o.`业务日期` AS DATE) <= m.as_of_date
  WHERE o.`订单状态` = '已完成'
    AND o.`业务日期` IS NOT NULL
  GROUP BY o.`会员ID`
)
SELECT
  m.`会员ID`, m.`会员等级`, m.`注册日期`, m.`注册渠道`, m.`注册门店ID`, m.`城市`,
  DATEDIFF(m.as_of_date, m.`注册日期`) AS `注册天数`,
  os.`首单日期`, os.`末单日期`,
  COALESCE(os.`总订单数`, 0) AS `总订单数`,
  COALESCE(os.`总消费金额`, 0) AS `总消费金额`,
  COALESCE(os.`近30天订单`, 0) AS `近30天订单`,
  COALESCE(os.`近7天订单`, 0) AS `近7天订单`,
  DATEDIFF(m.as_of_date, COALESCE(os.`末单日期`, m.`注册日期`)) AS `距末单天数`,
  CASE
    WHEN m.`参数状态` <> '已命中'                                           THEN '待核验'
    WHEN os.`首单日期` IS NULL
         AND DATEDIFF(m.as_of_date, m.`注册日期`) <= m.`活跃天数上限`        THEN '新客-未首单'
    WHEN os.`首单日期` IS NULL                                               THEN '注册未消费'
    WHEN DATEDIFF(m.as_of_date, os.`首单日期`) <= m.`活跃天数上限`           THEN '新客-已首单'
    WHEN DATEDIFF(m.as_of_date, os.`末单日期`) <= m.`活跃天数上限`
         AND os.`总订单数` >= 3                                              THEN '活跃'
    WHEN DATEDIFF(m.as_of_date, os.`末单日期`) <= m.`活跃天数上限`           THEN '一般活跃'
    WHEN DATEDIFF(m.as_of_date, os.`末单日期`) <= m.`沉睡天数上限`           THEN '沉睡'
    WHEN DATEDIFF(m.as_of_date, os.`末单日期`) <= m.`流失天数上限`           THEN '流失预警'
    ELSE '流失'
  END AS `生命周期阶段`,
  m.`活跃天数上限`, m.`沉睡天数上限`, m.`流失天数上限`,
  m.`参数生效日期`, m.`参数状态`,
  m.as_of_date AS `数据快照日期`
FROM member_base m
LEFT JOIN order_stats os
  ON m.`会员ID` = os.`会员ID`
```
- 等价SQL:
```sql
WITH params AS (
  -- 统一运行参数：调度到其他快照时只替换此处
  SELECT DATE '2026-06-24' AS as_of_date
),
ranked_lifecycle_params AS (
  SELECT
    p.`业务线`,
    p.`会员类型`,
    CAST(p.`活跃天数上限` AS INT) AS `活跃天数上限`,
    CAST(p.`沉睡天数上限` AS INT) AS `沉睡天数上限`,
    CAST(p.`流失天数上限` AS INT) AS `流失天数上限`,
    CAST(p.`生效日期` AS DATE) AS `参数生效日期`,
    ROW_NUMBER() OVER (
      PARTITION BY p.`业务线`, p.`会员类型`
      ORDER BY CAST(p.`生效日期` AS DATE) DESC,
               COALESCE(CAST(p.`失效日期` AS DATE), DATE '9999-12-31') DESC
    ) AS `参数版本序号`
  FROM input3 p
  CROSS JOIN params run
  WHERE p.`业务线` = '全品牌'
    AND CAST(p.`生效日期` AS DATE) <= run.as_of_date
    AND (
      p.`失效日期` IS NULL
      OR CAST(p.`失效日期` AS DATE) >= run.as_of_date
    )
),
active_lifecycle_params AS (
  SELECT
    `业务线`, `会员类型`,
    `活跃天数上限`, `沉睡天数上限`, `流失天数上限`, `参数生效日期`
  FROM ranked_lifecycle_params
  WHERE `参数版本序号` = 1
),
member_base AS (
  SELECT
    m.`会员ID`, m.`会员等级`, CAST(m.`注册日期` AS DATE) AS `注册日期`,
    m.`注册渠道`, m.`注册门店ID`, m.`城市`,
    lp.`活跃天数上限`, lp.`沉睡天数上限`, lp.`流失天数上限`,
    lp.`参数生效日期`, run.as_of_date,
    CASE
      WHEN lp.`会员类型` IS NULL THEN '待核验'
      WHEN lp.`活跃天数上限` IS NULL
        OR lp.`沉睡天数上限` IS NULL
        OR lp.`流失天数上限` IS NULL
        OR lp.`活跃天数上限` < 0
        OR lp.`活跃天数上限` > lp.`沉睡天数上限`
        OR lp.`沉睡天数上限` > lp.`流失天数上限` THEN '参数异常'
      ELSE '已命中'
    END AS `参数状态`
  FROM input2 m
  CROSS JOIN params run
  LEFT JOIN active_lifecycle_params lp
    ON m.`会员等级` = lp.`会员类型`
  WHERE m.`会员ID` IS NOT NULL
    AND m.`会员ID` <> ''
    AND m.`注册日期` IS NOT NULL
    AND CAST(m.`注册日期` AS DATE) <= run.as_of_date
),
order_stats AS (
  SELECT
    o.`会员ID`,
    MIN(CAST(o.`业务日期` AS DATE)) AS `首单日期`,
    MAX(CAST(o.`业务日期` AS DATE)) AS `末单日期`,
    COUNT(DISTINCT o.`订单ID`) AS `总订单数`,
    SUM(COALESCE(o.`实付金额`, 0)) AS `总消费金额`,
    COUNT(DISTINCT CASE
      WHEN CAST(o.`业务日期` AS DATE) BETWEEN DATE_SUB(m.as_of_date, 29) AND m.as_of_date
      THEN o.`订单ID`
    END) AS `近30天订单`,
    COUNT(DISTINCT CASE
      WHEN CAST(o.`业务日期` AS DATE) BETWEEN DATE_SUB(m.as_of_date, 6) AND m.as_of_date
      THEN o.`订单ID`
    END) AS `近7天订单`
  FROM input1 o
  JOIN member_base m
    ON o.`会员ID` = m.`会员ID`
   AND CAST(o.`业务日期` AS DATE) >= m.`注册日期`
   AND CAST(o.`业务日期` AS DATE) <= m.as_of_date
  WHERE o.`订单状态` = '已完成'
    AND o.`业务日期` IS NOT NULL
  GROUP BY o.`会员ID`
)
SELECT
  m.`会员ID`, m.`会员等级`, m.`注册日期`, m.`注册渠道`, m.`注册门店ID`, m.`城市`,
  DATEDIFF(m.as_of_date, m.`注册日期`) AS `注册天数`,
  os.`首单日期`, os.`末单日期`,
  COALESCE(os.`总订单数`, 0) AS `总订单数`,
  COALESCE(os.`总消费金额`, 0) AS `总消费金额`,
  COALESCE(os.`近30天订单`, 0) AS `近30天订单`,
  COALESCE(os.`近7天订单`, 0) AS `近7天订单`,
  DATEDIFF(m.as_of_date, COALESCE(os.`末单日期`, m.`注册日期`)) AS `距末单天数`,
  CASE
    WHEN m.`参数状态` <> '已命中'                                           THEN '待核验'
    WHEN os.`首单日期` IS NULL
         AND DATEDIFF(m.as_of_date, m.`注册日期`) <= m.`活跃天数上限`        THEN '新客-未首单'
    WHEN os.`首单日期` IS NULL                                               THEN '注册未消费'
    WHEN DATEDIFF(m.as_of_date, os.`首单日期`) <= m.`活跃天数上限`           THEN '新客-已首单'
    WHEN DATEDIFF(m.as_of_date, os.`末单日期`) <= m.`活跃天数上限`
         AND os.`总订单数` >= 3                                              THEN '活跃'
    WHEN DATEDIFF(m.as_of_date, os.`末单日期`) <= m.`活跃天数上限`           THEN '一般活跃'
    WHEN DATEDIFF(m.as_of_date, os.`末单日期`) <= m.`沉睡天数上限`           THEN '沉睡'
    WHEN DATEDIFF(m.as_of_date, os.`末单日期`) <= m.`流失天数上限`           THEN '流失预警'
    ELSE '流失'
  END AS `生命周期阶段`,
  m.`活跃天数上限`, m.`沉睡天数上限`, m.`流失天数上限`,
  m.`参数生效日期`, m.`参数状态`,
  m.as_of_date AS `数据快照日期`
FROM member_base m
LEFT JOIN order_stats os
  ON m.`会员ID` = os.`会员ID`
```


### 节点5
- Id: id_1779326818720
- Name: dws_会员生命周期
- Type: OUTPUT_DATASET
- **Sources (Inputs):**
  - id_1779326818719 (SQL处理)
- Position: (800,100)
- OutputDsName: dws_会员生命周期
- ParentDirId: v2b6bde3d41444cfd9e6d7ef
- ParentDirName: 0523-马甲-demo
- DataSourceDsId: x808f7e31adc4423e9471801
- DataSourceCreated: true
- DirPath: 根目录 > 0523-马甲-demo
- 等价SQL:
```sql
SELECT * FROM input1
```


---

## 血缘关系

### 上游资源 (3)
- **dwd_订单** (DATA_SET_FILE)
  - ID: j23ea7e60564e47458b71d82
- **dim_会员主档** (DATA_SET_FILE)
  - ID: h551155a12fc04d88a57d319
- **param_会员生命周期阈值** (DATA_SET_FILE；v1.4.1 新增 input3)
  - ID: 待发布时生成

### 下游资源 (1)
- **dws_会员生命周期** (DATA_SET_ETL)
  - ID: x808f7e31adc4423e9471801
