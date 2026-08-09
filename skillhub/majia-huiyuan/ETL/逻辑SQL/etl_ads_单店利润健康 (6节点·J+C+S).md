你是一个ETL专家, 正在查看如下ETL的定义(注意: 这里的所有节点都会运行在 Apache Spark 3.4上, 所有的SQL语法都是Spark的语法, 当用户让优化性能时, 不要给出建索引等通用的建议, 因为Spark不能建立索引, 并且这个ETL只能使用Spark SQL(不能使用DataFrame API), 主要给出可以"通过优化ETL节点的写法来优化性能"这种优化建议):

## 基本信息
- UniformResourceType: DATA_PROCESS_ETL
---
## ETL 流程摘要

- **总节点数:** 6
- **节点类型分布:**
  - CALCULATOR: 1
  - INPUT_DATASET: 2
  - JOIN_DATA: 1
  - OUTPUT_DATASET: 1
  - SQL_SCRIPT: 1
- **数据输入源:**
  - l6ee75fc812be413583215e4 (dws_单店利润月汇总)
  - l9312c8ef7ec14877889f06b (param_利润健康阈值)
- **数据输出目标:**
  - ads_单店利润健康 (目录: 马甲的demo-0523)
- **时间口径:** 继承上游 `数据快照日期`，与同批次 `as_of_date` 一致
---
## ETL 节点详细信息


### 节点1
- Id: id_1779346047774
- Name: dws_单店利润月汇总
- Type: INPUT_DATASET
- **Used By (Outputs):**
  - id_1779346047776 (关联店型阈值表)
- Position: (100,100)
- InputDsId: l6ee75fc812be413583215e4
- DisplayType: DATAFLOW
- PreviewScope: ALL
- 等价SQL:
```sql
SELECT * FROM input
```


### 节点2
- Id: id_1779346047778
- Name: 连续亏损窗口分析
- Type: SQL_SCRIPT
- **Sources (Inputs):**
  - id_1779346047777 (利润健康标签+建议动作)

- **Used By (Outputs):**
  - id_1779346047779 (ads_单店利润健康)
- Position: (900,250)
- SqlScript:
```sql
WITH loss_numbered AS (
  -- 只对亏损月编号；月份减去序号后相同，表示自然月连续且没有被盈利月打断。
  SELECT
    r.`门店ID`, r.`月份`,
    ADD_MONTHS(
      TO_DATE(CONCAT(SUBSTR(CAST(r.`月份` AS STRING), 1, 7), '-01')),
      -CAST(ROW_NUMBER() OVER (PARTITION BY r.`门店ID` ORDER BY r.`月份`) AS INT)
    ) AS `连续分组键`
  FROM input1 r
  WHERE r.`店面贡献利润` < 0
),
loss_streak AS (
  SELECT
    `门店ID`, `月份`,
    ROW_NUMBER() OVER (PARTITION BY `门店ID`, `连续分组键` ORDER BY `月份`) AS `连续亏损月数`
  FROM loss_numbered
),
loss_summary AS (
  SELECT
    r.`门店ID`,
    SUM(CASE WHEN r.`店面贡献利润` < 0 THEN 1 ELSE 0 END) AS `历史亏损月数`,
    MAX(CASE WHEN TO_DATE(CONCAT(SUBSTR(CAST(r.`月份` AS STRING), 1, 7), '-01'))
                         BETWEEN ADD_MONTHS(TRUNC(r.`数据快照日期`, 'MM'), -2)
                             AND TRUNC(r.`数据快照日期`, 'MM')
                  AND r.`店面贡献利润` < 0 THEN 1 ELSE 0 END) AS `近3月有亏损`
  FROM input1 r
  GROUP BY r.`门店ID`
)
SELECT
  r.`门店ID`, r.`门店名称`, r.`省份`, r.`城市`, r.`城市层级`, r.`门店类型`, r.`商圈`, r.`直营加盟类型`,
  r.`月份`, r.`月营收`, r.`毛利`, r.`店面贡献利润`, r.`单店净利润`,
  r.`毛利率`, r.`店面贡献利润率`, r.`单店净利率`, r.`堂食占比`, r.`外卖占比`,
  r.`人工占比`, r.`房租占比`, r.`客单价`,
  r.`房租占比上限`, r.`人工占比上限`, r.`堂食占比下限`, r.`持续亏损预警月数`,
  r.`毛利率塌方阈值pp`, r.`适用范围`,
  r.`房租超标`, r.`人工超标`, r.`堂食衰减`, r.`本月亏损`, r.`利润健康等级`, r.`预警条数`, r.`建议动作`,
  s.`历史亏损月数`,
  s.`近3月有亏损`,
  COALESCE(g.`连续亏损月数`, 0) AS `连续亏损月数`,
  CASE WHEN COALESCE(g.`连续亏损月数`, 0) >= r.`持续亏损预警月数`
       THEN '持续亏损' ELSE '非持续亏损' END AS `持续亏损标签`,
  r.`数据快照日期`
FROM input1 r
LEFT JOIN loss_streak g ON r.`门店ID` = g.`门店ID` AND r.`月份` = g.`月份`
LEFT JOIN loss_summary s ON r.`门店ID` = s.`门店ID`
```
- 等价SQL:
```sql
WITH loss_numbered AS (
  -- 只对亏损月编号；月份减去序号后相同，表示自然月连续且没有被盈利月打断。
  SELECT
    r.`门店ID`, r.`月份`,
    ADD_MONTHS(
      TO_DATE(CONCAT(SUBSTR(CAST(r.`月份` AS STRING), 1, 7), '-01')),
      -CAST(ROW_NUMBER() OVER (PARTITION BY r.`门店ID` ORDER BY r.`月份`) AS INT)
    ) AS `连续分组键`
  FROM input1 r
  WHERE r.`店面贡献利润` < 0
),
loss_streak AS (
  SELECT
    `门店ID`, `月份`,
    ROW_NUMBER() OVER (PARTITION BY `门店ID`, `连续分组键` ORDER BY `月份`) AS `连续亏损月数`
  FROM loss_numbered
),
loss_summary AS (
  SELECT
    r.`门店ID`,
    SUM(CASE WHEN r.`店面贡献利润` < 0 THEN 1 ELSE 0 END) AS `历史亏损月数`,
    MAX(CASE WHEN TO_DATE(CONCAT(SUBSTR(CAST(r.`月份` AS STRING), 1, 7), '-01'))
                         BETWEEN ADD_MONTHS(TRUNC(r.`数据快照日期`, 'MM'), -2)
                             AND TRUNC(r.`数据快照日期`, 'MM')
                  AND r.`店面贡献利润` < 0 THEN 1 ELSE 0 END) AS `近3月有亏损`
  FROM input1 r
  GROUP BY r.`门店ID`
)
SELECT
  r.`门店ID`, r.`门店名称`, r.`省份`, r.`城市`, r.`城市层级`, r.`门店类型`, r.`商圈`, r.`直营加盟类型`,
  r.`月份`, r.`月营收`, r.`毛利`, r.`店面贡献利润`, r.`单店净利润`,
  r.`毛利率`, r.`店面贡献利润率`, r.`单店净利率`, r.`堂食占比`, r.`外卖占比`,
  r.`人工占比`, r.`房租占比`, r.`客单价`,
  r.`房租占比上限`, r.`人工占比上限`, r.`堂食占比下限`, r.`持续亏损预警月数`,
  r.`毛利率塌方阈值pp`, r.`适用范围`,
  r.`房租超标`, r.`人工超标`, r.`堂食衰减`, r.`本月亏损`, r.`利润健康等级`, r.`预警条数`, r.`建议动作`,
  s.`历史亏损月数`,
  s.`近3月有亏损`,
  COALESCE(g.`连续亏损月数`, 0) AS `连续亏损月数`,
  CASE WHEN COALESCE(g.`连续亏损月数`, 0) >= r.`持续亏损预警月数`
       THEN '持续亏损' ELSE '非持续亏损' END AS `持续亏损标签`,
  r.`数据快照日期`
FROM input1 r
LEFT JOIN loss_streak g ON r.`门店ID` = g.`门店ID` AND r.`月份` = g.`月份`
LEFT JOIN loss_summary s ON r.`门店ID` = s.`门店ID`
```


### 节点3
- Id: id_1779346047775
- Name: param_利润健康阈值
- Type: INPUT_DATASET
- **Used By (Outputs):**
  - id_1779346047776 (关联店型阈值表)
- Position: (100,400)
- InputDsId: l9312c8ef7ec14877889f06b
- DisplayType: CSV
- PreviewScope: ALL
- 等价SQL:
```sql
SELECT * FROM input
```


### 节点4
- Id: id_1779346047777
- Name: 利润健康标签+建议动作
- Type: CALCULATOR
- **Sources (Inputs):**
  - id_1779346047776 (关联店型阈值表)

- **Used By (Outputs):**
  - id_1779346047778 (连续亏损窗口分析)
- Position: (600,250)
- FormulaNames:
  - 房租超标
  - 人工超标
  - 堂食衰减
  - 本月亏损
  - 利润健康等级
  - 预警条数
  - 建议动作
- 等价SQL:
```sql
SELECT
  `门店ID`, `门店名称`, `省份`, `城市`, `城市层级`, `门店类型`, `商圈`, `直营加盟类型`,
  `月份`, `月营收`, `毛利`, `店面贡献利润`, `单店净利润`,
  `毛利率`, `店面贡献利润率`, `单店净利率`, `堂食占比`, `外卖占比`,
  `人工占比`, `房租占比`, `客单价`,
  `房租占比上限`, `人工占比上限`, `堂食占比下限`, `持续亏损预警月数`,
  `毛利率塌方阈值pp`, `适用范围`,
  case when `房租占比` > `房租占比上限` then 'TRUE' else 'FALSE' end AS `房租超标`,
  case when `人工占比` > `人工占比上限` then 'TRUE' else 'FALSE' end AS `人工超标`,
  case when `堂食占比` < `堂食占比下限` and `门店类型` <> '外卖卫星店' then 'TRUE' else 'FALSE' end AS `堂食衰减`,
  case when `店面贡献利润` < 0 then 'TRUE' else 'FALSE' end AS `本月亏损`,
  case when `店面贡献利润` < 0 then '严重亏损' when `店面贡献利润率` < 0.05 then '微利' when `店面贡献利润率` < 0.12 then '关注' when `店面贡献利润率` < 0.20 then '健康' else '标杆' end AS `利润健康等级`,
  (case when `房租占比` > `房租占比上限` then 1 else 0 end) + (case when `人工占比` > `人工占比上限` then 1 else 0 end) + (case when `堂食占比` < `堂食占比下限` and `门店类型` <> '外卖卫星店' then 1 else 0 end) + (case when `店面贡献利润` < 0 then 1 else 0 end) AS `预警条数`,
  case when `店面贡献利润` < 0 then '启动关店评估' when `房租占比` > `房租占比上限` then '考虑闭店或营收提升' when `人工占比` > `人工占比上限` then '检查排班 / 客流不匹配' when `堂食占比` < `堂食占比下限` and `门店类型` <> '外卖卫星店' then '进入堂食提振专题' when `店面贡献利润率` >= 0.20 then '标杆门店, 复制打法' else '正常运营' end AS `建议动作`,
  `数据快照日期`
FROM input1
```


### 节点5
- Id: id_1779346047776
- Name: 关联店型阈值表
- Type: JOIN_DATA
- **Sources (Inputs):**
  - id_1779346047774 (dws_单店利润月汇总)
  - id_1779346047775 (param_利润健康阈值)

- **Used By (Outputs):**
  - id_1779346047777 (利润健康标签+建议动作)
- Position: (300,250)
- 等价SQL:
```sql
SELECT
  p.`门店ID`, p.`门店名称`, p.`省份`, p.`城市`, p.`城市层级`, p.`门店类型`, p.`商圈`, p.`直营加盟类型`,
  p.`月份`, p.`月营收`, p.`毛利`, p.`店面贡献利润`, p.`单店净利润`,
  p.`毛利率`, p.`店面贡献利润率`, p.`单店净利率`, p.`堂食占比`, p.`外卖占比`,
  p.`人工占比`, p.`房租占比`, p.`客单价`,
  t.`房租占比上限`, t.`人工占比上限`, t.`堂食占比下限`, t.`持续亏损预警月数`,
  t.`毛利率塌方阈值pp`, t.`适用范围`, p.`数据快照日期`
FROM input1 p
LEFT JOIN input2 t ON p.`门店类型` = t.`门店类型`
```


### 节点6
- Id: id_1779346047779
- Name: ads_单店利润健康
- Type: OUTPUT_DATASET
- **Sources (Inputs):**
  - id_1779346047778 (连续亏损窗口分析)
- Position: (1100,250)
- OutputDsName: ads_单店利润健康
- ParentDirId: v2b6bde3d41444cfd9e6d7ef
- ParentDirName: 马甲的demo-0523
- DataSourceDsId: p39cc9d0866ac442bb777c63
- DataSourceCreated: true
- DirPath: 根目录 > 马甲的demo-0523
- 等价SQL:
```sql
SELECT * FROM input1
```


---

## 血缘关系

### 上游资源 (2)
- **param_利润健康阈值** (DATA_SET_FILE)
  - ID: l9312c8ef7ec14877889f06b
- **dws_单店利润月汇总** (DATA_SET_ETL)
  - ID: l6ee75fc812be413583215e4

### 下游资源 (1)
- **ads_单店利润健康** (DATA_SET_ETL)
  - ID: p39cc9d0866ac442bb777c63
