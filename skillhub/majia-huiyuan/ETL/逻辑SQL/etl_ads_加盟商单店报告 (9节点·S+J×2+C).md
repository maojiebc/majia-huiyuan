你是一个ETL专家, 正在查看如下ETL的定义(注意: 这里的所有节点都会运行在 Apache Spark 3.4上, 所有的SQL语法都是Spark的语法, 当用户让优化性能时, 不要给出建索引等通用的建议, 因为Spark不能建立索引, 并且这个ETL只能使用Spark SQL(不能使用DataFrame API), 主要给出可以"通过优化ETL节点的写法来优化性能"这种优化建议):

## 基本信息
- UniformResourceType: DATA_PROCESS_ETL
---
## ETL 流程摘要

- **总节点数:** 9
- **节点类型分布:**
  - CALCULATOR: 1
  - INPUT_DATASET: 4
  - JOIN_DATA: 2
  - OUTPUT_DATASET: 1
  - SQL_SCRIPT: 1
- **数据输入源:**
  - l6ee75fc812be413583215e4 (dws_单店利润月汇总)
  - w55d0570b98a143579807416 (dwd_加盟合同明细)
  - e620121168c3447c3abe4948 (dim_加盟商主档)
  - vf66c6e915ad048c49cbcf25 (dws_加盟回本测算)
- **数据输出目标:**
  - ads_加盟商单店报告 (目录: 马甲的demo-0523)
- **时间口径:** 继承 `dws_单店利润月汇总.数据快照日期`，与同批次 `as_of_date` 一致
---
## ETL 节点详细信息


### 节点1
- Id: id_1779346227920
- Name: 同侪定位+改进建议+总部支持
- Type: CALCULATOR
- **Sources (Inputs):**
  - id_1779346227919 (关联回本测算)

- **Used By (Outputs):**
  - id_1779346227921 (ads_加盟商单店报告)
- Position: (1043,64)
- FormulaNames:
  - 本店位置标签
  - 可改进项
  - 总部本月支持
- 等价SQL:
```sql
SELECT
  `门店ID`, `门店名称`, `省份`, `城市`, `城市层级`, `门店类型`, `商圈`,
  `加盟商ID`, `加盟商名称`, `加盟商类型`, `合作状态`, `信用等级`, `签约日期`,
  `月份`, `月营收`, `堂食营收`, `外卖营收`, `订单数`,
  `毛利`, `店面贡献利润`, `单店净利润`, `毛利率`, `店面贡献利润率`,
  `堂食占比`, `外卖占比`, `人工占比`, `房租占比`, `客单价`,
  `同侪门店数`, `城市同店型_营收_P25`, `城市同店型_营收_中位数`, `城市同店型_营收_P75`,
  `城市同店型_利润率_P25`, `城市同店型_利润率_中位数`, `城市同店型_利润率_P75`,
  `城市同店型_堂食占比_中位数`, `营收_对中位数比`, `利润率_对中位数差`, `堂食占比_对中位数差`,
  `总投资额`, `累计店面贡献利润`, `累计回本率`, `预计完整回本月数`, `已开业月数`,
  `投资起始日`, `剩余回本月数`, `回本状态`, `预计完整回本日期`,
  `招商承诺回本月数`, `回本偏离度`, `回本风险等级`, `标杆门店标志`,
  case when `营收_对中位数比` >= 1.5 then '顶部 Top 25%' when `营收_对中位数比` >= 1.1 then '中上 P50-P75' when `营收_对中位数比` >= 0.9 then '中位 ±10%' when `营收_对中位数比` >= 0.7 then '中下 P25-P50' else '尾部 Bottom 25%' end AS `本店位置标签`,
  case when `店面贡献利润` < 0 then '亏损中, 建议联系督导专项支持' when `营收_对中位数比` < 0.7 then '营收低于同侪 30%+, 建议会员拉新+私域引流' when `堂食占比_对中位数差` < -0.10 then '堂食占比低于同侪 10pp+, 建议堂食提振专题' when `人工占比` > 0.25 then '人工占比偏高, 建议优化排班' when `房租占比` > 0.30 then '房租压力大, 建议提升日均营收' when `利润率_对中位数差` >= 0.05 then '同侪标杆, 持续优秀!' else '运营正常, 继续保持' end AS `可改进项`,
  case when `合作状态` = '正常合作' then '提供品牌活动 + 私域内容 + 督导巡店' when `合作状态` = '续约预警' then '加强督导 + 专项营销补贴' when `合作状态` = '关注名单' then '高频督导 + 定制运营方案' else '专项处理' end AS `总部本月支持`,
  `数据快照日期`
FROM input1
```


### 节点2
- Id: id_1779346227921
- Name: ads_加盟商单店报告
- Type: OUTPUT_DATASET
- **Sources (Inputs):**
  - id_1779346227920 (同侪定位+改进建议+总部支持)
- Position: (1247,64)
- OutputDsName: ads_加盟商单店报告
- ParentDirId: v2b6bde3d41444cfd9e6d7ef
- ParentDirName: 马甲的demo-0523
- DataSourceDsId: n7e1bd96a3dcf48e88f18022
- DataSourceCreated: true
- DirPath: 根目录 > 马甲的demo-0523
- 等价SQL:
```sql
SELECT * FROM input1
```


### 节点3
- Id: id_1779346227918
- Name: 关联加盟商信息
- Type: JOIN_DATA
- **Sources (Inputs):**
  - id_1779346227917 (franchise_profit+同城同店型同侪)
  - id_1779346227915 (dim_加盟商主档)

- **Used By (Outputs):**
  - id_1779346227919 (关联回本测算)
- Position: (635,64)
- 等价SQL:
```sql
SELECT
  f.`门店ID`, f.`门店名称`, f.`省份`, f.`城市`, f.`城市层级`, f.`门店类型`, f.`商圈`,
  f.`加盟商ID`, d.`加盟商名称`, d.`加盟商类型`, d.`合作状态`, d.`信用等级`, f.`签约日期`,
  f.`月份`, f.`月营收`, f.`堂食营收`, f.`外卖营收`, f.`订单数`,
  f.`毛利`, f.`店面贡献利润`, f.`单店净利润`, f.`毛利率`, f.`店面贡献利润率`,
  f.`堂食占比`, f.`外卖占比`, f.`人工占比`, f.`房租占比`, f.`客单价`,
  f.`同侪门店数`, f.`城市同店型_营收_P25`, f.`城市同店型_营收_中位数`, f.`城市同店型_营收_P75`,
  f.`城市同店型_利润率_P25`, f.`城市同店型_利润率_中位数`, f.`城市同店型_利润率_P75`,
  f.`城市同店型_堂食占比_中位数`, f.`营收_对中位数比`, f.`利润率_对中位数差`, f.`堂食占比_对中位数差`,
  f.`数据快照日期`
FROM input1 f
LEFT JOIN input2 d ON f.`加盟商ID` = d.`加盟商ID`
```


### 节点4
- Id: id_1779346227919
- Name: 关联回本测算
- Type: JOIN_DATA
- **Sources (Inputs):**
  - id_1779346227918 (关联加盟商信息)
  - id_1779346227916 (dws_加盟回本测算)

- **Used By (Outputs):**
  - id_1779346227920 (同侪定位+改进建议+总部支持)
- Position: (839,64)
- 等价SQL:
```sql
SELECT
  f.`门店ID`, f.`门店名称`, f.`省份`, f.`城市`, f.`城市层级`, f.`门店类型`, f.`商圈`,
  f.`加盟商ID`, f.`加盟商名称`, f.`加盟商类型`, f.`合作状态`, f.`信用等级`, f.`签约日期`,
  f.`月份`, f.`月营收`, f.`堂食营收`, f.`外卖营收`, f.`订单数`,
  f.`毛利`, f.`店面贡献利润`, f.`单店净利润`, f.`毛利率`, f.`店面贡献利润率`,
  f.`堂食占比`, f.`外卖占比`, f.`人工占比`, f.`房租占比`, f.`客单价`,
  f.`同侪门店数`, f.`城市同店型_营收_P25`, f.`城市同店型_营收_中位数`, f.`城市同店型_营收_P75`,
  f.`城市同店型_利润率_P25`, f.`城市同店型_利润率_中位数`, f.`城市同店型_利润率_P75`,
  f.`城市同店型_堂食占比_中位数`, f.`营收_对中位数比`, f.`利润率_对中位数差`, f.`堂食占比_对中位数差`,
  r.`总投资额`, r.`累计店面贡献利润`, r.`累计回本率`, r.`预计完整回本月数`, r.`已开业月数`,
  r.`投资起始日`, r.`剩余回本月数`, r.`回本状态`, r.`预计完整回本日期`,
  r.`招商承诺回本月数`, r.`回本偏离度`, r.`回本风险等级`, r.`标杆门店标志`,
  f.`数据快照日期`
FROM input1 f
LEFT JOIN input2 r
  ON f.`门店ID` = r.`门店ID`
 AND f.`数据快照日期` = r.`数据快照日期`
```


### 节点5
- Id: id_1779346227913
- Name: dws_单店利润月汇总
- Type: INPUT_DATASET
- **Used By (Outputs):**
  - id_1779346227917 (franchise_profit+同城同店型同侪)
- Position: (227,64)
- InputDsId: l6ee75fc812be413583215e4
- DisplayType: DATAFLOW
- PreviewScope: ALL
- 等价SQL:
```sql
SELECT * FROM input
```


### 节点6
- Id: id_1779346227914
- Name: dwd_加盟合同明细
- Type: INPUT_DATASET
- **Used By (Outputs):**
  - id_1779346227917 (franchise_profit+同城同店型同侪)
- Position: (227,232)
- InputDsId: w55d0570b98a143579807416
- DisplayType: CSV
- PreviewScope: ALL
- 等价SQL:
```sql
SELECT * FROM input
```


### 节点7
- Id: id_1779346227915
- Name: dim_加盟商主档
- Type: INPUT_DATASET
- **Used By (Outputs):**
  - id_1779346227918 (关联加盟商信息)
- Position: (431,232)
- InputDsId: e620121168c3447c3abe4948
- DisplayType: CSV
- PreviewScope: ALL
- 等价SQL:
```sql
SELECT * FROM input
```


### 节点8
- Id: id_1779346227916
- Name: dws_加盟回本测算
- Type: INPUT_DATASET
- **Used By (Outputs):**
  - id_1779346227919 (关联回本测算)
- Position: (635,232)
- InputDsId: vf66c6e915ad048c49cbcf25
- DisplayType: DATAFLOW
- PreviewScope: ALL
- 等价SQL:
```sql
SELECT * FROM input
```


### 节点9
- Id: id_1779346227917
- Name: franchise_profit+同城同店型同侪
- Type: SQL_SCRIPT
- **Sources (Inputs):**
  - id_1779346227913 (dws_单店利润月汇总)
  - id_1779346227914 (dwd_加盟合同明细)

- **Used By (Outputs):**
  - id_1779346227918 (关联加盟商信息)
- Position: (431,64)
- SqlScript:
```sql
WITH franchise_profit_ranked AS (
  SELECT
    p.*, c.`加盟商ID`, c.`签约日期`,
    ROW_NUMBER() OVER (
      PARTITION BY p.`门店ID`, p.`月份`
      ORDER BY c.`签约日期` DESC, c.`合同ID` DESC
    ) AS contract_rn
  FROM input1 p
  JOIN input2 c
    ON p.`门店ID` = c.`门店ID`
   AND CAST(CONCAT(p.`月份`, '-01') AS DATE) BETWEEN c.`签约日期`
                                                   AND COALESCE(c.`到期日`, DATE '9999-12-31')
   AND c.`合同状态` <> '已作废'
),
franchise_profit AS (
  SELECT * FROM franchise_profit_ranked WHERE contract_rn = 1
),
peer_baseline AS (
  SELECT
    `城市层级`, `门店类型`, `月份`,
    PERCENTILE_APPROX(`月营收`, 0.25) AS `城市同店型_营收_P25`,
    PERCENTILE_APPROX(`月营收`, 0.50) AS `城市同店型_营收_中位数`,
    PERCENTILE_APPROX(`月营收`, 0.75) AS `城市同店型_营收_P75`,
    PERCENTILE_APPROX(`店面贡献利润率`, 0.25) AS `城市同店型_利润率_P25`,
    PERCENTILE_APPROX(`店面贡献利润率`, 0.50) AS `城市同店型_利润率_中位数`,
    PERCENTILE_APPROX(`店面贡献利润率`, 0.75) AS `城市同店型_利润率_P75`,
    PERCENTILE_APPROX(`堂食占比`, 0.50) AS `城市同店型_堂食占比_中位数`,
    COUNT(*) AS `同侪门店数`
  FROM franchise_profit
  GROUP BY `城市层级`, `门店类型`, `月份`
)
SELECT
  s.`门店ID`, s.`门店名称`, s.`省份`, s.`城市`, s.`城市层级`, s.`门店类型`, s.`商圈`,
  s.`加盟商ID`, s.`签约日期`, s.`数据快照日期`,
  s.`月份`, s.`月营收`, s.`堂食营收`, s.`外卖营收`, s.`订单数`,
  s.`毛利`, s.`店面贡献利润`, s.`单店净利润`,
  s.`毛利率`, s.`店面贡献利润率`, s.`堂食占比`, s.`外卖占比`,
  s.`人工占比`, s.`房租占比`, s.`客单价`,
  b.`同侪门店数`,
  b.`城市同店型_营收_P25`, b.`城市同店型_营收_中位数`, b.`城市同店型_营收_P75`,
  b.`城市同店型_利润率_P25`, b.`城市同店型_利润率_中位数`, b.`城市同店型_利润率_P75`,
  b.`城市同店型_堂食占比_中位数`,
  CASE WHEN b.`城市同店型_营收_中位数` > 0
       THEN s.`月营收` / b.`城市同店型_营收_中位数`
       ELSE 1 END AS `营收_对中位数比`,
  s.`店面贡献利润率` - b.`城市同店型_利润率_中位数` AS `利润率_对中位数差`,
  s.`堂食占比` - b.`城市同店型_堂食占比_中位数` AS `堂食占比_对中位数差`
FROM franchise_profit s
LEFT JOIN peer_baseline b ON s.`城市层级` = b.`城市层级` AND s.`门店类型` = b.`门店类型` AND s.`月份` = b.`月份`
```
- 等价SQL:
```sql
WITH franchise_profit_ranked AS (
  SELECT
    p.*, c.`加盟商ID`, c.`签约日期`,
    ROW_NUMBER() OVER (
      PARTITION BY p.`门店ID`, p.`月份`
      ORDER BY c.`签约日期` DESC, c.`合同ID` DESC
    ) AS contract_rn
  FROM input1 p
  JOIN input2 c
    ON p.`门店ID` = c.`门店ID`
   AND CAST(CONCAT(p.`月份`, '-01') AS DATE) BETWEEN c.`签约日期`
                                                   AND COALESCE(c.`到期日`, DATE '9999-12-31')
   AND c.`合同状态` <> '已作废'
),
franchise_profit AS (
  SELECT * FROM franchise_profit_ranked WHERE contract_rn = 1
),
peer_baseline AS (
  SELECT
    `城市层级`, `门店类型`, `月份`,
    PERCENTILE_APPROX(`月营收`, 0.25) AS `城市同店型_营收_P25`,
    PERCENTILE_APPROX(`月营收`, 0.50) AS `城市同店型_营收_中位数`,
    PERCENTILE_APPROX(`月营收`, 0.75) AS `城市同店型_营收_P75`,
    PERCENTILE_APPROX(`店面贡献利润率`, 0.25) AS `城市同店型_利润率_P25`,
    PERCENTILE_APPROX(`店面贡献利润率`, 0.50) AS `城市同店型_利润率_中位数`,
    PERCENTILE_APPROX(`店面贡献利润率`, 0.75) AS `城市同店型_利润率_P75`,
    PERCENTILE_APPROX(`堂食占比`, 0.50) AS `城市同店型_堂食占比_中位数`,
    COUNT(*) AS `同侪门店数`
  FROM franchise_profit
  GROUP BY `城市层级`, `门店类型`, `月份`
)
SELECT
  s.`门店ID`, s.`门店名称`, s.`省份`, s.`城市`, s.`城市层级`, s.`门店类型`, s.`商圈`,
  s.`加盟商ID`, s.`签约日期`, s.`数据快照日期`,
  s.`月份`, s.`月营收`, s.`堂食营收`, s.`外卖营收`, s.`订单数`,
  s.`毛利`, s.`店面贡献利润`, s.`单店净利润`,
  s.`毛利率`, s.`店面贡献利润率`, s.`堂食占比`, s.`外卖占比`,
  s.`人工占比`, s.`房租占比`, s.`客单价`,
  b.`同侪门店数`,
  b.`城市同店型_营收_P25`, b.`城市同店型_营收_中位数`, b.`城市同店型_营收_P75`,
  b.`城市同店型_利润率_P25`, b.`城市同店型_利润率_中位数`, b.`城市同店型_利润率_P75`,
  b.`城市同店型_堂食占比_中位数`,
  CASE WHEN b.`城市同店型_营收_中位数` > 0
       THEN s.`月营收` / b.`城市同店型_营收_中位数`
       ELSE 1 END AS `营收_对中位数比`,
  s.`店面贡献利润率` - b.`城市同店型_利润率_中位数` AS `利润率_对中位数差`,
  s.`堂食占比` - b.`城市同店型_堂食占比_中位数` AS `堂食占比_对中位数差`
FROM franchise_profit s
LEFT JOIN peer_baseline b ON s.`城市层级` = b.`城市层级` AND s.`门店类型` = b.`门店类型` AND s.`月份` = b.`月份`
```


---

## 血缘关系

### 上游资源 (4)
- **dim_加盟商主档** (DATA_SET_FILE)
  - ID: e620121168c3447c3abe4948
- **dwd_加盟合同明细** (DATA_SET_FILE)
  - ID: w55d0570b98a143579807416
- **dws_单店利润月汇总** (DATA_SET_ETL)
  - ID: l6ee75fc812be413583215e4
- **dws_加盟回本测算** (DATA_SET_ETL)
  - ID: vf66c6e915ad048c49cbcf25

### 下游资源 (1)
- **ads_加盟商单店报告** (DATA_SET_ETL)
  - ID: n7e1bd96a3dcf48e88f18022
