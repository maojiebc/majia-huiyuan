我正在查看一个数据集的详细配置信息：

## 基本信息

- 数据集ID: h6c06660548ba4d8daaf2bd3
- 数据集名称: dws_券效益分析
- 显示类型: DATAFLOW
- UniformResourceType: DATA_SET_ETL
- 结构版本: v1.4.1

> v1.4.1 以 `券ID` 为券实例主键，通过实例记录的 `订单ID` 连接核销订单。统计粒度是 `券模板ID × 发放日期` 的发券同期群，不再把发放日汇总与核销日汇总做同日连接。

## 字段结构概览

- **总字段数:** 17
- **维度字段:** 8
- **度量字段:** 9

### 字段列表

| 字段 | 类型 | 属性 | 口径 |
|---|---|---|---|
| 券模板ID | STRING | DIM | 券模板主键 |
| 券名称 | STRING | DIM | 券模板名称 |
| 券类型 | STRING | DIM | 券模板类型 |
| 优惠形式 | STRING | DIM | 满减、折扣等 |
| 发放日期 | DATE | DIM | 发券同期群日期 |
| 发放数 | LONG | METRIC | 去重券实例数 |
| 核销数 | LONG | METRIC | 快照日前已核销的去重券实例数 |
| 核销率 | DOUBLE | METRIC | 核销数 / 发放数 |
| 已核销优惠成本 | DOUBLE | METRIC | 已核销券实例折扣金额合计 |
| 核销订单GMV | DOUBLE | METRIC | 券实例通过订单ID直连的已完成订单实付金额；同订单只归一次 |
| 核销订单数 | LONG | METRIC | 券实例直连且通过唯一归因的订单数 |
| 核销GMV成本比 | DOUBLE | METRIC | 核销订单GMV / 已核销优惠成本；描述性比值，不是 ROI |
| 增量GMV | DOUBLE | METRIC | 仅有对照组时可计算；当前为 NULL |
| 增量ROI | DOUBLE | METRIC | 仅有对照组及完整成本时可计算；当前为 NULL |
| 增量测算状态 | STRING | DIM | 当前为未接入对照组 |
| 订单归因规则 | STRING | DIM | 券实例订单ID直连；多券同单只归一次 |
| 数据快照日期 | DATE | DIM | ETL 必填参数 `as_of_date` |

## Malloy 数据源定义

```malloy
source: `dws_券效益分析` is table('h6c06660548ba4d8daaf2bd3') extend {
  // 券模板ID (STRING)
  // 券名称 (STRING)
  // 券类型 (STRING)
  // 优惠形式 (STRING)
  // 发放日期 (DATE)
  // 发放数 (LONG)
  // 核销数 (LONG)
  // 核销率 (DOUBLE)
  // 已核销优惠成本 (DOUBLE)
  // 核销订单GMV (DOUBLE)
  // 核销订单数 (LONG)
  // 核销GMV成本比 (DOUBLE)
  // 增量GMV (DOUBLE)
  // 增量ROI (DOUBLE)
  // 增量测算状态 (STRING)
  // 订单归因规则 (STRING)
  // 数据快照日期 (DATE)
}
```

## 血缘关系

- 上游 ETL：`ETL/逻辑SQL/etl_dws_券效益分析.md`
- 上游数据：`dwd_券事件`、`dwd_订单`、`dim_券模板`
