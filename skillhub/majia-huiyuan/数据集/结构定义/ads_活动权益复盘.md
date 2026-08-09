我正在查看一个数据集的详细配置信息：

## 基本信息

- 数据集ID: q2154240ed0334aec8883ae8
- 数据集名称: ads_活动权益复盘
- 显示类型: DATAFLOW
- UniformResourceType: DATA_SET_ETL
- 结构版本: v1.4.1

> v1.4.1 分开呈现券实例核销、触达后关联、活动参与后关联和实验增量四类结果。前三类不是自然增量；没有对照组与完整成本时，增量 GMV/ROI 必须为 NULL。

## 字段结构概览

- **总字段数:** 33
- **维度字段:** 10
- **度量字段:** 23

### 字段列表

| 字段 | 类型 | 属性 | 口径 |
|---|---|---|---|
| 活动ID | STRING | DIM | 活动主键 |
| 活动名称 | STRING | DIM | 活动名称 |
| 活动类型 | STRING | DIM | 活动类型 |
| 活动渠道 | STRING | DIM | 活动主渠道 |
| 开始日期 | DATE | DIM | 活动开始日期 |
| 结束日期 | DATE | DIM | 活动结束日期 |
| 预算 | LONG | METRIC | 活动主档预算；不代表完整实际成本 |
| 券发放数 | LONG | METRIC | 来源活动下的去重券实例数 |
| 券核销数 | LONG | METRIC | 快照日前已核销券实例数 |
| 已记录权益成本 | DOUBLE | METRIC | 已核销券实例折扣金额合计 |
| 核销订单数 | LONG | METRIC | 券实例通过订单ID直连的唯一已完成订单数 |
| 核销订单GMV | DOUBLE | METRIC | 核销订单实付金额；不是增量 GMV |
| 触达人数 | LONG | METRIC | 活动有效触达去重会员数 |
| 查看人数 | LONG | METRIC | 活动已查看触达去重会员数 |
| 活动参与人数 | LONG | METRIC | 结果为成功的活动参与去重会员数 |
| 触达后关联下单人数 | LONG | METRIC | 被最近一次有效触达唯一归因的下单会员数 |
| 触达后关联订单数 | LONG | METRIC | 被最近一次有效触达唯一归因的订单数 |
| 触达后关联GMV | DOUBLE | METRIC | 有限窗口关联订单实付金额；不是增量 GMV |
| 参与后关联下单人数 | LONG | METRIC | 被最近一次成功参与唯一归因的下单会员数 |
| 参与后关联订单数 | LONG | METRIC | 被最近一次成功参与唯一归因的订单数 |
| 参与后关联GMV | DOUBLE | METRIC | 有限窗口参与关联订单实付金额；不是增量 GMV |
| 券核销率 | DOUBLE | METRIC | 券核销数 / 券发放数 |
| 打开率 | DOUBLE | METRIC | 查看人数 / 触达人数 |
| 触达后关联下单率 | DOUBLE | METRIC | 触达后关联下单人数 / 触达人数 |
| 参与后关联下单率 | DOUBLE | METRIC | 参与后关联下单人数 / 活动参与人数 |
| 核销GMV成本比 | DOUBLE | METRIC | 核销订单GMV / 已记录权益成本；不是 ROI |
| 增量GMV | DOUBLE | METRIC | 仅对照实验或准实验可计算；当前为 NULL |
| 增量ROI | DOUBLE | METRIC | 仅增量GMV减完整成本后可计算；当前为 NULL |
| 增量测算状态 | STRING | DIM | 是否具备对照组及完整成本 |
| 归因窗口天数 | LONG | METRIC | 当前为 7，表示事件后 0–7 天 |
| 触达归因规则 | STRING | DIM | 下单前最近一次有效触达；每订单唯一 |
| 活动参与归因规则 | STRING | DIM | 下单前最近一次成功活动参与；每订单唯一 |
| 数据快照日期 | DATE | DIM | ETL 必填参数 `as_of_date` |

## Malloy 数据源定义

```malloy
source: `ads_活动权益复盘` is table('q2154240ed0334aec8883ae8') extend {
  // 活动ID (STRING)
  // 活动名称 (STRING)
  // 活动类型 (STRING)
  // 活动渠道 (STRING)
  // 开始日期 (DATE)
  // 结束日期 (DATE)
  // 预算 (LONG)
  // 券发放数 (LONG)
  // 券核销数 (LONG)
  // 已记录权益成本 (DOUBLE)
  // 核销订单数 (LONG)
  // 核销订单GMV (DOUBLE)
  // 触达人数 (LONG)
  // 查看人数 (LONG)
  // 活动参与人数 (LONG)
  // 触达后关联下单人数 (LONG)
  // 触达后关联订单数 (LONG)
  // 触达后关联GMV (DOUBLE)
  // 参与后关联下单人数 (LONG)
  // 参与后关联订单数 (LONG)
  // 参与后关联GMV (DOUBLE)
  // 券核销率 (DOUBLE)
  // 打开率 (DOUBLE)
  // 触达后关联下单率 (DOUBLE)
  // 参与后关联下单率 (DOUBLE)
  // 核销GMV成本比 (DOUBLE)
  // 增量GMV (DOUBLE)
  // 增量ROI (DOUBLE)
  // 增量测算状态 (STRING)
  // 归因窗口天数 (LONG)
  // 触达归因规则 (STRING)
  // 活动参与归因规则 (STRING)
  // 数据快照日期 (DATE)
}
```

## 血缘关系

- 上游 ETL：`ETL/逻辑SQL/etl_ads_活动权益复盘 (17节点·C+G+F+C+G×2+S+J×3+C).md`
- 上游数据：`dwd_券事件`、`dwd_会员触达`、`dwd_订单`、`dim_活动主档`、`dwd_活动参与`
- 下游看板：`05-活动权益复盘`
