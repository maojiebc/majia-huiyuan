我正在查看一个数据集的详细配置信息：

## 基本信息

- 数据集ID: u37d100614fbd4abe89f7731
- 数据集名称: dws_私域转化漏斗
- 显示类型: DATAFLOW
- UniformResourceType: DATA_SET_ETL
- 结构版本: v1.4.1

> v1.4.1 接入真实订单，并以“下单前最近一次有效触达”建立每订单唯一归因桥。触达指标按触达发生日、关联订单指标按订单发生日进入同一个自然日面板；无对照组时不得解释为增量。

## 字段结构概览

- **总字段数:** 15
- **维度字段:** 7
- **度量字段:** 8

### 字段列表

| 字段 | 类型 | 属性 | 口径 |
|---|---|---|---|
| 活动ID | STRING | DIM | 活动主键 |
| 活动名称 | STRING | DIM | 活动主档名称 |
| 活动类型 | STRING | DIM | 活动主档类型 |
| 业务日期 | DATE | DIM | 指标实际发生日；触达指标取触达日，关联订单指标取订单发生日 |
| 触达渠道 | STRING | DIM | 被唯一归因触达的渠道 |
| 触达人次 | LONG | METRIC | 当日有效触达记录数 |
| 查看人次 | LONG | METRIC | 当日已查看触达记录数 |
| 触达人数 | LONG | METRIC | 当日去重触达会员数 |
| 打开率 | DOUBLE | METRIC | 查看人次 / 触达人次 |
| 触达后关联下单人数 | LONG | METRIC | 当日下单且被唯一归因到该活动渠道的会员数 |
| 触达后关联订单数 | LONG | METRIC | 当日下单且被唯一归因到该活动渠道的订单数 |
| 触达后关联GMV | DOUBLE | METRIC | 当日唯一归因订单实付金额；不是增量 GMV |
| 归因窗口天数 | LONG | METRIC | 当前为 7，表示触达后 0–7 天 |
| 归因规则 | STRING | DIM | 下单前最近一次有效触达；每订单唯一 |
| 数据快照日期 | DATE | DIM | ETL 必填参数 `as_of_date` |

## Malloy 数据源定义

```malloy
source: `dws_私域转化漏斗` is table('u37d100614fbd4abe89f7731') extend {
  // 活动ID (STRING)
  // 活动名称 (STRING)
  // 活动类型 (STRING)
  // 业务日期 (DATE)
  // 触达渠道 (STRING)
  // 触达人次 (LONG)
  // 查看人次 (LONG)
  // 触达人数 (LONG)
  // 打开率 (DOUBLE)
  // 触达后关联下单人数 (LONG)
  // 触达后关联订单数 (LONG)
  // 触达后关联GMV (DOUBLE)
  // 归因窗口天数 (LONG)
  // 归因规则 (STRING)
  // 数据快照日期 (DATE)
}
```

## 血缘关系

- 上游 ETL：`ETL/逻辑SQL/etl_dws_私域转化漏斗 (10节点·F+C+G+C+G+J+C).md`
- 上游数据：`dwd_会员触达`、`dwd_订单`、`dim_活动主档`
