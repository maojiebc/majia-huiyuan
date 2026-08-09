我正在查看一个数据集的详细配置信息：

## 基本信息

- 数据集ID: r9024c50adcdb45c397cde0a
- 数据集名称: ads_高层经营驾驶舱
- 显示类型: DATAFLOW
- UniformResourceType: DATA_SET_ETL
- 结构版本: v1.4.1

> v1.4.1 将触达关联销售按订单发生日汇总，并强制一笔订单只归属于下单前最近一次有效触达。没有对照组，因此字段使用“触达后关联”，不使用“私域贡献/增量”命名。

## 字段结构概览

- **总字段数:** 15
- **维度字段:** 3
- **度量字段:** 12

### 字段列表

| 字段 | 类型 | 属性 | 口径 |
|---|---|---|---|
| 业务日期 | DATE | DIM | 订单发生日 |
| 总销售 | DOUBLE | METRIC | 当日已完成订单实付金额 |
| 会员销售 | DOUBLE | METRIC | 当日有会员ID的已完成订单实付金额 |
| 到店销售 | DOUBLE | METRIC | 当日到店已完成订单实付金额 |
| 总订单数 | LONG | METRIC | 当日已完成订单数 |
| 活跃会员数 | LONG | METRIC | 当日去重下单会员数 |
| 新增会员数 | LONG | METRIC | 当日产生会员首单的去重会员数 |
| 触达后关联订单数 | LONG | METRIC | 当日被唯一归因到有效触达的订单数 |
| 触达后关联销售 | DOUBLE | METRIC | 当日被唯一归因订单的实付金额；不是增量收入 |
| 会员销售占比 | DOUBLE | METRIC | 会员销售 / 总销售 |
| 到店销售额占比 | DOUBLE | METRIC | 到店销售 / 总销售；修正原“到店订单占比”误名 |
| 触达后关联销售占比 | DOUBLE | METRIC | 触达后关联销售 / 同日总销售 |
| 归因窗口天数 | LONG | METRIC | 当前为 7，表示触达后 0–7 天 |
| 归因规则 | STRING | DIM | 下单前最近一次有效触达；每订单唯一 |
| 数据快照日期 | DATE | DIM | ETL 必填参数 `as_of_date` |

## Malloy 数据源定义

```malloy
source: `ads_高层经营驾驶舱` is table('r9024c50adcdb45c397cde0a') extend {
  // 业务日期 (DATE)
  // 总销售 (DOUBLE)
  // 会员销售 (DOUBLE)
  // 到店销售 (DOUBLE)
  // 总订单数 (LONG)
  // 活跃会员数 (LONG)
  // 新增会员数 (LONG)
  // 触达后关联订单数 (LONG)
  // 触达后关联销售 (DOUBLE)
  // 会员销售占比 (DOUBLE)
  // 到店销售额占比 (DOUBLE)
  // 触达后关联销售占比 (DOUBLE)
  // 归因窗口天数 (LONG)
  // 归因规则 (STRING)
  // 数据快照日期 (DATE)
}
```

## 血缘关系

- 上游 ETL：`ETL/逻辑SQL/ads_高层经营驾驶舱.md`
- 上游数据：`dwd_订单`、`dwd_会员触达`
- 下游看板：`01-高层经营驾驶舱`
