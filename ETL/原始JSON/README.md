# 原始 ETL JSON 的版本边界

本目录是 2026-06-24 从观远 BI workshop 实例导出的历史快照，用于审计原节点 DAG、字段 ID 和平台结构。

它仍保留 v1.4.0 的历史 SQL，**不包含 v1.4.1 对归因、留存、利润、回本、SCD2 与统一快照的修复**，因此不能当作 v1.4.1 可直接导入包。当前校正口径以：

- `ETL/逻辑SQL/`
- `ETL/公共口径/`
- `tests/test_business_contracts.py`

为准。资源 ID 也是原 workshop 实例私有值，迁移时必须重新生成。
