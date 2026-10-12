# 从字段契约生成建表语句

这四份 DDL 覆盖 55 张逻辑表、866 列。字段顺序、中英名、类型和粒度都来自 [`清单/字段契约.csv`](../清单/字段契约.csv)。默认中文名与样本、ETL 一致；生产环境推荐用英文名，先完成字段映射和目标引擎验证。

**Spark 3.4.4 自带的 SessionCatalog 拒绝中文持久表名，即使使用反引号。** 已提交的中文 `spark.sql` 保留为命名映射参考并通过解析；真正建表请生成 `--naming en` 版本。独立 Spark 测试实际执行英文 DDL，以契约逐列映射中文 CSV 与派生表。原 ETL 的中文临时视图可以运行，不需要换 catalog。

```sh
python tools/build_field_contract.py
python tools/build_field_contract.py --check
python tools/build_field_contract.py --spark-report work/spark-report/run-report.json
python tools/build_ddl.py
python tools/build_ddl.py --check
python tools/build_ddl.py --naming en --out work/ddl-en
python tools/build_ddl.py --naming en --out work/ddl-en --check
```

四份已提交的 SQL 是默认中文命名的生成物，不手改。英文翻译和自然粒度的可审阅来源是 [`字段契约来源.json`](../清单/字段契约来源.json)；同一中文字段跨表使用同一英文名，497 个不同字段都有唯一的 snake_case 英文名，表名保留分层前缀，均不超过 63 字符。

基础表以历史结构定义为类型来源。历史导出缺失的新列采用 `tools/build_mini_world.py:field_types` 的明确补充规则，并逐列标记为“历史导出未包含”。历史元数据把 `dim_会员主档.手机号` 写成 LONG，但当前样本必须保持 `156****0925` 这样的打码文本，因此已裁定为 STRING。历史原始 JSON 和正文不改写。

25 张派生表采用 [`Spark类型证据.json`](../清单/Spark类型证据.json) 的实际 schema，包含 DECIMAL(38,16)、BOOLEAN、INTEGER 等运行类型；不再依靠字段名字猜类型。证据来自 Spark 3.4.4 本地模式、合成小数据执行，记录当时 HEAD、`worktree_dirty=true` 和每个文档节点 SQL 原文的 SHA-256；报告的 as_of 确定执行时唯一允许的日期替换。该 HEAD 不代表未提交修改已在 Git 中；生成器逐节点比对当前 SQL 指纹，公共 SQL 则比对文件原始字节，SQL 变化后必须先重跑，再用 `--spark-report` 更新类型证据。提交后的 CI 报告提供对应提交的验证证据。与历史字段类型不同的列列在 [`类型冲突.csv`](类型冲突.csv)，派生表以实际执行类型为准。

| 契约类型 | Spark | MySQL | PostgreSQL | ClickHouse |
|---|---|---|---|---|
| STRING | STRING | TEXT | TEXT | String |
| LONG | BIGINT | BIGINT | BIGINT | Int64 |
| INTEGER | INT | INT | INTEGER | Int32 |
| DOUBLE | DOUBLE | DOUBLE | DOUBLE PRECISION | Float64 |
| BOOLEAN | BOOLEAN | BOOLEAN | BOOLEAN | Bool |
| DATE | DATE | DATE | DATE | Date |
| TIMESTAMP | TIMESTAMP | DATETIME | TIMESTAMP | DateTime('Asia/Shanghai') |
| DECIMAL(p,s) | DECIMAL(p,s) | DECIMAL(p,s) | DECIMAL(p,s) | Decimal(p,s) |

源类型是 DOUBLE 的金额列照实映射，不在生成时偷偷改成定点数。生产金额通常建议按业务精度改为 DECIMAL(18,2)，但需先确认最大值、舍入和汇总精度；已经由 Spark 算出的 DECIMAL(38,16) 也保留原精度。MySQL BOOLEAN 实际按该引擎的布尔别名处理。时间字段是无时区的业务时间，Spark 测试使用 Asia/Shanghai，跨引擎载入需保持一致的时间约定。

MySQL 使用 InnoDB 与 utf8mb4；中文表名即使加反引号，也仍受 `lower_case_table_names`、文件系统大小写和运维工具支持影响。英文命名更适合跨系统部署。PostgreSQL 使用双引号并通过 `COMMENT ON COLUMN` 写中文说明，其余方言在列定义中写 COMMENT。

ClickHouse 使用 MergeTree，ORDER BY 取契约中的自然粒度键，可空列写成 Nullable。实际输出存在可空分组键，例如导购汇总的周起始、成本结构的门店类型；对这些表设置 `allow_nullable_key=1`。这只是存储与排序配置，不代表去重或非空主键约束。Spark、MySQL、PostgreSQL 同样不自动把粒度键宣称为数据库主键。粒度在单个 as_of_date 批次内校验；若生产表保留多个快照，应单独设计快照存储键。

建表成功也不代表 ETL 能跨引擎执行。Spark/MySQL 的 DATEDIFF 使用“结束、开始”，PostgreSQL 常用日期相减，ClickHouse 的 dateDiff 使用“单位、开始、结束”；日期截断和格式化函数也不同。字符串拼接在 Spark/MySQL/ClickHouse 常用 CONCAT，PostgreSQL 可用 CONCAT 或 `||`。移植时须逐段调整原始 Spark SQL 并验证 NULL、时间窗和精度语义。

轻量测试解析四种方言的中文与英文 DDL，并核对原样本粒度唯一性。独立 Spark 测试真实创建英文命名的 55 张表，用声明类型严格读取 30 张微型宇宙基础表，载入 25 张当场执行的派生表，核对类型、粒度唯一性与每一行的完整读回值。可空分组键保留当前 SQL 结果，不由 DDL 改口径。原始输入中的字面量 `'null'` 保留原值；会员统计按作者确认的缺失身份规则排除，不影响订单总销售。Spark 的 Parquet 表读取会放宽字段 nullable 元信息，非空输出信息保留在契约中；它不替代生产约束验证。MySQL、PostgreSQL、ClickHouse 仅做语法解析，尚未在对应服务器上执行。

```sh
REQUIRE_SPARK=1 python -m unittest tests.test_spark_ddl -v
```

运行报告包括 `ddl-readback-report.json` 与 `ddl-pipeline/run-report.json`。7 张无 ETL 消费的基础表在契约中标为“无（参考结构）”，仍参与建表、载入和读回。
