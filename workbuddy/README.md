# WorkBuddy 专家发布适配层

这里仅保存 WorkBuddy 特有的市场字段、Agent 外壳与审核头像。会员数据顾问的知识正文仍以仓库根目录为唯一真源；构建器复用现有 SkillHub 文本精简构建逻辑，生成一个自包含的 `majia-huiyuan` 技能目录。

生成可上传 ZIP：

```bash
python3 tools/build_workbuddy_bundle.py
```

核对本地生成物是否仍与唯一真源一致：

```bash
python3 tools/build_workbuddy_bundle.py --check
```

默认产物为 `dist/workbuddy/majia-huiyuan.zip`。ZIP 只保留 WorkBuddy 运行需要的专家配置、头像、Agent 定义和文本知识，不包含 `.git`、缓存、原始 JSON、模拟数据样本或仓库构建工具。

平台实际解析还会检查专家花名长度；当前测试固定要求中文花名不超过 15 个字符。头像固定为 512×512 PNG 且不超过 500KB。上传成功只代表包已解析，审核通过与正式上架仍需在开放平台分别核验。
