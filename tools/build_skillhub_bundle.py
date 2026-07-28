#!/usr/bin/env python3
"""Build the tracked, text-only SkillHub distribution for majia-huiyuan."""

from __future__ import annotations

import json
import shutil
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "skillhub" / "majia-huiyuan"

ROOT_FILES = (
    "SKILL.md",
    "README.md",
    "README.en.md",
    "AGENTS.md",
    "llms.txt",
)

DIRECTORIES = (
    "公式库",
    "清单",
    "数据集/结构定义",
    "ETL/逻辑SQL",
    "看板/页面文档",
)


def copy_file(relative: str) -> None:
    source = ROOT / relative
    target = OUTPUT / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, target)


def copy_text_tree(relative: str) -> None:
    source = ROOT / relative
    for path in sorted(source.rglob("*")):
        if path.is_file() and not path.name.startswith("."):
            copy_file(str(path.relative_to(ROOT)))


def adapt_skill_md() -> None:
    path = OUTPUT / "SKILL.md"
    text = path.read_text(encoding="utf-8")
    text = text.replace(
        "## 三大资产（都在本 skill 目录内）",
        "## 三大资产\n\n"
        "> SkillHub 为文本精简包：保留结构定义、ETL 逻辑、看板文档、公式库与方法论正文；"
        "数据样本、原始 JSON 和图片请从 GitHub 完整版读取。",
    )
    text = text.replace(
        "`数据集/数据样本/*.csv` 表头 + `数据集/结构定义/*.md` 的类型信息推 schema",
        "`数据集/结构定义/*.md` 的字段与类型信息推 schema；需要取值样本时读取 GitHub 完整版的 `数据集/数据样本/*.csv`",
    )
    path.write_text(text, encoding="utf-8")


def normalize_line_endings() -> None:
    for path in OUTPUT.rglob("*"):
        if path.is_file():
            path.write_bytes(path.read_bytes().replace(b"\r\n", b"\n"))


def main() -> int:
    if OUTPUT.exists():
        shutil.rmtree(OUTPUT)
    OUTPUT.mkdir(parents=True)

    for relative in ROOT_FILES:
        copy_file(relative)
    copy_file("LICENSE")
    (OUTPUT / "LICENSE").rename(OUTPUT / "LICENSE.md")

    for relative in DIRECTORIES:
        copy_text_tree(relative)

    # Keep the method narrative without its embedded raster assets.
    copy_file("分享/区域运营的一天/README.md")
    adapt_skill_md()
    normalize_line_endings()

    files = [path for path in OUTPUT.rglob("*") if path.is_file()]
    payload = {
        "output": str(OUTPUT),
        "fileCount": len(files),
        "bytes": sum(path.stat().st_size for path in files),
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
