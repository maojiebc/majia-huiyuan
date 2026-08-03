#!/usr/bin/env python3
"""Build (or verify) the tracked, text-only SkillHub distribution for majia-huiyuan.

Usage
-----
  python3 tools/build_skillhub_bundle.py           # rebuild skillhub/
  python3 tools/build_skillhub_bundle.py --check   # exit 1 if skillhub/ is out of sync
"""

from __future__ import annotations

import argparse
import filecmp
import json
import shutil
import tempfile
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

# Anchor → replacement pairs for adapt_skill_md().
# If any anchor is not found the build fails loudly instead of silently skipping.
_GENERATED_MD = """\
> ⚠️ **此目录为自动生成产物，请勿手动修改。**
>
> 由 `tools/build_skillhub_bundle.py` 从仓库根目录构建生成。
> 直接修改此处的文件会在下次重建时被覆盖。
>
> 如需修改内容，请：
> 1. 编辑根目录对应的源文件
> 2. 运行 `python3 tools/build_skillhub_bundle.py` 重建
> 3. 用 `python3 tools/build_skillhub_bundle.py --check` 验证根目录与此目录保持同步
"""

_SKILL_MD_PATCHES: list[tuple[str, str]] = [
    (
        "## 三大资产（都在本 skill 目录内）",
        "## 三大资产\n\n"
        "> SkillHub 为文本精简包：保留结构定义、ETL 逻辑、看板文档、公式库与方法论正文；"
        "数据样本、原始 JSON 和图片请从 GitHub 完整版读取。",
    ),
    (
        "`数据集/数据样本/*.csv` 表头 + `数据集/结构定义/*.md` 的类型信息推 schema",
        "`数据集/结构定义/*.md` 的字段与类型信息推 schema；"
        "需要取值样本时读取 GitHub 完整版的 `数据集/数据样本/*.csv`",
    ),
]


def copy_file(source_root: Path, output_root: Path, relative: str) -> None:
    source = source_root / relative
    target = output_root / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, target)


def copy_text_tree(source_root: Path, output_root: Path, relative: str) -> None:
    source = source_root / relative
    for path in sorted(source.rglob("*")):
        if path.is_file() and not path.name.startswith("."):
            copy_file(source_root, output_root, str(path.relative_to(source_root)))


def adapt_skill_md(output_root: Path) -> None:
    """Apply known patches to SKILL.md inside the distribution.

    Raises RuntimeError if any anchor string is missing — prevents silent
    drift when the source file is edited without updating this script.
    """
    path = output_root / "SKILL.md"
    text = path.read_text(encoding="utf-8")
    for anchor, replacement in _SKILL_MD_PATCHES:
        if anchor not in text:
            raise RuntimeError(
                f"adapt_skill_md: anchor not found in skillhub SKILL.md — "
                f"update _SKILL_MD_PATCHES to match the current source.\n"
                f"Missing anchor: {anchor!r}"
            )
        text = text.replace(anchor, replacement, 1)
    path.write_text(text, encoding="utf-8")


def normalize_line_endings(output_root: Path) -> None:
    for path in output_root.rglob("*"):
        if path.is_file():
            path.write_bytes(path.read_bytes().replace(b"\r\n", b"\n"))


def build(output_root: Path) -> dict:
    """Build the distribution into *output_root* (creates or replaces it)."""
    if output_root.exists():
        shutil.rmtree(output_root)
    output_root.mkdir(parents=True)

    for relative in ROOT_FILES:
        copy_file(ROOT, output_root, relative)
    copy_file(ROOT, output_root, "LICENSE")
    (output_root / "LICENSE").rename(output_root / "LICENSE.md")

    for relative in DIRECTORIES:
        copy_text_tree(ROOT, output_root, relative)

    copy_file(ROOT, output_root, "分享/区域运营的一天/README.md")
    adapt_skill_md(output_root)

    # Write the "do not hand-edit" marker last so it survives normalize.
    (output_root / "GENERATED.md").write_text(_GENERATED_MD, encoding="utf-8")

    normalize_line_endings(output_root)

    files = [path for path in output_root.rglob("*") if path.is_file()]
    return {
        "output": str(output_root),
        "fileCount": len(files),
        "bytes": sum(path.stat().st_size for path in files),
    }


def check_drift() -> bool:
    """Return True if skillhub/ matches a fresh build, False if drifted."""
    with tempfile.TemporaryDirectory() as tmp:
        fresh = Path(tmp) / "majia-huiyuan"
        build(fresh)
        # Compare every file in the fresh build against the committed copy.
        diffs: list[str] = []
        for fresh_file in sorted(fresh.rglob("*")):
            if not fresh_file.is_file():
                continue
            rel = fresh_file.relative_to(fresh)
            committed = OUTPUT / rel
            if not committed.exists():
                diffs.append(f"  MISSING in skillhub/: {rel}")
            elif not filecmp.cmp(fresh_file, committed, shallow=False):
                diffs.append(f"  CHANGED: {rel}")
        # Also flag files present in skillhub/ but not in fresh build.
        if OUTPUT.exists():
            for committed_file in sorted(OUTPUT.rglob("*")):
                if not committed_file.is_file():
                    continue
                rel = committed_file.relative_to(OUTPUT)
                if not (fresh / rel).exists():
                    diffs.append(f"  EXTRA in skillhub/ (stale): {rel}")
        if diffs:
            print("skillhub/ is out of sync with root directory:")
            for line in diffs:
                print(line)
            print("\nRun `python3 tools/build_skillhub_bundle.py` to rebuild.")
            return False
        return True


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="verify skillhub/ matches a fresh build; exit 1 if drifted",
    )
    args = parser.parse_args()

    if args.check:
        ok = check_drift()
        if ok:
            print("skillhub/ is in sync. ✓")
        return 0 if ok else 1

    payload = build(OUTPUT)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
