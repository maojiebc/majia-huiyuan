#!/usr/bin/env python3
"""Build a self-contained WorkBuddy expert ZIP from repository sources."""
from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path
import shutil
import tempfile
from types import ModuleType
import zipfile


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "workbuddy"
DEFAULT_OUTPUT = ROOT / "dist/workbuddy/majia-huiyuan"
DEFAULT_ARCHIVE = ROOT / "dist/workbuddy/majia-huiyuan.zip"


def _load_skillhub_builder() -> ModuleType:
    path = ROOT / "tools/build_skillhub_bundle.py"
    spec = importlib.util.spec_from_file_location("huiyuan_skillhub_builder", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"无法加载 SkillHub 构建器：{path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _read_manifest() -> dict[str, object]:
    value = json.loads((SOURCE / "expert.json").read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise RuntimeError("workbuddy/expert.json 必须是 JSON 对象")
    root_manifest = json.loads((ROOT / "manifest.json").read_text(encoding="utf-8"))
    value["version"] = root_manifest["version"]
    return value


def _write_archive(output: Path, archive: Path) -> None:
    archive.parent.mkdir(parents=True, exist_ok=True)
    if archive.exists():
        archive.unlink()
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as target:
        for path in sorted(output.rglob("*")):
            if path.is_file():
                relative = Path(output.name) / path.relative_to(output)
                target.write(path, relative.as_posix())


def build(output: Path, archive: Path) -> dict[str, object]:
    manifest = _read_manifest()
    if output.exists():
        shutil.rmtree(output)
    output.mkdir(parents=True)

    shutil.copytree(SOURCE / "agents", output / "agents")
    shutil.copytree(SOURCE / "avatars", output / "avatars")
    (output / ".codebuddy-plugin").mkdir()
    (output / ".codebuddy-plugin/plugin.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    with tempfile.TemporaryDirectory(prefix="huiyuan-workbuddy-skill-") as directory:
        skill_source = Path(directory) / "majia-huiyuan"
        skill_builder = _load_skillhub_builder()
        skill_metadata = skill_builder.build(skill_source)
        shutil.copytree(skill_source, output / "skills/majia-huiyuan")

    for source, name in ((ROOT / "README.md", "README.md"), (ROOT / "LICENSE", "LICENSE")):
        if source.is_file():
            shutil.copy2(source, output / name)

    _write_archive(output, archive)
    files = [path for path in output.rglob("*") if path.is_file()]
    return {
        "output": str(output),
        "archive": str(archive),
        "slug": manifest["name"],
        "version": manifest["version"],
        "agentCount": len(manifest["agents"]),
        "skillFileCount": skill_metadata["fileCount"],
        "fileCount": len(files),
        "bytes": sum(path.stat().st_size for path in files),
    }


def _snapshot(root: Path) -> dict[Path, bytes]:
    if not root.is_dir():
        return {}
    return {
        path.relative_to(root): path.read_bytes()
        for path in root.rglob("*")
        if path.is_file()
    }


def check(output: Path) -> int:
    with tempfile.TemporaryDirectory(prefix="huiyuan-workbuddy-check-") as directory:
        candidate = Path(directory) / output.name
        archive = Path(directory) / f"{output.name}.zip"
        build(candidate, archive)
        expected = _snapshot(candidate)
        if not output.exists():
            print("WorkBuddy bundle 临时构建校验通过（本地没有预生成 dist）")
            return 0
        actual = _snapshot(output)
    if actual == expected:
        print("WorkBuddy bundle 生成物一致")
        return 0
    print(
        json.dumps(
            {
                "missing": sorted(str(path) for path in expected.keys() - actual.keys()),
                "extra": sorted(str(path) for path in actual.keys() - expected.keys()),
                "changed": sorted(
                    str(path)
                    for path in expected.keys() & actual.keys()
                    if expected[path] != actual[path]
                ),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 1


def main() -> int:
    parser = argparse.ArgumentParser(description="构建 WorkBuddy 单专家上传包")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--archive", type=Path, default=DEFAULT_ARCHIVE)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    output = args.output.expanduser().resolve()
    archive = args.archive.expanduser().resolve()
    if args.check:
        return check(output)
    result = build(output, archive)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
