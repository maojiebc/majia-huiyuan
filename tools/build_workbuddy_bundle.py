#!/usr/bin/env python3
"""Build a self-contained WorkBuddy expert ZIP from repository sources."""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import re
import struct
from pathlib import Path
import shutil
import tempfile
from types import ModuleType
import zipfile
from urllib.parse import quote, unquote, urlsplit


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "workbuddy"
DEFAULT_OUTPUT = ROOT / "dist/workbuddy/majia-huiyuan"
DEFAULT_ARCHIVE = ROOT / "dist/workbuddy/majia-huiyuan.zip"
SLUG = "majia-huiyuan"


def _no_symlinks(path: Path) -> None:
    # macOS exposes its standard temporary directory through /var -> /private/var.
    system_aliases = {Path("/var"): Path("/private/var"), Path("/tmp"): Path("/private/tmp")}
    if any(parent.is_symlink() and parent.resolve() != system_aliases.get(parent)
           for parent in (path, *path.parents)):
        raise ValueError(f"不允许符号链接：{path}")
    if path.is_dir() and any(item.is_symlink() for item in path.rglob("*")):
        raise ValueError(f"输入或输出中有符号链接：{path}")


def _validate_targets(output: Path, archive: Path) -> None:
    for path in (output, archive):
        _no_symlinks(path)
        if ROOT.is_relative_to(path) or Path.home().is_relative_to(path):
            raise ValueError("输出不能是项目目录、主目录或其上级目录")
        if path.is_relative_to(ROOT) and not path.is_relative_to(ROOT / "dist"):
            raise ValueError("仓库内的输出只能放在 dist 子目录")
    if archive.is_relative_to(output) or output.is_relative_to(archive):
        raise ValueError("ZIP 必须放在输出目录之外")
    if archive.suffix.lower() != ".zip" or archive.is_dir():
        raise ValueError("archive 必须是 .zip 文件")
    if output.exists():
        marker = output / ".codebuddy-plugin/plugin.json"
        if not marker.is_file() or json.loads(marker.read_text())["name"] != SLUG:
            raise ValueError("拒绝覆盖非本项目生成的输出目录")


def _validate_manifest(manifest: dict) -> None:
    for field in ("name", "agentName", "plugin"):
        if manifest.get(field) != SLUG:
            raise ValueError(f"{field} 必须为 {SLUG}")
    if manifest.get("expertType") != "agent":
        raise ValueError("必须使用单专家 expertType=agent")
    if not re.fullmatch(r"\d+\.\d+\.\d+", str(manifest.get("version", ""))):
        raise ValueError("version 必须是三段版本号")
    skill_version = re.search(r'^  version: "([^"]+)"$', (ROOT / "SKILL.md").read_text(encoding="utf-8"), re.MULTILINE)
    if skill_version is None or skill_version.group(1) != manifest["version"]:
        raise ValueError("manifest.json 与 SKILL.md 版本不一致")
    for field in ("displayName", "profession", "displayDescription", "defaultInitPrompt"):
        value = manifest.get(field)
        if not isinstance(value, dict) or any(not isinstance(value.get(lang), str) or not value[lang].strip()
                                             for lang in ("zh", "en")):
            raise ValueError(f"{field} 必须包含非空中英文字段")
    if len(manifest["displayName"]["zh"].encode("utf-16-le")) // 2 > 15:
        raise ValueError("专家花名不能超过 15 字")
    count = len(re.findall(r"[\u3400-\u9fff]", manifest["displayDescription"]["zh"]))
    if not 40 <= count <= 50:
        raise ValueError("专家简介需包含 40–50 个汉字")
    for field in ("tags", "quickPrompts"):
        values = manifest.get(field)
        if not isinstance(values, list) or len(values) != 3:
            raise ValueError(f"{field} 必须有三项")
        for value in values:
            if not isinstance(value, dict) or any(not isinstance(value.get(lang), str) or not value[lang].strip()
                                                 for lang in ("zh", "en")):
                raise ValueError(f"{field} 必须有非空中英文字段")
    if manifest["defaultInitPrompt"] != manifest["quickPrompts"][0]:
        raise ValueError("默认问题必须等于第一个示例问题")
    for field, expected in (("agents", [f"./agents/{SLUG}.md"]),
                            ("skills", [f"./skills/{SLUG}"]), ("avatar", "avatars/expert.png")):
        if manifest.get(field) != expected:
            raise ValueError(f"{field} 必须引用本专家的包内路径")
    if not manifest.get("categoryId"):
        raise ValueError("缺少 categoryId")
    avatar = SOURCE / manifest["avatar"]
    data = avatar.read_bytes()
    if (len(data) < 24 or data[:8] != b"\x89PNG\r\n\x1a\n" or
            struct.unpack(">II", data[16:24]) != (512, 512) or len(data) > 500_000):
        raise ValueError("头像必须为 512×512 PNG，且不超过 500KB")


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
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as target:
        for path in sorted(output.rglob("*")):
            if path.is_file():
                relative = Path(SLUG) / path.relative_to(output)
                info = zipfile.ZipInfo(relative.as_posix(), date_time=(2020, 1, 1, 0, 0, 0))
                info.create_system = 3
                info.external_attr = 0o100644 << 16
                info.compress_type = zipfile.ZIP_DEFLATED
                target.writestr(info, path.read_bytes(), compresslevel=9)


def _adapt_knowledge(skill: Path, version: str) -> None:
    """Resolve references omitted by the text-only bundle to the matching GitHub tag."""
    skill = skill.resolve()
    for path in skill.rglob("*"):
        if not path.is_file() or path.suffix not in (".md", ".txt"):
            continue
        text = path.read_text(encoding="utf-8").replace("SkillHub 为文本精简包", "WorkBuddy 为文本精简包")
        def replace(match: re.Match) -> str:
            raw = match.group(1)
            parsed = urlsplit(raw)
            if parsed.scheme or parsed.netloc or not parsed.path:
                return match.group(0)
            local = (path.parent / unquote(parsed.path)).resolve()
            if local.exists() and local.is_relative_to(skill):
                return match.group(0)
            if local.name == "LICENSE" and local.with_name("LICENSE.md").exists():
                return match.group(0).replace(raw, raw.replace("LICENSE", "LICENSE.md"))
            if not local.is_relative_to(skill):
                return match.group(0)
            relative = local.relative_to(skill)
            original = ROOT / relative
            if not original.exists():
                return match.group(0)
            kind = "tree" if original.is_dir() else "blob"
            url = f"https://github.com/maojiebc/{SLUG}/{kind}/v{version}/{quote(relative.as_posix())}"
            if parsed.fragment:
                url += "#" + parsed.fragment
            return match.group(0).replace(raw, url)
        text = re.sub(r"\]\(([^\s)]+)\)", replace, text)
        path.write_text(text, encoding="utf-8")


def build(output: Path, archive: Path) -> dict[str, object]:
    _no_symlinks(output)
    _no_symlinks(archive)
    output, archive = output.resolve(), archive.resolve()
    _validate_targets(output, archive)
    _no_symlinks(SOURCE)
    manifest = _read_manifest()
    _validate_manifest(manifest)
    builder = _load_skillhub_builder()
    for relative in (*builder.ROOT_FILES, *builder.DIRECTORIES, "LICENSE", "分享/区域运营的一天/README.md"):
        _no_symlinks(ROOT / relative)
    output.parent.mkdir(parents=True, exist_ok=True)
    archive.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".workbuddy-build-", dir=output.parent) as directory:
        stage = Path(directory)
        candidate = stage / SLUG
        candidate.mkdir()
        skill_metadata = _populate(candidate, manifest, builder)
        # Preserve unknown additions in an existing output instead of erasing them.
        if output.exists() and set(_snapshot(output)) - set(_snapshot(candidate)):
            raise ValueError("输出目录含有本次构建之外的文件，请使用新的输出目录")
        with tempfile.TemporaryDirectory(prefix=".workbuddy-zip-", dir=archive.parent) as zip_dir:
            candidate_zip = Path(zip_dir) / "bundle.zip"
            _write_archive(candidate, candidate_zip)
            if candidate_zip.stat().st_size > 20 * 1024 * 1024:
                raise ValueError("ZIP 超过 20MB")
            backup = stage / "previous"
            if output.exists():
                output.rename(backup)
            try:
                candidate.rename(output)
                candidate_zip.replace(archive)
            except OSError:
                if output.exists():
                    shutil.rmtree(output)
                if backup.exists():
                    backup.rename(output)
                raise
    files = [path for path in output.rglob("*") if path.is_file()]
    return {
        "output": str(output), "archive": str(archive), "slug": manifest["name"],
        "version": manifest["version"], "agentCount": len(manifest["agents"]),
        "skillFileCount": skill_metadata["fileCount"], "fileCount": len(files),
        "bytes": sum(path.stat().st_size for path in files),
        "archiveBytes": archive.stat().st_size,
        "sha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
    }


def _populate(output: Path, manifest: dict, skill_builder: ModuleType) -> dict:
    for relative in (*manifest["agents"], manifest["avatar"]):
        target = output / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(SOURCE / relative, target)
    (output / ".codebuddy-plugin").mkdir()
    (output / ".codebuddy-plugin/plugin.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    with tempfile.TemporaryDirectory(prefix="huiyuan-workbuddy-skill-") as directory:
        skill_source = Path(directory) / "majia-huiyuan"
        skill_metadata = skill_builder.build(skill_source)
        _adapt_knowledge(skill_source, manifest["version"])
        (skill_source / "GENERATED.md").write_text(
            "# 自动生成的 WorkBuddy 知识包\n\n"
            "请编辑 GitHub 仓库的源文件，再运行 tools/build_workbuddy_bundle.py 重建。\n",
            encoding="utf-8",
        )
        shutil.copytree(skill_source, output / "skills/majia-huiyuan")

    for source, name in ((SOURCE / "PACKAGE_README.md", "README.md"), (ROOT / "LICENSE", "LICENSE")):
        if source.is_file():
            shutil.copy2(source, output / name)

    return skill_metadata


def _snapshot(root: Path) -> dict[Path, bytes]:
    if not root.is_dir():
        return {}
    return {
        path.relative_to(root): path.read_bytes()
        for path in root.rglob("*")
        if path.is_file()
    }


def check(output: Path, archive: Path | None = None) -> int:
    _no_symlinks(output)
    if archive is not None:
        _no_symlinks(archive)
    with tempfile.TemporaryDirectory(prefix="huiyuan-workbuddy-check-") as directory:
        candidate = Path(directory) / SLUG
        candidate_zip = Path(directory) / "bundle.zip"
        build(candidate, candidate_zip)
        expected = _snapshot(candidate)
        if archive is not None:
            if not archive.is_file() and output.exists():
                raise ValueError("已存在输出目录，但缺少 ZIP")
            if archive.is_file():
                with zipfile.ZipFile(archive) as target:
                    names = target.namelist()
                    expected_zip = {f"{SLUG}/{key.as_posix()}": value for key, value in expected.items()}
                    if len(names) != len(set(names)) or set(names) != set(expected_zip):
                        raise ValueError("ZIP 文件清单与当前源码不一致")
                    if target.testzip() or any(target.read(key) != value for key, value in expected_zip.items()):
                        raise ValueError("ZIP 内容与当前源码不一致")
        if not output.exists():
            print("WorkBuddy bundle 临时构建校验通过（本地没有预生成 dist）")
            return 0
        actual = _snapshot(output)
    if actual == expected:
        print("WorkBuddy bundle 目录与 ZIP 校验通过")
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
    parser.add_argument("--archive", type=Path)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    output = args.output.expanduser().absolute()
    archive = args.archive.expanduser().absolute() if args.archive else output.with_suffix(".zip")
    try:
        if args.check:
            return check(output, archive)
        result = build(output, archive)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    except (ValueError, OSError, KeyError, RuntimeError, zipfile.BadZipFile) as error:
        print(f"WorkBuddy 构建/校验失败：{error}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
