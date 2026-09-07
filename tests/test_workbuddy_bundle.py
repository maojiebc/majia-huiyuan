"""WorkBuddy expert bundle contract tests."""
from __future__ import annotations

import json
import importlib.util
import os
import posixpath
from pathlib import Path
import re
import struct
import subprocess
import sys
import tempfile
from typing import Any, ClassVar
import unittest
from unittest import mock
import zipfile
from urllib.parse import unquote, urlsplit


ROOT = Path(__file__).resolve().parents[1]
BUILD_SCRIPT = ROOT / "tools/build_workbuddy_bundle.py"
SPEC = importlib.util.spec_from_file_location("workbuddy_builder", BUILD_SCRIPT)
BUILDER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BUILDER)


def _run(command: list[object], *, cwd: Path) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env.pop("PYTHONPATH", None)
    env.update({"PYTHONDONTWRITEBYTECODE": "1", "PYTHONNOUSERSITE": "1"})
    return subprocess.run(
        [str(part) for part in command],
        cwd=cwd,
        env=env,
        text=True,
        capture_output=True,
        check=False,
        timeout=120,
    )


def _output(result: subprocess.CompletedProcess[str]) -> str:
    return f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"


def _json_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise AssertionError(f"{path} must contain a JSON object")
    return value


def _frontmatter(path: Path) -> dict[str, object]:
    text = path.read_text(encoding="utf-8")
    match = re.match(r"\A---\n(?P<body>.*?)\n---\n", text, re.DOTALL)
    if match is None:
        raise AssertionError(f"{path} is missing YAML frontmatter")
    values: dict[str, object] = {}
    current: str | None = None
    for line in match.group("body").splitlines():
        if line.startswith("  ") and current:
            key, raw = line.strip().split(":", 1)
            nested = values.setdefault(current, {})
            assert isinstance(nested, dict)
            nested[key] = raw.strip().strip('"')
            continue
        key, raw = line.split(":", 1)
        current = key
        values[key] = raw.strip().strip('"') if raw.strip() else {}
    return values


def _png_dimensions(path: Path) -> tuple[int, int]:
    data = path.read_bytes()[:24]
    if len(data) != 24 or data[:8] != b"\x89PNG\r\n\x1a\n":
        raise AssertionError(f"{path} is not a valid PNG")
    return struct.unpack(">II", data[16:24])


class WorkBuddyBundleTests(unittest.TestCase):
    _temporary: ClassVar[tempfile.TemporaryDirectory[str]]
    temp_root: ClassVar[Path]
    bundle: ClassVar[Path]
    archive: ClassVar[Path]
    metadata: ClassVar[dict[str, Any]]

    @classmethod
    def setUpClass(cls) -> None:
        cls._temporary = tempfile.TemporaryDirectory(prefix="huiyuan-workbuddy-")
        cls.addClassCleanup(cls._temporary.cleanup)
        cls.temp_root = Path(cls._temporary.name)
        cls.bundle = cls.temp_root / "majia-huiyuan"
        cls.archive = cls.temp_root / "majia-huiyuan.zip"
        result = _run(
            [
                sys.executable,
                "-I",
                "-B",
                BUILD_SCRIPT,
                "--output",
                cls.bundle,
                "--archive",
                cls.archive,
            ],
            cwd=cls.temp_root,
        )
        if result.returncode != 0:
            raise AssertionError(f"WorkBuddy build failed\n{_output(result)}")
        cls.metadata = json.loads(result.stdout)

    def test_manifest_matches_single_expert_contract(self) -> None:
        manifest = _json_object(self.bundle / ".codebuddy-plugin/plugin.json")
        source_manifest = _json_object(ROOT / "manifest.json")
        self.assertEqual(manifest["name"], "majia-huiyuan")
        self.assertEqual(manifest["version"], source_manifest["version"])
        self.assertEqual(manifest["expertType"], "agent")
        self.assertEqual(manifest["agentName"], "majia-huiyuan")
        self.assertEqual(manifest["plugin"], manifest["name"])
        self.assertEqual(manifest["defaultInitPrompt"], manifest["quickPrompts"][0])
        self.assertLessEqual(len(manifest["displayName"]["zh"]), 15)
        self.assertEqual(len(manifest["agents"]), 1)
        self.assertEqual(len(manifest["tags"]), 3)
        self.assertEqual(len(manifest["quickPrompts"]), 3)
        chinese_description = manifest["displayDescription"]["zh"]
        chinese_characters = re.findall(r"[\u3400-\u9fff]", chinese_description)
        self.assertGreaterEqual(len(chinese_characters), 40)
        self.assertLessEqual(len(chinese_characters), 50)

    def test_manifest_paths_and_agent_frontmatter_are_self_contained(self) -> None:
        manifest = _json_object(self.bundle / ".codebuddy-plugin/plugin.json")
        bundle_root = self.bundle.resolve()
        for raw_path in [*manifest["agents"], *manifest["skills"], manifest["avatar"]]:
            target = (self.bundle / raw_path).resolve()
            self.assertTrue(target.is_relative_to(bundle_root), raw_path)
            self.assertTrue(target.exists(), raw_path)
        agent_path = self.bundle / manifest["agents"][0]
        fields = _frontmatter(agent_path)
        self.assertEqual(fields["name"], agent_path.stem)
        self.assertTrue(fields.get("description"))
        self.assertIsInstance(fields.get("displayName"), dict)
        self.assertIsInstance(fields.get("profession"), dict)

    def test_avatar_meets_platform_limits(self) -> None:
        avatar = self.bundle / "avatars/expert.png"
        self.assertEqual(_png_dimensions(avatar), (512, 512))
        self.assertLessEqual(avatar.stat().st_size, 500_000)

    def test_bundle_keeps_core_knowledge_and_excludes_build_debris(self) -> None:
        skill = self.bundle / "skills/majia-huiyuan"
        for relative in (
            "SKILL.md",
            "公式库/README.md",
            "ETL/公共口径/04_v1.4.1_业务验收.sql",
            "看板/页面文档/02-会员私域驾驶舱.md",
        ):
            self.assertTrue((skill / relative).is_file(), relative)
        relative_files = [
            path.relative_to(self.bundle)
            for path in self.bundle.rglob("*")
            if path.is_file()
        ]
        self.assertFalse(any("__pycache__" in path.parts for path in relative_files))
        self.assertFalse(any(path.suffix == ".pyc" for path in relative_files))
        self.assertFalse(any(path.name == ".DS_Store" for path in relative_files))
        self.assertFalse(any("原始JSON" in path.parts for path in relative_files))
        self.assertFalse(any("数据样本" in path.parts for path in relative_files))
        self.assertFalse(any(path.is_symlink() for path in self.bundle.rglob("*")))

    def test_archive_has_one_clean_top_level_directory(self) -> None:
        self.assertTrue(self.archive.is_file())
        with zipfile.ZipFile(self.archive) as archive:
            names = archive.namelist()
        self.assertTrue(names)
        self.assertTrue(all(name.startswith("majia-huiyuan/") for name in names))
        self.assertFalse(any("__MACOSX" in name or ".DS_Store" in name for name in names))
        self.assertEqual(Path(self.metadata["archive"]).resolve(), self.archive.resolve())
        self.assertEqual(
            self.metadata["version"],
            _json_object(ROOT / "manifest.json")["version"],
        )

    def test_check_builds_without_committed_dist(self) -> None:
        missing_output = self.temp_root / "not-prebuilt" / "majia-huiyuan"
        result = _run(
            [
                sys.executable,
                "-I",
                "-B",
                BUILD_SCRIPT,
                "--output",
                missing_output,
                "--check",
            ],
            cwd=self.temp_root,
        )
        self.assertEqual(result.returncode, 0, _output(result))
        self.assertIn("临时构建校验通过", result.stdout)
        self.assertFalse(missing_output.exists())

    def test_zip_is_reproducible_despite_file_timestamp_changes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            bundle = root / "custom-output"
            bundle.mkdir()
            source = bundle / "README.md"
            source.write_text("same content", encoding="utf-8")
            os.utime(source, (1_600_000_000, 1_600_000_000))
            BUILDER._write_archive(bundle, root / "one.zip")
            os.utime(source, (1_700_000_000, 1_700_000_000))
            BUILDER._write_archive(bundle, root / "two.zip")
            self.assertEqual((root / "one.zip").read_bytes(), (root / "two.zip").read_bytes())
            with zipfile.ZipFile(root / "one.zip") as archive:
                self.assertEqual(archive.namelist(), ["majia-huiyuan/README.md"])

    def test_check_rejects_corrupt_or_stale_archive(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive = root / "broken.zip"
            for payload in (b"broken zip",):
                archive.write_bytes(payload)
                result = _run([sys.executable, BUILD_SCRIPT, "--check", "--output", self.bundle,
                               "--archive", archive], cwd=root)
                self.assertNotEqual(result.returncode, 0, _output(result))
            with zipfile.ZipFile(archive, "w") as target:
                target.writestr("majia-huiyuan/README.md", "stale")
            result = _run([sys.executable, BUILD_SCRIPT, "--check", "--output", self.bundle,
                           "--archive", archive], cwd=root)
            self.assertNotEqual(result.returncode, 0, _output(result))

    def test_build_preserves_unrelated_output_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "documents"
            output.mkdir()
            sentinel = output / "my-notes.txt"
            sentinel.write_text("keep me", encoding="utf-8")
            with self.assertRaises(ValueError):
                BUILDER.build(output, root / "bundle.zip")
            self.assertEqual(sentinel.read_text(), "keep me")

    def test_invalid_manifest_is_rejected_before_replacing_previous_bundle(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output, archive = root / "bundle", root / "bundle.zip"
            BUILDER.build(output, archive)
            previous = archive.read_bytes()
            manifest = BUILDER._read_manifest()
            for value in ("名" * 16, ""):
                manifest["displayName"]["zh"] = value
                with mock.patch.object(BUILDER, "_read_manifest", return_value=manifest):
                    with self.assertRaises(ValueError):
                        BUILDER.build(output, archive)
                self.assertEqual(archive.read_bytes(), previous)

    def test_bundle_identifies_workbuddy_and_has_its_own_entrypoint(self):
        text = (self.bundle / "skills/majia-huiyuan/SKILL.md").read_text()
        self.assertNotIn("SkillHub 为文本精简包", text)
        readme = (self.bundle / "README.md").read_text()
        self.assertIn("skills/majia-huiyuan/SKILL.md", readme)
        self.assertTrue((self.bundle / "skills/majia-huiyuan/公式库/实战问题入口.md").is_file())

    def test_absent_source_links_point_to_versioned_github(self):
        readme = (self.bundle / "skills/majia-huiyuan/README.md").read_text()
        version = _json_object(ROOT / "manifest.json")["version"]
        self.assertIn(f"https://github.com/maojiebc/majia-huiyuan/blob/v{version}/workbuddy/README.md", readme)
        self.assertNotIn("](./数据集/数据样本/)", readme)
        self.assertIn("](./LICENSE.md)", readme)

    def test_unsafe_targets_and_symlinks_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            target = root / "existing"
            target.mkdir()
            (target / "keep.txt").write_text("keep")
            link = root / "link"
            link.symlink_to(target, target_is_directory=True)
            for output, archive in ((ROOT, root / "out.zip"),
                                    (root / "new", ROOT / "manifest.json"),
                                    (root / "new", root / "new" / "recursive.zip"),
                                    (link, root / "out.zip")):
                with self.subTest(output=output, archive=archive):
                    with self.assertRaises(ValueError):
                        BUILDER.build(output, archive)
            self.assertEqual((target / "keep.txt").read_text(), "keep")

    def test_failed_zip_write_preserves_previous_outputs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output, archive = root / "bundle", root / "bundle.zip"
            BUILDER.build(output, archive)
            before, zip_before = BUILDER._snapshot(output), archive.read_bytes()
            with mock.patch.object(BUILDER, "_write_archive", side_effect=OSError("disk full")):
                with self.assertRaises(OSError):
                    BUILDER.build(output, archive)
            self.assertEqual(BUILDER._snapshot(output), before)
            self.assertEqual(archive.read_bytes(), zip_before)

    def test_invalid_paths_and_avatar_are_rejected(self):
        manifest = BUILDER._read_manifest()
        for key, value in (("agents", ["../outside.md"]), ("skills", ["/tmp/external"]),
                           ("avatar", "../../outside.png"), ("tags", []), ("version", "99.0.0")):
            with self.subTest(key=key), mock.patch.dict(manifest, {key: value}):
                with self.assertRaises(ValueError):
                    BUILDER._validate_manifest(manifest)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "avatars").mkdir()
            (root / "avatars/expert.png").write_bytes(b"not a png")
            with mock.patch.object(BUILDER, "SOURCE", root):
                with self.assertRaises(ValueError):
                    BUILDER._validate_manifest(manifest)

    def test_check_rejects_archive_without_output_and_duplicate_entries(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive = root / "duplicate.zip"
            import warnings
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                with zipfile.ZipFile(archive, "w") as target:
                    target.writestr("majia-huiyuan/README.md", "one")
                    target.writestr("majia-huiyuan/README.md", "two")
            with self.assertRaises(ValueError):
                BUILDER.check(root / "absent", archive)

    def test_all_local_markdown_link_targets_exist_in_zip(self):
        with zipfile.ZipFile(self.archive) as archive:
            names = set(archive.namelist())
            for name in sorted(names):
                if not name.endswith((".md", ".txt")):
                    continue
                for raw in re.findall(r"\]\(([^\s)]+)\)", archive.read(name).decode("utf-8")):
                    parsed = urlsplit(raw)
                    if parsed.scheme or parsed.netloc or not parsed.path:
                        continue
                    target = posixpath.normpath(posixpath.join(posixpath.dirname(name), unquote(parsed.path)))
                    self.assertTrue(target in names or any(n.startswith(target.rstrip("/") + "/") for n in names),
                                    f"{name}: {raw}")


if __name__ == "__main__":
    unittest.main()
