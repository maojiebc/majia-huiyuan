"""WorkBuddy expert bundle contract tests."""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
import struct
import subprocess
import sys
import tempfile
from typing import Any, ClassVar
import unittest
import zipfile


ROOT = Path(__file__).resolve().parents[1]
BUILD_SCRIPT = ROOT / "tools/build_workbuddy_bundle.py"


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


if __name__ == "__main__":
    unittest.main()
