"""Metric cards link existing definitions, fields and acceptance checks."""
import csv
import json
import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CARDS = ROOT / '口径卡片'
REQUIRED = set('id 名称 一句话 业务问题 统计对象 分子 分母 时间窗 去重键 有效状态 退款与取消 观察完整性 口径选项 来源路径 字段 验收项 常见误读 禁用说法'.split())


def read_card(path):
    text = path.read_text(encoding='utf-8')
    match = re.match(r'^---\n(.*?)\n---\n(.*)$', text, re.S)
    if match is None:
        raise ValueError(f'Missing YAML frontmatter: {path}')
    # A deliberately small YAML subset: each top-level value uses JSON quoting.
    # This is valid YAML and keeps the lightweight suite dependency-free.
    data = {}
    for line in match.group(1).splitlines():
        key, value = line.split(':', 1)
        if key in data:
            raise ValueError(f'Duplicate key {key}: {path}')
        data[key] = json.loads(value.strip())
    return data, match.group(2)


class MetricCardTests(unittest.TestCase):
    def test_sixteen_cards_have_complete_definitions_and_unique_ids(self):
        paths = sorted(p for p in CARDS.glob('*.md') if p.name != 'README.md')
        self.assertEqual(16, len(paths))
        ids = []
        for path in paths:
            data, body = read_card(path)
            self.assertEqual(REQUIRED, set(data), path.name)
            self.assertTrue(all(data.values()), path.name)
            self.assertIn('模拟', body, path.name)
            self.assertIn('来源', body, path.name)
            ids.append(data['id'])
        self.assertEqual(len(ids), len(set(ids)))

    def test_card_sources_fields_and_acceptance_checks_exist(self):
        columns = {}
        contract = ROOT / '清单/字段契约.csv'
        if contract.exists():
            with contract.open(encoding='utf-8-sig', newline='') as handle:
                for row in csv.DictReader(handle):
                    columns.setdefault(row['表名'], set()).add(row['字段名'])
        if not columns:
            for path in (ROOT / '数据集/数据样本').glob('*.csv'):
                with path.open(encoding='utf-8-sig', newline='') as handle:
                    columns[path.stem] = set(next(csv.reader(handle)))
        acceptance = (ROOT / 'ETL/公共口径/04_v1.4.1_业务验收.sql').read_text()
        check_ids = set(re.findall(r"(?:SELECT|UNION ALL SELECT) '(\d{2})'", acceptance))
        for path in CARDS.glob('*.md'):
            if path.name == 'README.md':
                continue
            data, _ = read_card(path)
            for source in data['来源路径']:
                self.assertTrue((ROOT / source.split('#', 1)[0]).is_file(), (path.name, source))
            for ref in data['字段']:
                table, field = ref.split('.', 1)
                self.assertIn(field, columns.get(table, set()), (path.name, ref))
            for check in data['验收项']:
                self.assertTrue(check == '无' or check in check_ids, (path.name, check))

    def test_readme_links_each_card_once(self):
        self.assertTrue((CARDS / 'README.md').is_file())
        readme = (CARDS / 'README.md').read_text()
        links = re.findall(r'\]\(([^)]+\.md)\)', readme)
        paths = [p.name for p in CARDS.glob('*.md') if p.name != 'README.md']
        self.assertCountEqual(paths, links)
