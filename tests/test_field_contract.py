"""Current field contract and generated DDL, independent of optional Spark."""
import csv
import json
import re
import subprocess
import sys
import tempfile
import unittest
from collections import defaultdict
from pathlib import Path

import sqlglot

ROOT = Path(__file__).resolve().parents[1]


def contract():
    with (ROOT / '清单/字段契约.csv').open(encoding='utf-8-sig') as stream:
        tables = defaultdict(list)
        for field in csv.DictReader(stream):
            tables[field['表名']].append(field)
    return dict(tables)


class FieldContractTests(unittest.TestCase):
    def test_all_55_tables_and_866_columns_match_sample_order(self):
        tables = contract()
        self.assertEqual(55, len(tables))
        self.assertEqual(866, sum(map(len, tables.values())))
        for path in sorted((ROOT / '数据集/数据样本').glob('*.csv')):
            with self.subTest(table=path.stem), path.open(encoding='utf-8-sig') as stream:
                header = next(csv.reader(stream))
                fields = tables[path.stem]
                self.assertEqual(header, [f['字段名'] for f in fields])
                self.assertEqual(list(range(1, len(header)+1)), [int(f['字段序号']) for f in fields])
                for f in fields:
                    self.assertRegex(f['类型'], r'^(STRING|LONG|INTEGER|DOUBLE|BOOLEAN|DATE|TIMESTAMP|DECIMAL\(\d+,\d+\))$')
                    self.assertTrue(f['字段说明'])
                    self.assertTrue(f['类型来源'])
                    self.assertIn(f['可空'], ('是', '否'))

    def test_english_names_are_consistent_distinct_and_short(self):
        names = {}
        table_names = set()
        for table, fields in contract().items():
            english = fields[0]['表英文名']
            self.assertTrue(english.startswith(table.split('_')[0]+'_'))
            self.assertNotIn(english, table_names)
            table_names.add(english)
            local_names = []
            for f in fields:
                name = f['字段英文名']
                for value in (name, english):
                    self.assertRegex(value, r'^[a-z][a-z0-9_]*$')
                    self.assertLessEqual(len(value), 63)
                if f['字段名'] in names:
                    self.assertEqual(names[f['字段名']], name)
                names[f['字段名']] = name
                local_names.append(name)
            self.assertEqual(len(local_names), len(set(local_names)))
        self.assertEqual(497, len(names))
        self.assertEqual(len(names), len(set(names.values())))

    def test_natural_grain_is_unique_in_original_samples(self):
        for table, fields in contract().items():
            keys = [f['字段名'] for f in sorted(fields, key=lambda f: int(f['粒度键序号'] or 999)) if f['粒度键序号']]
            with self.subTest(table=table):
                self.assertTrue(keys)
                self.assertEqual(list(range(1, len(keys)+1)), sorted(int(f['粒度键序号']) for f in fields if f['粒度键序号']))
                with (ROOT / '数据集/数据样本' / (table+'.csv')).open(encoding='utf-8-sig') as stream:
                    rows = list(csv.DictReader(stream))
                values = [tuple(row[key] for key in keys) for row in rows]
                self.assertEqual(len(values), len(set(values)), (table, keys))
                self.assertTrue(all(all(v.strip() and v not in ('null', '\\N') for v in key) for key in values), (table, keys))

    def test_runtime_schema_and_base_type_provenance(self):
        from tools.build_mini_world import field_types
        evidence = json.loads((ROOT / '清单/Spark类型证据.json').read_text())
        actual = {e['output']: e for e in evidence['etls']}
        self.assertEqual(25, len(actual))
        self.assertEqual('3.4.4', evidence['spark_version'])
        for table, fields in contract().items():
            if table in actual:
                runtime = actual[table]['schema']['fields']
                self.assertEqual([f['字段名'] for f in fields], [f['name'] for f in runtime])
                self.assertEqual([f['类型'].lower() for f in fields], [f['type'] for f in runtime])
                self.assertEqual([f['可空']=='是' for f in fields], [f['nullable'] for f in runtime])
                self.assertTrue(all('Spark类型证据.json' in f['类型来源'] for f in fields))
            else:
                expected = field_types(table)
                self.assertEqual([expected[f['字段名']] for f in fields], [f['类型'] for f in fields])
        phone = next(f for f in contract()['dim_会员主档'] if f['字段名']=='手机号')
        self.assertEqual('STRING', phone['类型'])
        self.assertIn('LONG', phone['类型来源'])
        self.assertIn('156****0925', phone['字段说明'])

    def test_unused_base_tables_are_explicit_reference_structures(self):
        tables = contract()
        unused = {'dim_会员身份桥', 'dim_成本科目', 'dim_日期', 'dwd_会员注册绑定', 'dwd_积分事件', 'dwd_私域事件', 'dwd_加盟分账明细'}
        self.assertEqual(unused, {table for table, fields in tables.items() if all(f['下游消费']=='无（参考结构）' for f in fields)})

    def test_type_evidence_must_match_executed_sql_fingerprints(self):
        from tools.build_field_contract import build_contract
        evidence = json.loads((ROOT / '清单/Spark类型证据.json').read_text())
        evidence['etls'][0]['nodes'][-1]['sql_sha256'] = '0'*64
        with self.assertRaisesRegex(ValueError, 'SQL fingerprint differs'):
            build_contract(evidence=evidence)

    def test_public_sql_evidence_must_match_raw_document_bytes(self):
        from tools.build_field_contract import build_contract
        evidence = json.loads((ROOT / '清单/Spark类型证据.json').read_text())
        self.assertEqual(7, len(evidence['public_sql']))
        evidence['public_sql'][0]['sql_sha256'] = '0'*64
        with self.assertRaisesRegex(ValueError, 'Public SQL fingerprint differs'):
            build_contract(evidence=evidence)

    def test_all_four_dialects_have_55_parseable_creates(self):
        from sqlglot import exp
        for naming in ('zh', 'en'):
            with tempfile.TemporaryDirectory() as tmp:
                result = subprocess.run([sys.executable, str(ROOT / 'tools/build_ddl.py'), '--naming', naming, '--out', tmp], capture_output=True, text=True)
                self.assertEqual(0, result.returncode, result.stderr)
                for dialect in ('spark', 'mysql', 'postgres', 'clickhouse'):
                    statements = sqlglot.parse((Path(tmp) / (dialect+'.sql')).read_text(), read=dialect)
                    creates = [s for s in statements if isinstance(s, exp.Create)]
                    self.assertEqual(55, len(creates), (naming, dialect))
                    self.assertTrue(all(len(s.this.expressions)==len(contract()[table]) for table, s in zip(sorted(contract()), creates)))

    def test_checked_in_contract_and_ddl_do_not_drift(self):
        for tool in ('build_field_contract.py', 'build_ddl.py'):
            result = subprocess.run([sys.executable, str(ROOT / 'tools' / tool), '--check'], capture_output=True, text=True)
            self.assertEqual(0, result.returncode, result.stdout+result.stderr)

    def test_ddl_check_detects_modified_generated_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            command = [sys.executable, str(ROOT / 'tools/build_ddl.py'), '--out', tmp]
            self.assertEqual(0, subprocess.run(command, capture_output=True).returncode)
            path = Path(tmp) / 'spark.sql'
            path.write_text(path.read_text().replace('STRING', 'BIGINT', 1))
            result = subprocess.run(command+['--check'], capture_output=True, text=True)
            self.assertNotEqual(0, result.returncode)
            self.assertIn('spark.sql', result.stdout+result.stderr)


if __name__ == '__main__':
    unittest.main()
