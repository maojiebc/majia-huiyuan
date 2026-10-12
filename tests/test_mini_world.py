"""Coherent fixtures and strict document parsing; requires no Spark."""
import csv
import importlib.util
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

def read_rows(path):
    with path.open() as f:
        return list(csv.DictReader(f))

def module(name):
    path = ROOT / 'tools' / (name + '.py')
    if not path.exists():
        return None
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    import sys
    sys.modules[name] = result
    spec.loader.exec_module(result)
    return result

class MiniWorldTests(unittest.TestCase):
    def test_generator_is_deterministic_and_keeps_all_base_headers(self):
        builder = module('build_mini_world')
        self.assertIsNotNone(builder, 'missing coherent fixture generator')
        with tempfile.TemporaryDirectory() as tmp:
            left, right = Path(tmp)/'a', Path(tmp)/'b'
            builder.build_world(left)
            builder.build_world(right)
            files = sorted(left.glob('*.csv'))
            self.assertEqual(31, len(files))  # 30 base tables plus independent known answers
            for path in files:
                self.assertEqual(path.read_bytes(), (right/path.name).read_bytes())
                if path.name != 'expected.csv':
                    with path.open() as f, (ROOT/'数据集/数据样本'/path.name).open() as g:
                        self.assertEqual(next(csv.reader(g)), next(csv.reader(f)))
            orders = read_rows(left/'dwd_订单.csv')
            members = {r['会员ID'] for r in read_rows(left/'dim_会员主档.csv')}
            self.assertGreater(sum(r['会员ID'] in members for r in orders), 1000)
            self.assertTrue(all(r['业务日期'] <= '2026-06-24' for r in orders))

    def test_alternate_asof_moves_the_known_profit_break_month(self):
        builder=module('build_mini_world')
        self.assertIsNotNone(builder)
        with tempfile.TemporaryDirectory() as tmp:
            builder.build_world(Path(tmp),as_of='2026-05-31')
            costs=read_rows(Path(tmp)/'dwd_门店成本明细.csv')
            anchor=[r for r in costs if r['门店ID']=='S900001' and r['月份']=='2026-02-01']
            self.assertEqual(8,len(anchor))
            self.assertEqual(0,sum(float(r['成本金额']) for r in anchor))

    def test_adversarial_scenario_keeps_future_and_overlapping_raw_rows(self):
        builder = module('build_mini_world')
        self.assertIsNotNone(builder, 'missing coherent fixture generator')
        with tempfile.TemporaryDirectory() as tmp:
            builder.build_world(Path(tmp), scenario='adversarial')
            orders = read_rows(Path(tmp)/'dwd_订单.csv')
            self.assertTrue(any(r['业务日期'] > '2026-06-24' for r in orders))
            stores = read_rows(Path(tmp)/'dim_门店主档.csv')
            self.assertEqual(2, sum(r['门店ID'] == 'S900001' for r in stores))

class ParserTests(unittest.TestCase):
    def test_parse_all_documents_and_sort_dependencies(self):
        runner = module('run_pipeline')
        self.assertIsNotNone(runner, 'missing verbatim SQL runner')
        flows = runner.parse_pipeline(ROOT)
        self.assertEqual(25, len(flows))
        names = [flow.output for flow in flows]
        self.assertLess(names.index('dws_单店利润月汇总'), names.index('dws_加盟回本测算'))
        self.assertLess(names.index('ads_异常归因清单'), names.index('dqc_归因清单对账'))

    def test_unbound_bare_input_is_rejected_without_reusing_a_previous_view(self):
        runner = module('run_pipeline')
        text = "### 节点1\n- Id: bad\n- Name: bad\n- Type: SQL_SCRIPT\n- OutputDsName: dws_bad\n- 等价SQL:\n```sql\nSELECT * FROM input\n```"
        with self.assertRaisesRegex(ValueError, 'input'):
            runner.parse_document(text, Path('bad.md'))

    def test_input_alias_words_inside_literals_are_not_sources(self):
        runner = module('run_pipeline')
        text = "### 节点1\n- Id: literal\n- Name: literal\n- Type: SQL_SCRIPT\n- OutputDsName: dws_literal\n- 等价SQL:\n```sql\nSELECT 'input9' AS note\n```"
        flow = runner.parse_document(text, Path('literal.md'))
        self.assertEqual((), flow.nodes[0].sources)

    def test_internal_cte_named_input_is_not_an_unbound_temp_view(self):
        runner = module('run_pipeline')
        text = "### 节点1\n- Id: cte\n- Name: cte\n- Type: SQL_SCRIPT\n- OutputDsName: dws_cte\n- 等价SQL:\n```sql\nWITH input9 AS (SELECT 'ok' AS note) SELECT * FROM input9\n```"
        flow = runner.parse_document(text, Path('cte.md'))
        self.assertEqual((), flow.nodes[0].sources)

    def test_numbered_input_ports_are_positive_and_canonical(self):
        runner = module('run_pipeline')
        for alias in ('input0', 'input01'):
            with self.subTest(alias=alias), self.assertRaisesRegex(ValueError, alias):
                text = f"### 节点1\n- Id: bad\n- Name: bad\n- Type: SQL_SCRIPT\n- OutputDsName: dws_bad\n- 等价SQL:\n```sql\nSELECT * FROM {alias}\n```"
                runner.parse_document(text, Path('bad.md'))

    def test_dataset_input_node_cannot_use_a_stale_numbered_alias(self):
        runner = module('run_pipeline')
        text = "### 节点1\n- Id: bad\n- Name: dwd_订单\n- Type: INPUT_DATASET\n- OutputDsName: dws_bad\n- 等价SQL:\n```sql\nSELECT * FROM input1\n```"
        with self.assertRaisesRegex(ValueError, 'input1'):
            runner.parse_document(text, Path('bad.md'))

    def test_missing_sources_and_cycles_fail_without_guessing(self):
        runner = module('run_pipeline')
        self.assertIsNotNone(runner, 'missing verbatim SQL runner')
        with self.assertRaisesRegex(ValueError, 'input2'):
            runner.parse_document('### 节点1\n- Id: a\n- Name: a\n- Type: SQL_SCRIPT\n- **Sources (Inputs):**\n  - b (b)\n- 等价SQL:\n```sql\nSELECT * FROM input2\n```', Path('bad.md'))
        with self.assertRaisesRegex(ValueError, 'cycle|环'):
            runner.topological_sort({'a': {'b'}, 'b': {'a'}})

if __name__ == '__main__':
    unittest.main()
