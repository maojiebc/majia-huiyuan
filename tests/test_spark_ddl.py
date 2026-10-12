"""Actually create 55 Spark tables, load declared types, and read every row back."""
import csv
import importlib.util
import json
import os
import tempfile
import unittest
from collections import Counter
from pathlib import Path

from tools.build_ddl import generate, read_contract
from tools.run_pipeline import create_spark, run_pipeline

ROOT = Path(__file__).resolve().parents[1]
HAVE_SPARK = importlib.util.find_spec('pyspark') is not None
if os.environ.get('REQUIRE_SPARK') == '1' and not HAVE_SPARK:
    raise RuntimeError('REQUIRE_SPARK=1 but PySpark is unavailable')


def declared_schema(fields):
    from pyspark.sql.types import StructField, StructType, _parse_datatype_string
    from tools.build_ddl import sql_type
    return StructType([StructField(f['字段名'], _parse_datatype_string(sql_type(f['类型'], 'spark')), f['可空']=='是') for f in fields])


@unittest.skipUnless(HAVE_SPARK, 'PySpark optional; install requirements-spark.txt')
class SparkDDLTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.spark = create_spark()
        cls.tables = read_contract()
        cls.report_dir = Path(os.environ.get('SPARK_REPORT_DIR', cls.tmp.name+'/reports'))
        cls.report_dir.mkdir(parents=True, exist_ok=True)
        cls.frames, cls.pipeline_report = run_pipeline(cls.spark, ROOT/'tests/fixtures/mini_world', out=cls.report_dir/'ddl-pipeline')
        cls.spark.conf.set('spark.sql.storeAssignmentPolicy', 'STRICT')
        warehouse = Path(cls.tmp.name)/'warehouse'
        cls.spark.sql("CREATE DATABASE ddl_contract_verification LOCATION '"+str(warehouse)+"'")
        cls.spark.sql('USE ddl_contract_verification')
        # Every semicolon followed by a blank line terminates a generated CREATE.
        # Quotes/comments within the CREATE remain unchanged when sent to Spark.
        # Spark's built-in SessionCatalog rejects persistent Chinese table names.
        # Use the already-supported English variant with explicit column mapping.
        text = generate('spark', naming='en', tables=cls.tables)
        for statement in text.strip().split(';\n\n'):
            cls.spark.sql(statement.rstrip(';'))

    @classmethod
    def tearDownClass(cls):
        cls.spark.sql('USE default')
        cls.spark.sql('DROP DATABASE ddl_contract_verification CASCADE')
        cls.spark.stop()
        cls.tmp.cleanup()

    def test_all_55_tables_load_strictly_keep_grain_and_read_back_exact_rows(self):
        report = {
            'spark_version': self.spark.version,
            'as_of': self.pipeline_report['as_of'],
            'seed': self.pipeline_report['seed'],
            'git_sha': self.pipeline_report['git_sha'],
            'worktree_dirty': self.pipeline_report['worktree_dirty'],
            'scope': '55 Spark DDL tables; 30 strict typed synthetic CSV inputs and 25 freshly executed ETL outputs; complete row readback and natural-grain uniqueness',
            'naming': 'en; explicit contract mapping from Chinese source table and column names',
            'other_engines': 'MySQL/PostgreSQL/ClickHouse parse verification only; no server execution',
            'tables': [],
        }
        try:
            self.assertEqual(55, sum(not table.isTemporary for table in self.spark.catalog.listTables('ddl_contract_verification')))
            for table, fields in self.tables.items():
                with self.subTest(table=table):
                    schema = declared_schema(fields)
                    expected_names = [f.name for f in schema]
                    if table.startswith(('dim_', 'dwd_', 'param_')):
                        path = ROOT/'tests/fixtures/mini_world'/(table+'.csv')
                        with path.open(encoding='utf-8-sig') as stream:
                            self.assertEqual(expected_names, next(csv.reader(stream)))
                        frame = (self.spark.read.option('header', True).option('enforceSchema', False)
                                 .option('mode', 'FAILFAST').option('nullValue', '\\N').option('emptyValue', '')
                                 .option('timestampFormat', 'yyyy-MM-dd HH:mm:ss').schema(schema).csv(str(path)))
                        source = 'strict typed mini_world CSV'
                    else:
                        frame = self.frames[table]
                        source = 'fresh documented ETL output'
                    self.assertEqual(expected_names, frame.columns)
                    self.assertEqual([f.dataType for f in schema], [f.dataType for f in frame.schema])
                    source_rows = frame.collect()  # forces FAILFAST across every source column
                    self.assertGreater(len(source_rows), 0)
                    keys = [f['字段名'] for f in sorted(fields, key=lambda f: int(f['粒度键序号'] or 999)) if f['粒度键序号']]
                    key_counts = Counter(tuple(row[key] for key in keys) for row in source_rows)
                    duplicates = [(key, count) for key, count in key_counts.items() if count > 1]
                    self.assertEqual([], duplicates, (table, keys))
                    english_table = fields[0]['表英文名']
                    english_columns = [f['字段英文名'] for f in fields]
                    target = self.spark.table('`'+english_table+'`')
                    self.assertEqual(english_columns, target.columns)
                    self.assertEqual([f.dataType for f in schema], [f.dataType for f in target.schema])
                    mapped = frame.toDF(*english_columns)
                    mapped.write.mode('append').insertInto('`'+english_table+'`')
                    readback = self.spark.table('`'+english_table+'`').collect()
                    self.assertEqual(Counter(tuple(row) for row in source_rows), Counter(tuple(row) for row in readback))
                    report['tables'].append({'table': table, 'persistent_table': english_table, 'source': source, 'columns': len(fields), 'rows_loaded': len(source_rows), 'rows_read_back': len(readback), 'grain': keys, 'duplicate_grains': len(duplicates), 'nullable_key_rows': sum(any(row[k] is None for k in keys) for row in source_rows), 'row_readback_exact': True})
            self.assertEqual(55, len(report['tables']))
            self.assertEqual(30, sum(e['source']=='strict typed mini_world CSV' for e in report['tables']))
            self.assertEqual(25, sum(e['source']=='fresh documented ETL output' for e in report['tables']))
            report['passed'] = True
        except Exception as exc:
            report['error'] = str(exc)
            raise
        finally:
            (self.report_dir/'ddl-readback-report.json').write_text(json.dumps(report, ensure_ascii=False, indent=2)+'\n')

    def test_malformed_input_fails_with_declared_type(self):
        table = 'dim_会员主档'
        with (ROOT/'tests/fixtures/mini_world'/(table+'.csv')).open() as stream:
            rows = list(csv.reader(stream))[:2]
        rows[1][rows[0].index('注册日期')] = 'not-a-date'
        bad = Path(self.tmp.name)/'malformed.csv'
        with bad.open('w', newline='') as stream:
            csv.writer(stream).writerows(rows)
        frame = self.spark.read.option('header', True).option('mode', 'FAILFAST').schema(declared_schema(self.tables[table])).csv(str(bad))
        self.spark.sparkContext.setLogLevel('OFF')
        try:
            with self.assertRaisesRegex(Exception, 'MALFORMED|Malformed|FAILFAST'):
                frame.collect()
        finally:
            self.spark.sparkContext.setLogLevel('ERROR')


if __name__ == '__main__':
    unittest.main()
