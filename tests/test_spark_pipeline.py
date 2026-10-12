"""Real Spark execution and independent differential verification."""
import csv
import importlib.util
import os
import tempfile
import unittest
from datetime import date, datetime
from pathlib import Path
from tests import test_fact_bridges_on_samples as bridge_samples
from tests.fact_bridges import touch_order_bridge,coupon_order_bridge,participation_order_bridge
from tests.task_generation import generate_rule_tasks
from tests.time_norms import store_calendar,store_month_skeleton,scd2_join
from tests.test_business_contracts import cohort_grid
from tools.run_pipeline import create_spark,run_pipeline,render_sql,register,load_inputs,parse_pipeline
from tools.build_mini_world import build_world

ROOT=Path(__file__).resolve().parents[1]
HAVE_SPARK=importlib.util.find_spec('pyspark') is not None
if os.environ.get('REQUIRE_SPARK')=='1' and not HAVE_SPARK:
    raise RuntimeError('REQUIRE_SPARK=1 but PySpark is unavailable')

@unittest.skipUnless(HAVE_SPARK,'PySpark optional; install requirements-spark.txt')
class SparkPipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark=create_spark()
        cls.tmp=tempfile.TemporaryDirectory()
        cls.out=Path(os.environ.get('SPARK_REPORT_DIR',cls.tmp.name+'/report'))
        cls.frames,cls.report=run_pipeline(cls.spark,ROOT/'tests/fixtures/mini_world',out=cls.out)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop();cls.tmp.cleanup()

    def rows(self,name):return [row.asDict() for row in self.frames[name].collect()]

    def test_all_25_outputs_are_nonempty_and_preserve_header_order(self):
        self.assertEqual(25,len(self.report['etls']))
        for entry in self.report['etls']:
            with self.subTest(table=entry['output']):
                self.assertGreater(entry['rows'],0)
                with (ROOT/'数据集/数据样本'/f"{entry['output']}.csv").open(encoding='utf-8-sig') as f:
                    self.assertEqual(next(csv.reader(f)),entry['columns'])

    def test_invalid_typed_csv_fails_and_member_ids_keep_null_distinctions(self):
        bad=Path(self.tmp.name)/'malformed';bad.mkdir(exist_ok=True)
        with (ROOT/'tests/fixtures/mini_world/dwd_订单.csv').open() as f:
            rows=list(csv.DictReader(f))
        columns=list(rows[0]);rows[0]['下单时间']='not-a-timestamp'
        with (bad/'dwd_订单.csv').open('w',newline='') as f:
            writer=csv.DictWriter(f,fieldnames=columns);writer.writeheader();writer.writerow(rows[0])
        self.spark.sparkContext.setLogLevel('OFF')
        try:
            with self.assertRaisesRegex(Exception,'MALFORMED|Malformed|FAILFAST'):
                load_inputs(self.spark,bad)
        finally:
            self.spark.sparkContext.setLogLevel('ERROR')
        ids={r['订单ID']:r['会员ID'] for r in self.rows('dwd_订单')}
        self.assertIsNone(ids['O900004'])
        self.assertEqual('',ids['O900005'])
        self.assertEqual('null',ids['O900006'])

    def test_report_records_nodes_and_public_execution(self):
        self.assertGreater(sum(e['nodes_executed'] for e in self.report['etls']),140)
        for entry in self.report['etls']:
            self.assertEqual(entry['nodes_executed'],len(entry['nodes']))
            done=set()
            for node in entry['nodes']:
                self.assertEqual('reused_input_checkpoint' if node['kind']=='INPUT_DATASET' else 'eager_local_checkpoint',node['materialization'])
                if node['kind']!='INPUT_DATASET' and entry['nodes_executed']>1:
                    self.assertTrue(set(node['sources']) <= done)
                done.add(node['id'])
        self.assertEqual(7,len(self.report['public_sql']))
        for entry in self.report['public_sql']:
            self.assertGreater(entry['rows'],0)
            self.assertTrue(entry['schema']['fields'])
            self.assertGreaterEqual(entry['seconds'],0)

    def test_missing_member_placeholders_keep_order_sales_without_creating_members(self):
        # Catches either forgotten member filters or filtering unknown orders out
        # of total sales. Raw values stay distinct; only membership use is rejected.
        invalid={None,'','null','NULL',' null ','\t','\tNULL\t','\u00a0null\u00a0','\u00a0','   '}
        orders=[r for r in self.rows('dwd_订单') if r['订单状态']=='已完成']
        unknown=[r for r in orders if r['会员ID'] in invalid]
        self.assertEqual(12,len(unknown))
        self.assertEqual(1200,sum(r['实付金额'] for r in unknown))
        for table in ('dws_会员RFM分层','dws_会员生命周期','dws_渠道迁移分析',
                      'bridge_触达订单归因','bridge_活动参与订单归因',
                      'bridge_规则任务生成','ads_会员经营任务池'):
            with self.subTest(table=table):
                self.assertFalse(any(r['会员ID'] in invalid for r in self.rows(table)))
        expected_total=sum(r['实付金额'] for r in orders)
        expected_member=sum(r['实付金额'] for r in orders if r['会员ID'] not in invalid)
        for table in ('ads_高层经营驾驶舱','ads_会员私域驾驶舱'):
            rows=self.rows(table)
            self.assertEqual(expected_total,sum(r['总销售'] for r in rows),table)
            self.assertEqual(expected_member,sum(r['会员销售'] for r in rows),table)
            self.assertEqual(len(orders),sum(r['总订单数'] for r in rows),table)
        daily=self.rows('ads_高层经营驾驶舱')
        for row in daily:
            source=[o for o in orders if o['业务日期']==row['业务日期']]
            self.assertEqual(len({o['会员ID'] for o in source if o['会员ID'] not in invalid}),row['活跃会员数'])
        self.assertIn(('O900006','C_NULL'),{(r['订单ID'],r['券ID']) for r in self.rows('bridge_券核销订单')})

    def test_intermediate_runtime_error_cannot_be_pruned_by_downstream_projection(self):
        isolated=Path(self.tmp.name)/'pruned-error'
        docs=isolated/'ETL/逻辑SQL';docs.mkdir(parents=True)
        fixture=isolated/'fixtures';fixture.mkdir()
        (isolated/'.git').symlink_to(ROOT/'.git',target_is_directory=True)
        with (ROOT/'tests/fixtures/mini_world/dwd_订单.csv').open() as f:
            rows=list(csv.reader(f))
        with (fixture/'dwd_订单.csv').open('w',newline='') as f:
            csv.writer(f).writerows(rows[:2])
        (docs/'pruned.md').write_text("""### 节点1
- Id: base
- Name: dwd_订单
- Type: INPUT_DATASET
- 等价SQL:
```sql
SELECT * FROM input
```
### 节点2
- Id: bad
- Name: runtime-error
- Type: SQL_SCRIPT
- **Sources (Inputs):**
  - base (dwd_订单)
- 等价SQL:
```sql
SELECT *, raise_error('unused-stage-guard') AS unused_column FROM input1
```
### 节点3
- Id: out
- Name: test-output
- Type: OUTPUT_DATASET
- **Sources (Inputs):**
  - bad (runtime-error)
- OutputDsName: dws_test
- 等价SQL:
```sql
SELECT `订单ID` FROM input1
```
""")
        error=None
        self.spark.sparkContext.setLogLevel('OFF')
        try:
            run_pipeline(self.spark,fixture,root=isolated)
        except Exception as exc:
            error=exc
        finally:
            self.spark.sparkContext.setLogLevel('ERROR')
        self.assertIsNotNone(error)
        self.assertIn('unused-stage-guard',str(error))
        self.assertIn('node bad',str(error))

    def test_all_19_clean_business_checks_are_zero(self):
        self.assertEqual(19,len(self.report['checks']))
        self.assertEqual({str(i):'0' for i in range(10,29)},{r['序号']:r['实际值'] for r in self.report['checks']})

    def test_three_fact_bridges_match_python_by_order_and_event(self):
        bridge_samples.SampleBridgeTests.setUpClass()
        source=bridge_samples.SampleBridgeTests
        pairs=[('bridge_触达订单归因','触达ID','touch_id',touch_order_bridge(source.orders,source.touches)),('bridge_券核销订单','券ID','coupon_id',coupon_order_bridge(source.orders,source.coupons)),('bridge_活动参与订单归因','参与ID','participation_id',participation_order_bridge(source.orders,source.parts))]
        for table,col,key,expected in pairs:
            with self.subTest(table=table):
                self.assertGreater(len(expected),0)
                self.assertEqual({(r['order_id'],r[key]) for r in expected},{(r['订单ID'],r[col]) for r in self.rows(table)})

    def test_rule_tasks_match_python_per_member(self):
        life=[dict(member_id=r['会员ID'],snapshot=r['数据快照日期'],stage=r['生命周期阶段'],store_id=r['注册门店ID'],amount=r['总消费金额'],orders=r['总订单数']) for r in self.rows('dws_会员生命周期')]
        channels=[dict(member_id=r['会员ID'],snapshot=r['数据快照日期'],migrate=r['迁移类型']) for r in self.rows('dws_渠道迁移分析')]
        rfm=[dict(member_id=r['会员ID'],snapshot=r['数据快照日期'],label=r['RFM标签'],r_score=r['R分'],amount=r['消费金额'],orders=r['消费次数']) for r in self.rows('dws_会员RFM分层')]
        reviews=[dict(member_id=r['会员ID'],biz_date=r['评价日期'],score=r['评分'],reply=r['回复状态'],tag=r['负评标签'],review_id=r['评价ID'],store_id=r['门店ID']) for r in self.rows('dwd_评价')]
        coupons=[dict(member_id=r['会员ID'],issued=r['发放日期'],expires=r['失效日期'],redeemed=r['核销日期'],coupon_id=r['券ID'],store_id=r['门店ID']) for r in self.rows('dwd_券事件')]
        touches=[dict(member_id=r['会员ID'],biz_date=r['触达日期']) for r in self.rows('dwd_会员触达')]
        expected=generate_rule_tasks(life,channels,rfm,reviews,coupons,touches)
        self.assertEqual({(r['member_id'],r['task_type'],r['priority'],r['holdout']) for r in expected},{(r['会员ID'],r['任务类型'],r['任务优先级'],r['是否对照组']) for r in self.rows('bridge_规则任务生成')})

    def test_calendar_months_and_scd2_match_python(self):
        current=[dict(store_id=r['门店ID'],opened=r['开业日期'],closed=date.fromisoformat(r['闭店日期']) if r['闭店日期'] else None) for r in self.rows('dim_门店主档') if r['当前版本标记']==1]
        self.assertEqual({(r['store_id'],r['biz_date']) for r in store_calendar(current)},{(r['门店ID'],r['业务日期']) for r in self.rows('calendar_门店营业日历')})
        self.assertEqual({(r['store_id'],r['month'],r['as_of_date']) for r in store_month_skeleton(current)},{(r['门店ID'],r['月份日期'],r['维度命中日期']) for r in self.rows('calendar_门店月份骨架')})
        versions=[dict(store_id=r['门店ID'],version_id=r['门店版本ID'],start=r['生效起始日期'],end=r['生效截止日期']) for r in self.rows('dim_门店主档')]
        facts=[dict(store_id=s,biz_date=d) for s,d in {(r['门店ID'],r['业务日期']) for r in self.rows('dwd_订单')}]
        self.assertEqual({(r['store_id'],r['biz_date'],r['version_id']) for r in scd2_join(facts,versions)},{(r['门店ID'],r['事实日期'],r['门店版本ID']) for r in self.rows('scd2_门店时点关联')})

    def test_documented_loss_streak_resets_at_an_explicit_month_gap(self):
        from pyspark.sql.functions import col
        flow=next(f for f in parse_pipeline() if f.output=='ads_单店利润健康')
        node=next(n for n in flow.nodes if n.name=='连续亏损窗口分析')
        gap=self.frames['ads_单店利润健康'].filter((col('门店ID')=='S900001') & col('月份').isin('2026-04','2026-06'))
        register(gap,'input1')
        result=self.spark.sql(render_sql(node.sql,'2026-06-24')).collect()
        self.assertEqual({'2026-04':1,'2026-06':1},{r['月份']:r['连续亏损月数'] for r in result})

    def test_cohort_grid_matches_python(self):
        members=[dict(member_id=r['会员ID'],registered=r['注册日期']) for r in self.rows('dim_会员主档')]
        orders=[dict(member_id=r['会员ID'],date=r['业务日期'],status=r['订单状态']) for r in self.rows('dwd_订单')]
        expected=cohort_grid(members,orders,date(2026,6,24),max_month=11)
        expected=[r for r in expected if (r['cohort'].year*12+r['cohort'].month+r['n']) <= (2026*12+6)]
        actual=self.rows('dws_会员同期群留存')
        self.assertEqual({(r['cohort'],f"M{r['n']}",r['size'],r['retained'],int(r['complete'])) for r in expected},{(r['同期群月份'],r['留存月份序号'],r['同期群人数'],r['留存人数'],r['是否完整观察期']) for r in actual})
        for r in actual:
            if r['留存人数'] is None:self.assertIsNone(r['留存率'])
            else:self.assertAlmostEqual(round(r['留存人数']/r['同期群人数'],4),r['留存率'])

    def test_hand_calculated_known_answers(self):
        with (ROOT/'tests/fixtures/mini_world/expected.csv').open() as f:expectations=list(csv.DictReader(f))
        for e in expectations:
            with self.subTest(object=e['埋点对象'],field=e['字段']):
                keys=e['键字段'].split('|');values=e['埋点对象'].split('|')
                rows=[r for r in self.rows(e['表']) if all(str(r[k])==v for k,v in zip(keys,values))]
                if e['字段']=='__row_count__':self.assertEqual(int(e['期望值']),len(rows));continue
                self.assertEqual(1,len(rows))
                value=rows[0][e['字段']]
                if isinstance(value,(int,float)):self.assertAlmostEqual(float(e['期望值']),value)
                else:self.assertEqual(e['期望值'],str(value))

    def test_alternate_snapshot_and_adversarial_detection(self):
        # Raw future rows are deliberate: 21 must detect them; 20 must detect overlap.
        adverse=Path(self.tmp.name)/'adverse';build_world(adverse,scenario='adversarial')
        frames,report=run_pipeline(self.spark,adverse,out=Path(self.tmp.name)/'adverse-report')
        checks={r['序号']:int(r['实际值']) for r in report['checks']}
        self.assertGreater(checks['20'],0);self.assertEqual(4,checks['21'])
        for bridge in ('bridge_触达订单归因','bridge_券核销订单','bridge_活动参与订单归因'):
            self.assertNotIn('O999999',{r['订单ID'] for r in frames[bridge].collect()})
        hit=[r for r in frames['scd2_门店时点关联'].collect() if r['门店ID']=='S900001' and r['事实日期']==date(2026,6,23)]
        self.assertEqual('S900001V2',hit[0]['门店版本ID'])
        alternate=date(2026,5,31)
        frames,report=run_pipeline(self.spark,ROOT/'tests/fixtures/mini_world',as_of=alternate,out=Path(self.tmp.name)/'alternate-report')
        facts={'业务日期','订单日期','订单发生日期','触达日期','核销日期','发放日期','参与日期','评价日期','末单日期','首单日期','最近消费日期','参数生效日期','注册日期'}
        for entry in report['etls']:
            with self.subTest(table=entry['output']):
                self.assertGreater(entry['rows'],0)
                for row in frames[entry['output']].collect():
                    if '数据快照日期' in row.asDict():self.assertEqual(alternate,row['数据快照日期'])
                    for key in facts & set(row.asDict()):
                        value=row[key]
                        if isinstance(value,datetime):value=value.date()
                        if isinstance(value,date):self.assertLessEqual(value,alternate)

if __name__=='__main__':unittest.main()
