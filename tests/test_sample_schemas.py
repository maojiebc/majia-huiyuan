from __future__ import annotations

import csv
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SAMPLE_DIR = ROOT / "数据集" / "数据样本"
AS_OF_DATE = "2026-06-24"


def columns(value: str) -> list[str]:
    return value.split(",")


V141_SAMPLE_SCHEMAS = {
    "ads_会员私域驾驶舱": columns(
        "年月,当月活跃会员,新增首单会员,总销售,会员销售,到店销售,总订单数,触达次数,触达会员数,会员销售占比,到店销售额占比,数据快照日期"
    ),
    "ads_会员经营任务池": columns(
        "任务ID,任务优先级,任务类型,任务来源,会员ID,会员等级,会员城市,归属门店ID,门店名称,门店城市,店型,门店类型,员工导购ID,员工姓名,岗位,角色标签,人群标签,推荐动作,推荐权益,推荐原因,预计价值,任务生成时间,任务截止时间,任务失效时间,触达状态,触达时间,触达方式,触达后下单,触达后下单金额,任务结果,执行状态,转化阶段,数据快照日期"
    ),
    "ads_加盟商单店报告": columns(
        "门店ID,门店名称,省份,城市,城市层级,门店类型,商圈,加盟商ID,加盟商名称,加盟商类型,合作状态,信用等级,签约日期,月份,月营收,堂食营收,外卖营收,订单数,毛利,店面贡献利润,单店净利润,毛利率,店面贡献利润率,堂食占比,外卖占比,人工占比,房租占比,客单价,同侪门店数,城市同店型_营收_P25,城市同店型_营收_中位数,城市同店型_营收_P75,城市同店型_利润率_P25,城市同店型_利润率_中位数,城市同店型_利润率_P75,城市同店型_堂食占比_中位数,营收_对中位数比,利润率_对中位数差,堂食占比_对中位数差,总投资额,累计店面贡献利润,累计回本率,预计完整回本月数,已开业月数,投资起始日,剩余回本月数,回本状态,预计完整回本日期,招商承诺回本月数,回本偏离度,回本风险等级,标杆门店标志,本店位置标签,可改进项,总部本月支持,数据快照日期"
    ),
    "ads_单店利润健康": columns(
        "门店ID,门店名称,省份,城市,城市层级,门店类型,商圈,直营加盟类型,月份,月营收,毛利,店面贡献利润,单店净利润,毛利率,店面贡献利润率,单店净利率,堂食占比,外卖占比,人工占比,房租占比,客单价,房租占比上限,人工占比上限,堂食占比下限,持续亏损预警月数,毛利率塌方阈值pp,适用范围,房租超标,人工超标,堂食衰减,本月亏损,利润健康等级,预警条数,建议动作,历史亏损月数,近3月有亏损,连续亏损月数,持续亏损标签,数据快照日期"
    ),
    "ads_异常归因清单": columns(
        "业务日期,粒度,门店ID,门店名称,门店类型,城市,店长姓名,区域经理,异常来源,异常类型,异常详情,风险等级,豁免标记,处理状态,建议动作,数据快照日期"
    ),
    "ads_活动权益复盘": columns(
        "活动ID,活动名称,活动类型,活动渠道,开始日期,结束日期,预算,券发放数,券核销数,已记录权益成本,核销订单数,核销订单GMV,触达人数,查看人数,活动参与人数,触达后关联下单人数,触达后关联订单数,触达后关联GMV,参与后关联下单人数,参与后关联订单数,参与后关联GMV,券核销率,打开率,触达后关联下单率,参与后关联下单率,核销GMV成本比,增量GMV,增量ROI,增量测算状态,归因窗口天数,触达归因规则,活动参与归因规则,数据快照日期"
    ),
    "ads_门店每日指挥台": columns(
        "门店ID,门店名称,省份,城市,城市层级,店型,门店类型,商圈,是否90天内新店,新店标签,业务日期,订单数,销售额,平均客单价,会员订单数,到店订单数,外卖订单数,会员订单占比,到店占比,折扣率,当日评分,未回复负评数,今日异常,数据快照日期"
    ),
    "ads_高层经营驾驶舱": columns(
        "业务日期,总销售,会员销售,到店销售,总订单数,活跃会员数,新增会员数,触达后关联订单数,触达后关联销售,会员销售占比,到店销售额占比,触达后关联销售占比,归因窗口天数,归因规则,数据快照日期"
    ),
    "dws_会员RFM分层": columns(
        "会员ID,会员等级,注册渠道,城市,最近消费日期,距今天数,消费次数,消费金额,R分,F分,M分,RFM总分,RFM标签,数据快照日期"
    ),
    "dws_会员同期群留存": columns(
        "同期群月份,同期群人数,留存月份序号,留存月份,留存人数,留存率,是否完整观察期,数据快照日期"
    ),
    "dws_会员生命周期": columns(
        "会员ID,会员等级,注册日期,注册渠道,注册门店ID,城市,注册天数,首单日期,末单日期,总订单数,总消费金额,近30天订单,近7天订单,距末单天数,生命周期阶段,活跃天数上限,沉睡天数上限,流失天数上限,参数生效日期,参数状态,数据快照日期"
    ),
    "dws_体验口碑汇总": columns(
        "门店ID,门店名称,省份,城市,城市层级,店型,门店类型,业务日期,评价数,平均评分,负评数,好评数,未回复负评数,负评率,投诉数,待处理投诉,平均处理时长,体验风险等级,数据快照日期"
    ),
    "dws_券效益分析": columns(
        "券模板ID,券名称,券类型,优惠形式,发放日期,发放数,核销数,核销率,已核销优惠成本,核销订单GMV,核销订单数,核销GMV成本比,增量GMV,增量ROI,增量测算状态,订单归因规则,数据快照日期"
    ),
    "dws_加盟商经营汇总": columns(
        "加盟商ID,加盟商名称,加盟商类型,签约省份,合作状态,信用等级,入网日期,月份,经营门店数,月总营收,月总堂食营收,月总外卖营收,月总订单数,月总毛利,月总店面贡献利润,月总单店净利润,平均贡献利润率,平均堂食占比,亏损门店数,盈利门店数,门均营收,门均贡献利润,亏损率,经营健康等级,续约风险,数据快照日期"
    ),
    "dws_加盟回本测算": columns(
        "门店ID,加盟商ID,门店类型,签约日期,合同期年数,到期日,分成模型,续约状态,总投资额,可回收投资,不可回收投资,投资起始日,经营月数,累计营收,累计店面贡献利润,月均店面贡献利润,平均贡献利润率,累计回本率,预计完整回本月数,已开业月数,剩余回本月数,回本状态,预计完整回本日期,招商承诺回本月数,回本偏离度,回本风险等级,标杆门店标志,数据快照日期"
    ),
    "dws_单店利润月汇总": columns(
        "门店ID,门店名称,省份,城市,城市层级,门店类型,商圈,品牌线,直营加盟类型,是否90天内新店,新店标签,开业日期,月份,月营收,堂食营收,外卖营收,订单数,原材料成本,包材成本,平台抽佣,人工成本,房租物业,能耗水电,设备折旧,总部分摊,变动成本合计,半固定成本合计,固定成本合计,成本总计,毛利,店面贡献利润,单店净利润,毛利率,店面贡献利润率,单店净利率,堂食占比,外卖占比,人工占比,房租占比,客单价,数据快照日期"
    ),
    "dws_员工导购效能": columns(
        "员工ID,姓名,归属门店ID,岗位,角色标签,周起始,触达数,触达会员数,查看数,任务数,已触达任务,转化任务数,转化金额,任务完成率,触达后转化率,数据快照日期"
    ),
    "dws_商品销售分析": columns(
        "一级类目,二级类目,商品ID,商品名称,省份,城市,城市层级,店型,销售渠道,是否到店,业务日期,订单数,销量,销售额,总成本,毛利,数据快照日期"
    ),
    "dws_成本结构汇总": columns(
        "门店类型,月份,成本科目ID,成本科目名称,成本大类,门店数,成本总额,营收总额,平均占比,中位数占比,P90占比,最低占比,最高占比,科目营收占比,离散度,P90vsP50差异,数据快照日期"
    ),
    "dws_新店爬坡_Comp老店": columns(
        "门店ID,门店名称,城市,城市层级,店型,商圈,新店标签,是否90天内新店,开业日期,业务日期,订单数,销售额,会员订单数,开业天数,爬坡阶段,门店成长类型,平均客单价,会员订单占比,数据快照日期"
    ),
    "dws_渠道迁移分析": columns(
        "会员ID,会员等级,城市,近30天堂食,近30天外卖,前60天堂食,前60天外卖,迁移类型,数据快照日期"
    ),
    "dws_目标达成": columns(
        "门店ID,年月,目标指标,目标值,实际值,达成率,目标缺口,数据快照日期"
    ),
    "dws_私域转化漏斗": columns(
        "活动ID,活动名称,活动类型,业务日期,触达渠道,触达人次,查看人次,触达人数,打开率,触达后关联下单人数,触达后关联订单数,触达后关联GMV,归因窗口天数,归因规则,数据快照日期"
    ),
    "dws_门店日报": columns(
        "门店ID,门店名称,省份,城市,城市层级,店型,门店类型,商圈,品牌线,直营加盟类型,是否90天内新店,新店标签,业务日期,订单数,销售额,原价销售额,折扣金额合计,商品件数合计,会员订单数,到店销售额,外卖销售额,到店订单数,外卖订单数,去重会员数,平均客单价,会员订单占比,折扣率,到店占比,外卖占比,平均件数,门店版本ID,数据快照日期"
    ),
}


REMOVED_MISLEADING_FIELDS = {
    "ads_会员私域驾驶舱": {"到店销售占比"},
    "ads_高层经营驾驶舱": {"私域贡献销售", "到店订单占比", "私域贡献收入占比"},
    "ads_活动权益复盘": {"总折扣", "转化人数", "拉动销售", "总体转化率", "券ROI"},
    "dws_会员同期群留存": {"注册月份", "留存桶"},
    "dws_券效益分析": {"业务日期", "总折扣金额", "拉动销售额", "拉动订单数", "ROI"},
    "dws_私域转化漏斗": {"下单人数", "下单金额", "整体转化率", "查看转化率"},
}


class SampleSchemaTests(unittest.TestCase):
    def read_csv(self, name: str) -> list[list[str]]:
        with (SAMPLE_DIR / f"{name}.csv").open(encoding="utf-8-sig", newline="") as handle:
            return list(csv.reader(handle))

    def test_v141_changed_sample_headers_match_contract(self):
        self.assertEqual(24, len(V141_SAMPLE_SCHEMAS))
        for name, expected in V141_SAMPLE_SCHEMAS.items():
            with self.subTest(dataset=name):
                rows = self.read_csv(name)
                self.assertTrue(rows, f"样本为空: {name}")
                self.assertEqual(expected, rows[0], f"v1.4.1 样本表头漂移: {name}")

    def test_every_sample_row_matches_its_header_width(self):
        for path in sorted(SAMPLE_DIR.glob("*.csv")):
            with self.subTest(dataset=path.stem):
                rows = self.read_csv(path.stem)
                self.assertTrue(rows, f"样本为空: {path.name}")
                width = len(rows[0])
                for line_number, row in enumerate(rows[1:], start=2):
                    self.assertEqual(width, len(row), f"{path.name}:{line_number} 行宽错误")

    def test_legacy_misleading_fields_are_removed(self):
        for name, forbidden in REMOVED_MISLEADING_FIELDS.items():
            with self.subTest(dataset=name):
                header = set(self.read_csv(name)[0])
                self.assertTrue(forbidden.isdisjoint(header), f"仍含旧误导字段: {name}")

    def test_changed_samples_share_one_snapshot_date(self):
        for name, schema in V141_SAMPLE_SCHEMAS.items():
            with self.subTest(dataset=name):
                rows = self.read_csv(name)
                snapshot_index = schema.index("数据快照日期")
                self.assertGreater(len(rows), 1, f"样本无数据行: {name}")
                self.assertEqual({AS_OF_DATE}, {row[snapshot_index] for row in rows[1:]})

    def test_incremental_metrics_stay_null_without_control_group(self):
        for name in ("ads_活动权益复盘", "dws_券效益分析"):
            with self.subTest(dataset=name):
                rows = self.read_csv(name)
                header = rows[0]
                for row in rows[1:]:
                    self.assertEqual("", row[header.index("增量GMV")])
                    self.assertEqual("", row[header.index("增量ROI")])


if __name__ == "__main__":
    unittest.main()
