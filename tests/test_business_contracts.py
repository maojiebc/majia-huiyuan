from __future__ import annotations

import calendar
import csv
import json
import re
import unittest
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
AS_OF = date(2026, 6, 24)


def add_months(value: date, months: int) -> date:
    index = value.year * 12 + value.month - 1 + months
    year, month0 = divmod(index, 12)
    day = min(value.day, calendar.monthrange(year, month0 + 1)[1])
    return date(year, month0 + 1, day)


def month_start(value: date) -> date:
    return value.replace(day=1)


def latest_touch_attribution(orders, touches, as_of: date, window_days: int = 7):
    priority = {"企微1V1": 1, "短信": 2, "Push": 3}
    result = {}
    for order in orders:
        if order["status"] != "已完成" or order["time"].date() > as_of:
            continue
        candidates = [
            touch
            for touch in touches
            if touch["member_id"] == order["member_id"]
            and touch["status"] == "已发送"
            and touch["time"] <= order["time"] < touch["time"] + timedelta(days=window_days + 1)
        ]
        if candidates:
            result[order["order_id"]] = max(
                candidates,
                key=lambda t: (t["time"], -priority.get(t["channel"], 9), t["touch_id"]),
            )
    return result


def cohort_grid(members, orders, as_of: date, max_month: int = 3):
    cohorts = defaultdict(list)
    for member in members:
        cohorts[month_start(member["registered"])].append(member["member_id"])
    completed_order_months = defaultdict(set)
    for order in orders:
        if order["status"] == "已完成" and order["date"] <= as_of:
            completed_order_months[order["member_id"]].add(month_start(order["date"]))
    rows = []
    for cohort_month, member_ids in cohorts.items():
        for n in range(max_month + 1):
            target_month = add_months(cohort_month, n)
            observed_through = add_months(target_month, 1) - timedelta(days=1)
            complete = as_of >= observed_through
            retained = (
                len(member_ids)
                if n == 0
                else (
                    sum(target_month in completed_order_months[member_id] for member_id in member_ids)
                    if complete
                    else None
                )
            )
            rows.append(
                {
                    "cohort": cohort_month,
                    "n": n,
                    "size": len(member_ids),
                    "retained": retained,
                    "rate": retained / len(member_ids) if retained is not None else None,
                    "complete": complete,
                }
            )
    return rows


def store_month_skeleton(opened: date, closed: date | None, as_of: date):
    end = min(closed or as_of, as_of)
    current = month_start(opened)
    months = []
    while current <= month_start(end):
        months.append(current)
        current = add_months(current, 1)
    return months


def loss_streaks(monthly_profit):
    result = []
    previous_month = None
    streak = 0
    for month, profit in sorted(monthly_profit):
        consecutive = previous_month is not None and add_months(previous_month, 1) == month
        streak = streak + 1 if profit < 0 and consecutive else (1 if profit < 0 else 0)
        result.append((month, streak))
        previous_month = month
    return result


class ReferenceLogicTests(unittest.TestCase):
    def test_one_order_has_at_most_one_latest_touch(self):
        touches = [
            {"touch_id": "T1", "member_id": "M1", "time": datetime(2026, 6, 1, 9), "channel": "短信", "status": "已发送"},
            {"touch_id": "T2", "member_id": "M1", "time": datetime(2026, 6, 3, 9), "channel": "企微1V1", "status": "已发送"},
        ]
        orders = [
            {"order_id": "O1", "member_id": "M1", "time": datetime(2026, 6, 4, 12), "status": "已完成", "gmv": 80},
            {"order_id": "O2", "member_id": "M1", "time": datetime(2026, 5, 31, 12), "status": "已完成", "gmv": 50},
        ]
        attributed = latest_touch_attribution(orders, touches, AS_OF)
        self.assertEqual({"O1": "T2"}, {order_id: touch["touch_id"] for order_id, touch in attributed.items()})
        self.assertEqual(len(attributed), len(set(attributed)))

    def test_attributed_gmv_cannot_exceed_eligible_order_gmv(self):
        orders = [
            {"order_id": "O1", "member_id": "M1", "time": datetime(2026, 6, 4, 12), "status": "已完成", "gmv": 80},
            {"order_id": "O2", "member_id": "M2", "time": datetime(2026, 6, 4, 12), "status": "已完成", "gmv": 50},
        ]
        touches = [{"touch_id": "T1", "member_id": "M1", "time": datetime(2026, 6, 1, 9), "channel": "短信", "status": "已发送"}]
        attributed = latest_touch_attribution(orders, touches, AS_OF)
        attributed_gmv = sum(order["gmv"] for order in orders if order["order_id"] in attributed)
        eligible_gmv = sum(order["gmv"] for order in orders if order["status"] == "已完成")
        self.assertLessEqual(attributed_gmv, eligible_gmv)

    def test_attribution_window_includes_day_seven_but_excludes_day_eight(self):
        touches = [
            {"touch_id": "T1", "member_id": "M1", "time": datetime(2026, 6, 1, 9), "channel": "短信", "status": "已发送"}
        ]
        orders = [
            {"order_id": "O7", "member_id": "M1", "time": datetime(2026, 6, 8, 21), "status": "已完成", "gmv": 80},
            {"order_id": "O8", "member_id": "M1", "time": datetime(2026, 6, 9, 9), "status": "已完成", "gmv": 50},
        ]
        attributed = latest_touch_attribution(orders, touches, AS_OF)
        self.assertIn("O7", attributed)
        self.assertNotIn("O8", attributed)

    def test_coupon_redemption_must_be_inside_instance_validity(self):
        coupons = [
            {"coupon_id": "C1", "issued": date(2026, 6, 1), "expires": date(2026, 6, 10), "redeemed": date(2026, 6, 5)},
            {"coupon_id": "C2", "issued": date(2026, 6, 1), "expires": date(2026, 6, 10), "redeemed": date(2026, 6, 11)},
            {"coupon_id": "C3", "issued": date(2026, 6, 5), "expires": date(2026, 6, 10), "redeemed": date(2026, 6, 4)},
        ]
        valid = {
            coupon["coupon_id"]
            for coupon in coupons
            if coupon["redeemed"] is not None
            and coupon["issued"] <= coupon["redeemed"] <= coupon["expires"]
            and coupon["redeemed"] <= AS_OF
        }
        self.assertEqual({"C1"}, valid)

    def test_cohort_m0_bounds_and_right_censoring(self):
        members = [
            {"member_id": "M1", "registered": date(2026, 1, 5)},
            {"member_id": "M2", "registered": date(2026, 1, 8)},
        ]
        orders = [
            {"member_id": "M1", "date": date(2026, 2, 2), "status": "已完成"},
            {"member_id": "M2", "date": date(2026, 2, 3), "status": "已取消"},
        ]
        rows = cohort_grid(members, orders, date(2026, 3, 15), max_month=2)
        m0, m1, m2 = rows
        self.assertEqual(m0["size"], m0["retained"])
        self.assertTrue(all(row["retained"] <= row["size"] for row in rows if row["retained"] is not None))
        self.assertEqual(1, m1["retained"])
        self.assertFalse(m2["complete"])
        self.assertIsNone(m2["retained"])
        self.assertIsNone(m2["rate"])

    def test_zero_sales_month_with_cost_remains_in_profit_skeleton(self):
        months = store_month_skeleton(date(2026, 1, 12), None, date(2026, 3, 31))
        revenue = {date(2026, 1, 1): 1000, date(2026, 3, 1): 900}
        costs = {date(2026, 2, 1): 300}
        rows = [(month, revenue.get(month, 0), costs.get(month, 0)) for month in months]
        self.assertIn((date(2026, 2, 1), 0, 300), rows)

    def test_loss_streak_requires_consecutive_calendar_months(self):
        streaks = loss_streaks(
            [(date(2026, 1, 1), -10), (date(2026, 2, 1), -20), (date(2026, 3, 1), 5), (date(2026, 5, 1), -30)]
        )
        self.assertEqual([1, 2, 0, 1], [streak for _, streak in streaks])

    def test_payback_date_uses_investment_start_and_remaining_months(self):
        investment_start = date(2025, 1, 15)
        as_of = date(2026, 1, 15)
        full_months = 24
        expected_payback = add_months(investment_start, full_months)
        elapsed = (as_of.year - investment_start.year) * 12 + as_of.month - investment_start.month
        remaining = max(full_months - elapsed, 0)
        self.assertEqual(date(2027, 1, 15), expected_payback)
        self.assertEqual(12, remaining)
        self.assertGreaterEqual(expected_payback, investment_start)

    def test_scd2_point_in_time_join_preserves_fact_count(self):
        versions = [
            {"store_id": "S1", "start": date(2025, 1, 1), "end": date(2025, 12, 31), "type": "社区店"},
            {"store_id": "S1", "start": date(2026, 1, 1), "end": None, "type": "商场店"},
        ]
        facts = [{"id": "O1", "store_id": "S1", "date": date(2025, 8, 1)}, {"id": "O2", "store_id": "S1", "date": date(2026, 2, 1)}]
        matches = []
        for fact in facts:
            hit = [v for v in versions if v["store_id"] == fact["store_id"] and v["start"] <= fact["date"] <= (v["end"] or date.max)]
            self.assertEqual(1, len(hit))
            matches.append((fact["id"], hit[0]["type"]))
        self.assertEqual(len(facts), len(matches))
        self.assertEqual([("O1", "社区店"), ("O2", "商场店")], matches)

    def test_all_fact_dates_are_not_later_than_snapshot(self):
        fact_dates = [date(2026, 6, 1), date(2026, 6, 24)]
        self.assertTrue(all(value <= AS_OF for value in fact_dates))


class RepositoryContractTests(unittest.TestCase):
    def read(self, relative: str) -> str:
        return (ROOT / relative).read_text(encoding="utf-8")

    def test_common_bridges_enforce_order_uniqueness(self):
        for relative in (
            "ETL/公共口径/01_触达订单归因桥.sql",
            "ETL/公共口径/02_券实例核销订单桥.sql",
            "ETL/公共口径/03_活动参与订单归因桥.sql",
        ):
            sql = self.read(relative)
            self.assertRegex(sql, r"PARTITION BY o\.`订单ID`|PARTITION BY `订单ID`")
            self.assertIn("WHERE rn = 1", sql)
            self.assertIn("as_of_date", sql)
            self.assertIn("rn AS `归因优先级`", sql)

    def test_release_inventory_and_versions_are_consistent(self):
        with (ROOT / "清单/数据集清单.csv").open(encoding="utf-8-sig", newline="") as handle:
            inventory = list(csv.DictReader(handle))
        names = {row["名称"] for row in inventory}
        samples = {path.stem for path in (ROOT / "数据集/数据样本").glob("*.csv")}
        structures = {path.stem for path in (ROOT / "数据集/结构定义").glob("*.md")}

        self.assertEqual(55, len(inventory))
        self.assertEqual(55, len(names))
        self.assertEqual(names, samples)
        self.assertEqual(names, structures)
        self.assertEqual(25, len(list((ROOT / "ETL/逻辑SQL").glob("*.md"))))

        manifest = json.loads(self.read("manifest.json"))
        self.assertEqual("1.4.1", manifest["version"])
        for relative in ("README.md", "README.en.md", "SKILL.md", "llms.txt"):
            self.assertIn("1.4.1", self.read(relative), relative)

    def test_common_bridges_match_etl_window_and_coupon_validity(self):
        touch_bridge = self.read("ETL/公共口径/01_触达订单归因桥.sql")
        participation_bridge = self.read("ETL/公共口径/03_活动参与订单归因桥.sql")
        activity = self.read("ETL/逻辑SQL/etl_ads_活动权益复盘 (17节点·C+G+F+C+G×2+S+J×3+C).md")
        funnel = self.read("ETL/逻辑SQL/etl_dws_私域转化漏斗 (10节点·F+C+G+C+G+J+C).md")
        cockpit = self.read("ETL/逻辑SQL/ads_高层经营驾驶舱.md")
        for text in (touch_bridge, participation_bridge, activity, funnel, cockpit):
            self.assertIn("<", text)
            self.assertIn("INTERVAL 8 DAYS", text)

        coupon_bridge = self.read("ETL/公共口径/02_券实例核销订单桥.sql")
        coupon_analysis = self.read("ETL/逻辑SQL/etl_dws_券效益分析.md")
        for text in (coupon_bridge, coupon_analysis, activity):
            self.assertIn("BETWEEN c.`发放日期`", text)
            self.assertIn("COALESCE(c.`失效日期`, DATE '9999-12-31')", text)

    def test_member_attribution_bridges_exclude_blank_identity_keys(self):
        for relative in (
            "ETL/公共口径/01_触达订单归因桥.sql",
            "ETL/公共口径/03_活动参与订单归因桥.sql",
        ):
            sql = self.read(relative)
            self.assertGreaterEqual(sql.count("`会员ID` <> ''"), 2, relative)

    def test_group_by_nodes_export_real_aggregation(self):
        for path in (ROOT / "ETL/逻辑SQL").glob("*.md"):
            for section in re.split(r"(?=^### 节点\d+)", path.read_text(encoding="utf-8"), flags=re.MULTILINE):
                if "- Type: GROUP_BY" in section:
                    self.assertIn("GROUP BY", section, f"GROUP_BY 仍是占位导出: {path.name}")

    def test_time_sensitive_etls_do_not_mix_clocks(self):
        for path in (ROOT / "ETL/逻辑SQL").glob("*.md"):
            text = path.read_text(encoding="utf-8")
            self.assertNotIn("2026-05-20", text, path.name)
            self.assertNotIn("CURRENT_DATE", text, path.name)
            self.assertNotIn("CURRENT_TIMESTAMP", text, path.name)
            self.assertIn("as_of_date", text, path.name)

    def test_snapshot_literal_only_defines_the_as_of_parameter(self):
        paths = list((ROOT / "ETL/逻辑SQL").glob("*.md"))
        paths.extend((ROOT / "ETL/公共口径").glob("*.sql"))
        for path in paths:
            for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
                if "DATE '2026-06-24'" in line:
                    self.assertRegex(
                        line,
                        r"(?i)AS\s+`?as_of_date`?",
                        f"快照字面量未通过 as_of_date 参数：{path.name}:{line_number}",
                    )

    def test_ninety_day_new_store_window_is_day_zero_through_eighty_nine(self):
        new_store = self.read("ETL/逻辑SQL/etl_dws_新店爬坡_Comp老店 (8节点·F+C+G+J).md")
        self.assertIn("`开业天数` BETWEEN 0 AND 89", new_store)
        self.assertNotIn("`开业天数` <= 90", new_store)
        self.assertNotIn("`开业天数` BETWEEN 0 AND 90", new_store)

    def test_store_dimension_joins_declare_temporal_semantics(self):
        for path in (ROOT / "ETL/逻辑SQL").glob("*.md"):
            text = path.read_text(encoding="utf-8")
            if "dim_门店主档" in text:
                self.assertRegex(text, r"生效起始日期|当前版本标记", path.name)

    def test_blocked_business_shortcuts_do_not_return(self):
        logic = "\n".join(
            path.read_text(encoding="utf-8")
            for path in (ROOT / "ETL/逻辑SQL").glob("*.md")
        )
        self.assertNotRegex(logic, r"触达人数`?\s*\*\s*0\.08")
        self.assertNotRegex(logic, r"0\.08\s*\*\s*35")
        payback = self.read("ETL/逻辑SQL/etl_dws_加盟回本测算 (9节点·S×3+J+C).md")
        self.assertNotRegex(payback, r"(?:THEN|ELSE)\s+999(?:\D|$)")

    def test_readme_no_longer_promises_copy_paste_production_sql(self):
        readme = self.read("README.md")
        self.assertNotIn("可以照着抄", readme)
        self.assertNotIn("给你抄算法", readme)
        self.assertNotIn("表名换成你家的就能用", readme)
        self.assertIn("待验证示例", readme)

    def test_current_dashboard_docs_do_not_reference_retired_metrics(self):
        retired = (
            "私域贡献销售",
            "私域贡献收入占比",
            "到店订单占比",
            "总折扣` is",
            "转化人数` is",
            "拉动销售",
            "总体转化率",
            "券ROI",
        )
        dashboard_docs = "\n".join(
            path.read_text(encoding="utf-8")
            for path in (ROOT / "看板/页面文档").glob("*.md")
        )
        for field in retired:
            self.assertNotIn(field, dashboard_docs)

        boundary = self.read("看板/页面JSON/README.md")
        self.assertIn("v1.4.0", boundary)
        self.assertIn("请勿把它们作为当前 schema", boundary)

    def test_parameter_tables_are_split_by_business_meaning(self):
        lifecycle = self.read("数据集/数据样本/param_会员生命周期阈值.csv").splitlines()[0]
        store_alert = self.read("数据集/数据样本/param_门店会员占比预警阈值.csv").splitlines()[0]
        self.assertIn("活跃天数上限", lifecycle)
        self.assertIn("流失天数上限", lifecycle)
        self.assertIn("会员占比月降幅预警", store_alert)

    def test_business_acceptance_sql_has_nine_new_checks(self):
        sql = self.read("ETL/公共口径/04_v1.4.1_业务验收.sql")
        self.assertEqual(9, len(re.findall(r"(?:SELECT|UNION ALL SELECT) '\d{2}'", sql)))
        self.assertIn("`留存月份序号` = 'M0'", sql)
        self.assertIn("previous_streak + 1", sql)
        self.assertIn("expected_streak", sql)
        self.assertIn("FROM `ads_单店利润健康`", sql)
        self.assertIn("`预计完整回本日期`", sql)
        self.assertNotIn("`预计回本日期`", sql)

    def test_repository_has_no_ds_store_files(self):
        self.assertEqual([], list(ROOT.rglob(".DS_Store")))

    def test_all_published_sql_parses_as_spark(self):
        import sqlglot

        blocks = []
        for path in (ROOT / "ETL/逻辑SQL").glob("*.md"):
            text = path.read_text(encoding="utf-8")
            blocks.extend((path, sql) for sql in re.findall(r"```sql\s*(.*?)```", text, re.DOTALL))
        for path in (ROOT / "ETL/公共口径").glob("*.sql"):
            blocks.append((path, path.read_text(encoding="utf-8")))

        self.assertGreaterEqual(len(blocks), 175)
        for path, sql in blocks:
            try:
                sqlglot.parse(sql, read="spark")
            except sqlglot.errors.ParseError as exc:
                self.fail(f"Spark SQL 静态解析失败: {path.name}: {exc}")

    def test_each_sql_block_defines_its_own_runtime_params(self):
        for path in (ROOT / "ETL/逻辑SQL").glob("*.md"):
            text = path.read_text(encoding="utf-8")
            for sql in re.findall(r"```sql\s*(.*?)```", text, re.DOTALL):
                if re.search(r"CROSS\s+JOIN\s+params\b", sql, re.IGNORECASE):
                    self.assertRegex(sql, r"(?i)WITH\s+params\s+AS\s*\(", path.name)


if __name__ == "__main__":
    unittest.main()
