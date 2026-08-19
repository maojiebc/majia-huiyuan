"""Oracle and sample checks for store calendar, month skeleton, and SCD2."""

from __future__ import annotations

import csv
import unittest
from datetime import date
from pathlib import Path

from tests.fact_bridges import AS_OF
from tests.time_norms import scd2_join, store_calendar, store_month_skeleton


ROOT = Path(__file__).resolve().parents[1]
SAMPLE = ROOT / "数据集" / "数据样本"


def _read(name: str) -> list[dict[str, str]]:
    with (SAMPLE / name).open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


class TimeNormOracleTests(unittest.TestCase):
    def test_calendar_stops_at_close_and_keeps_zero_order_days(self):
        rows = store_calendar(
            [
                {"store_id": "S1", "opened": date(2026, 6, 20), "closed": date(2026, 6, 22)},
                {"store_id": "S2", "opened": date(2026, 6, 23), "closed": None},
                {"store_id": "S3", "opened": date(2026, 6, 25), "closed": None},
            ]
        )
        s1 = [row["biz_date"] for row in rows if row["store_id"] == "S1"]
        s2 = [row["biz_date"] for row in rows if row["store_id"] == "S2"]
        self.assertEqual([date(2026, 6, 20), date(2026, 6, 21), date(2026, 6, 22)], s1)
        self.assertEqual([date(2026, 6, 23), date(2026, 6, 24)], s2)
        self.assertNotIn("S3", {row["store_id"] for row in rows})
        self.assertTrue(all(row["biz_date"] <= AS_OF for row in rows))

    def test_month_skeleton_keeps_zero_sales_months(self):
        rows = store_month_skeleton(
            [{"store_id": "S1", "opened": date(2026, 1, 18), "closed": None}],
            as_of=date(2026, 3, 15),
        )
        months = [row["month"] for row in rows]
        self.assertEqual([date(2026, 1, 1), date(2026, 2, 1), date(2026, 3, 1)], months)
        self.assertEqual(date(2026, 2, 28), rows[1]["as_of_date"])
        self.assertEqual(date(2026, 3, 15), rows[2]["as_of_date"])
        self.assertIn(date(2026, 2, 1), months)

    def test_overlapping_scd2_keeps_later_start_then_higher_version(self):
        versions = [
            {"store_id": "S1", "start": date(2026, 1, 1), "end": date(2026, 12, 31), "version_id": "V1"},
            {"store_id": "S1", "start": date(2026, 6, 1), "end": date(2026, 12, 31), "version_id": "V2"},
            {"store_id": "S1", "start": date(2026, 6, 1), "end": date(2026, 12, 31), "version_id": "V9"},
        ]
        rows = scd2_join([{"store_id": "S1", "biz_date": date(2026, 6, 10), "fact_id": "F1"}], versions)
        self.assertEqual(["V9"], [row["version_id"] for row in rows])

    def test_scd2_preserves_in_window_fact_count(self):
        versions = [
            {"store_id": "S1", "start": date(2026, 1, 1), "end": date(2026, 5, 31), "version_id": "V1"},
        ]
        facts = [
            {"store_id": "S1", "biz_date": date(2026, 3, 1), "fact_id": "F1"},
            {"store_id": "S1", "biz_date": date(2026, 6, 10), "fact_id": "F2"},
            {"store_id": "S2", "biz_date": date(2026, 6, 10), "fact_id": "F3"},
            {"store_id": "S1", "biz_date": date(2026, 6, 25), "fact_id": "F4"},
        ]
        rows = scd2_join(facts, versions)
        self.assertEqual(3, len(rows))
        self.assertEqual(["V1", None, None], [row["version_id"] for row in rows])
        self.assertEqual({"F1", "F2", "F3"}, {row["fact_id"] for row in rows})


class TimeNormSampleTests(unittest.TestCase):
    def test_sample_store_daily_is_unique(self):
        keys = [(row["门店ID"], row["业务日期"]) for row in _read("dws_门店日报.csv")]
        self.assertEqual(len(keys), len(set(keys)))

    def test_sample_profit_month_is_unique(self):
        keys = [(row["门店ID"], row["月份"]) for row in _read("dws_单店利润月汇总.csv")]
        self.assertEqual(len(keys), len(set(keys)))

    def test_sample_new_store_day_is_unique(self):
        keys = [(row["门店ID"], row["业务日期"]) for row in _read("dws_新店爬坡_Comp老店.csv")]
        self.assertEqual(len(keys), len(set(keys)))

    def test_sample_review_day_is_unique(self):
        keys = [(row["门店ID"], row["业务日期"]) for row in _read("dws_体验口碑汇总.csv")]
        self.assertEqual(len(keys), len(set(keys)))

    def test_sample_incomplete_non_m0_retention_is_null(self):
        rows = _read("dws_会员同期群留存.csv")
        incomplete = [
            row
            for row in rows
            if row["是否完整观察期"] == "0" and row["留存月份序号"] != "M0"
        ]
        self.assertGreaterEqual(len(incomplete), 1)
        self.assertTrue(all(not row["留存人数"] and not row["留存率"] for row in incomplete))
        june_m0 = [
            row
            for row in rows
            if row["同期群月份"] == "2026-06-01" and row["留存月份序号"] == "M0"
        ]
        self.assertEqual(1, len(june_m0))
        self.assertEqual(june_m0[0]["留存人数"], june_m0[0]["同期群人数"])
        self.assertEqual("0", june_m0[0]["是否完整观察期"])
