"""Run the three fact-bridge oracles against workshop sample CSVs."""

from __future__ import annotations

import csv
import unittest
from datetime import date, datetime
from pathlib import Path

from tests.fact_bridges import (
    assert_unique_orders,
    attributed_gmv,
    coupon_order_bridge,
    eligible_order_gmv,
    participation_order_bridge,
    touch_order_bridge,
)


ROOT = Path(__file__).resolve().parents[1]
SAMPLE = ROOT / "数据集" / "数据样本"


def _parse_date(value: str) -> date | None:
    if not value:
        return None
    return date.fromisoformat(value[:10])


def _parse_dt(value: str) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value)


def _read(name: str) -> list[dict[str, str]]:
    with (SAMPLE / name).open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _orders() -> list[dict]:
    rows = []
    for row in _read("dwd_订单.csv"):
        when = _parse_dt(row["下单时间"])
        biz = _parse_date(row["业务日期"])
        if when is None or biz is None:
            continue
        rows.append(
            {
                "order_id": row["订单ID"],
                "member_id": row["会员ID"] or None,
                "time": when,
                "biz_date": biz,
                "status": row["订单状态"],
                "gmv": float(row["实付金额"] or 0),
            }
        )
    return rows


class SampleBridgeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.orders = _orders()
        cls.touches = [
            {
                "touch_id": row["触达ID"],
                "member_id": row["会员ID"] or None,
                "time": _parse_dt(row["触达时间"]),
                "biz_date": _parse_date(row["触达日期"]),
                "status": row["触达状态"],
            }
            for row in _read("dwd_会员触达.csv")
            if _parse_dt(row["触达时间"]) and _parse_date(row["触达日期"])
        ]
        cls.coupons = [
            {
                "coupon_id": row["券ID"],
                "member_id": row["会员ID"] or None,
                "order_id": row["订单ID"] or None,
                "issued": _parse_date(row["发放日期"]),
                "expires": _parse_date(row["失效日期"]),
                "redeemed": _parse_date(row["核销日期"]),
                "discount": float(row["折扣金额"] or 0),
            }
            for row in _read("dwd_券事件.csv")
            if _parse_date(row["发放日期"])
        ]
        cls.parts = [
            {
                "participation_id": row["参与ID"],
                "member_id": row["会员ID"] or None,
                "time": _parse_dt(row["参与时间"]),
                "biz_date": _parse_date(row["参与日期"]),
                "result": row["结果"],
            }
            for row in _read("dwd_活动参与.csv")
            if _parse_dt(row["参与时间"]) and _parse_date(row["参与日期"])
        ]

    def test_sample_touch_bridge_is_unique_and_capped(self):
        rows = touch_order_bridge(self.orders, self.touches)
        self.assertEqual([], assert_unique_orders(rows))
        self.assertLessEqual(attributed_gmv(rows), eligible_order_gmv(self.orders))

    def test_sample_coupon_bridge_is_unique_and_capped(self):
        rows = coupon_order_bridge(self.orders, self.coupons)
        self.assertEqual([], assert_unique_orders(rows))
        self.assertLessEqual(attributed_gmv(rows), eligible_order_gmv(self.orders))

    def test_sample_participation_bridge_is_unique_and_capped(self):
        rows = participation_order_bridge(self.orders, self.parts)
        self.assertEqual([], assert_unique_orders(rows))
        self.assertLessEqual(attributed_gmv(rows), eligible_order_gmv(self.orders))

    def test_three_caliber_may_share_an_order_but_not_inside_one_bridge(self):
        touch_ids = {row["order_id"] for row in touch_order_bridge(self.orders, self.touches)}
        coupon_ids = {row["order_id"] for row in coupon_order_bridge(self.orders, self.coupons)}
        part_ids = {row["order_id"] for row in participation_order_bridge(self.orders, self.parts)}
        self.assertLessEqual(len(touch_ids), len(self.orders))
        self.assertLessEqual(len(coupon_ids), len(self.orders))
        self.assertLessEqual(len(part_ids), len(self.orders))
