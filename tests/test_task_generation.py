"""Oracle checks for rule-based task generation and anti-disturb."""

from __future__ import annotations

import unittest
from datetime import date

from tests.task_generation import generate_rule_tasks


class TaskGenerationOracleTests(unittest.TestCase):
    def test_arbitration_keeps_p0_over_p1_and_one_task_per_member(self):
        rows = generate_rule_tasks(
            lifecycle=[
                {
                    "member_id": "M1",
                    "store_id": "S1",
                    "stage": "沉睡",
                    "orders": 4,
                    "amount": 200,
                }
            ],
            channels=[],
            rfm=[{"member_id": "M1", "label": "重要价值客户", "r_score": 5, "orders": 4, "amount": 200}],
            reviews=[
                {
                    "member_id": "M1",
                    "store_id": "S1",
                    "score": 2,
                    "tag": "出餐慢",
                    "reply": "未回复",
                    "biz_date": date(2026, 6, 23),
                    "review_id": "R1",
                }
            ],
            coupons=[],
            touches=[],
        )
        self.assertEqual(1, len(rows))
        self.assertEqual("负评修复", rows[0]["task_type"])
        self.assertEqual("P0", rows[0]["priority"])

    def test_negative_review_skips_anti_disturb(self):
        rows = generate_rule_tasks(
            lifecycle=[],
            channels=[],
            rfm=[],
            reviews=[
                {
                    "member_id": "M1",
                    "store_id": "S1",
                    "score": 1,
                    "tag": "",
                    "reply": "未回复",
                    "biz_date": date(2026, 6, 22),
                    "review_id": "R1",
                }
            ],
            coupons=[],
            touches=[{"member_id": "M1", "biz_date": date(2026, 6, 20)}],
        )
        self.assertEqual(["负评修复"], [row["task_type"] for row in rows])

    def test_recent_touch_blocks_marketing_task(self):
        rows = generate_rule_tasks(
            lifecycle=[
                {
                    "member_id": "M1",
                    "store_id": "S1",
                    "stage": "沉睡",
                    "orders": 3,
                    "amount": 90,
                }
            ],
            channels=[],
            rfm=[],
            reviews=[],
            coupons=[],
            touches=[{"member_id": "M1", "biz_date": date(2026, 6, 20)}],
        )
        self.assertEqual([], rows)

    def test_second_order_rule_requires_exactly_one_completed_order(self):
        rows = generate_rule_tasks(
            lifecycle=[
                {
                    "member_id": "M1",
                    "store_id": "S1",
                    "stage": "新客-已首单",
                    "orders": 7,
                    "amount": 400,
                },
                {
                    "member_id": "M2",
                    "store_id": "S1",
                    "stage": "新客-已首单",
                    "orders": 1,
                    "amount": 40,
                },
            ],
            channels=[],
            rfm=[],
            reviews=[],
            coupons=[],
            touches=[],
        )
        self.assertEqual(["M2"], [row["member_id"] for row in rows])
        self.assertEqual(["首单后二单"], [row["task_type"] for row in rows])

    def test_holdout_uses_member_id_tail_except_negative_review(self):
        marketing = generate_rule_tasks(
            lifecycle=[
                {
                    "member_id": "M10",
                    "store_id": "S1",
                    "stage": "流失预警",
                    "orders": 5,
                    "amount": 250,
                },
                {
                    "member_id": "M11",
                    "store_id": "S1",
                    "stage": "流失预警",
                    "orders": 5,
                    "amount": 250,
                },
            ],
            channels=[],
            rfm=[],
            reviews=[],
            coupons=[],
            touches=[],
        )
        by_member = {row["member_id"]: row["holdout"] for row in marketing}
        self.assertEqual({"M10": 1, "M11": 0}, by_member)

        review = generate_rule_tasks(
            lifecycle=[],
            channels=[],
            rfm=[],
            reviews=[
                {
                    "member_id": "M10",
                    "store_id": "S1",
                    "score": 1,
                    "tag": "",
                    "reply": "未回复",
                    "biz_date": date(2026, 6, 22),
                    "review_id": "R1",
                }
            ],
            coupons=[],
            touches=[],
        )
        self.assertEqual([0], [row["holdout"] for row in review])
