"""Python oracle for rule-based NBA task generation.

Mirrors `ETL/公共口径/08_规则任务生成.sql`: nine rules, 7-day anti-disturb
except negative-review repair, then one task per member.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date, timedelta
from typing import Any

from tests.fact_bridges import AS_OF


def _blank(value: Any) -> bool:
    return value is None or value == ""


def _ticket_size(amount: Any, count: Any) -> float | None:
    if not count:
        return None
    return round(float(amount) / float(count), 2)


def _rule_candidates(
    lifecycle: Iterable[dict[str, Any]],
    channels: Iterable[dict[str, Any]],
    rfm: Iterable[dict[str, Any]],
    reviews: Iterable[dict[str, Any]],
    coupons: Iterable[dict[str, Any]],
    as_of: date,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    rfm_by_id = {row["member_id"]: row for row in rfm if not _blank(row.get("member_id"))}

    for member in lifecycle:
        if _blank(member.get("member_id")) or member.get("snapshot") not in (None, as_of):
            continue
        stage = member["stage"]
        store_id = member.get("store_id")
        value = _ticket_size(member.get("amount"), member.get("orders"))
        if stage == "流失预警":
            rows.append(
                {
                    "member_id": member["member_id"],
                    "store_id": store_id,
                    "priority": "P0",
                    "task_type": "流失预警",
                    "value": value,
                }
            )
        elif stage == "沉睡":
            rows.append(
                {
                    "member_id": member["member_id"],
                    "store_id": store_id,
                    "priority": "P1",
                    "task_type": "沉睡召回",
                    "value": value,
                }
            )
        elif stage in {"新客-未首单", "注册未消费"}:
            rows.append(
                {
                    "member_id": member["member_id"],
                    "store_id": store_id,
                    "priority": "P1",
                    "task_type": "新会员首单",
                    "value": None,
                }
            )
        elif stage == "新客-已首单" and member.get("orders") == 1:
            rows.append(
                {
                    "member_id": member["member_id"],
                    "store_id": store_id,
                    "priority": "P1",
                    "task_type": "首单后二单",
                    "value": value,
                }
            )

    review_best: dict[str, dict[str, Any]] = {}
    for review in reviews:
        if _blank(review.get("member_id")):
            continue
        if review["biz_date"] < as_of - timedelta(days=3) or review["biz_date"] > as_of:
            continue
        if review.get("reply") == "已回复":
            continue
        if not (review.get("score", 99) <= 2 or not _blank(review.get("tag"))):
            continue
        current = review_best.get(review["member_id"])
        key = (review["score"], -review["biz_date"].toordinal(), review.get("review_id", ""))
        if current is None or key < current["_key"]:
            review_best[review["member_id"]] = {**review, "_key": key}
    for review in review_best.values():
        rows.append(
            {
                "member_id": review["member_id"],
                "store_id": review.get("store_id"),
                "priority": "P0",
                "task_type": "负评修复",
                "value": None,
            }
        )

    for channel in channels:
        if _blank(channel.get("member_id")) or channel.get("snapshot") not in (None, as_of):
            continue
        if channel["migrate"] in {"纯外卖", "堂食→外卖迁移"}:
            rows.append(
                {
                    "member_id": channel["member_id"],
                    "store_id": None,
                    "priority": "P2",
                    "task_type": "外卖转到店",
                    "value": None,
                }
            )
        if channel["migrate"] == "纯堂食":
            profile = rfm_by_id.get(channel["member_id"])
            if profile is not None and profile.get("r_score", 99) <= 2:
                rows.append(
                    {
                        "member_id": channel["member_id"],
                        "store_id": None,
                        "priority": "P2",
                        "task_type": "堂食老客召回",
                        "value": _ticket_size(profile.get("amount"), profile.get("orders")),
                    }
                )

    for profile in rfm:
        if _blank(profile.get("member_id")) or profile.get("snapshot") not in (None, as_of):
            continue
        if profile.get("label") == "重要价值客户":
            rows.append(
                {
                    "member_id": profile["member_id"],
                    "store_id": None,
                    "priority": "P2",
                    "task_type": "高价值维护",
                    "value": _ticket_size(profile.get("amount"), profile.get("orders")),
                }
            )

    coupon_best: dict[str, dict[str, Any]] = {}
    for coupon in coupons:
        if _blank(coupon.get("member_id")) or coupon.get("redeemed") is not None:
            continue
        if coupon["issued"] > as_of:
            continue
        if not (as_of <= coupon["expires"] <= as_of + timedelta(days=7)):
            continue
        current = coupon_best.get(coupon["member_id"])
        key = (coupon["expires"], coupon.get("coupon_id", ""))
        if current is None or key < current["_key"]:
            coupon_best[coupon["member_id"]] = {**coupon, "_key": key}
    for coupon in coupon_best.values():
        rows.append(
            {
                "member_id": coupon["member_id"],
                "store_id": coupon.get("store_id"),
                "priority": "P2",
                "task_type": "领券未核销",
                "value": None,
            }
        )
    return rows


def _member_digits(member_id: str) -> int:
    digits = "".join(ch for ch in member_id if ch.isdigit())
    return int(digits) if digits else 0


def _holdout(task_type: str, member_id: str) -> int:
    if task_type == "负评修复":
        return 0
    return 1 if _member_digits(member_id) % 10 == 0 else 0


def generate_rule_tasks(
    lifecycle: Iterable[dict[str, Any]],
    channels: Iterable[dict[str, Any]],
    rfm: Iterable[dict[str, Any]],
    reviews: Iterable[dict[str, Any]],
    coupons: Iterable[dict[str, Any]],
    touches: Iterable[dict[str, Any]],
    as_of: date = AS_OF,
) -> list[dict[str, Any]]:
    recently_touched = {
        touch["member_id"]
        for touch in touches
        if not _blank(touch.get("member_id"))
        and as_of - timedelta(days=7) <= touch["biz_date"] <= as_of
    }
    reachable = [
        row
        for row in _rule_candidates(lifecycle, channels, rfm, reviews, coupons, as_of)
        if row["task_type"] == "负评修复" or row["member_id"] not in recently_touched
    ]
    winners: dict[str, dict[str, Any]] = {}
    for row in reachable:
        current = winners.get(row["member_id"])
        key = (
            row["priority"],
            1 if row["value"] is None else 0,
            -(row["value"] or 0),
            row["task_type"],
        )
        if current is None or key < current["_key"]:
            winners[row["member_id"]] = {
                **row,
                "_key": key,
                "holdout": _holdout(row["task_type"], row["member_id"]),
            }
    return [{k: v for k, v in row.items() if k != "_key"} for row in winners.values()]
