"""Python oracle for the three v1.4.1 fact bridges.

Mirrors `ETL/公共口径/01_*.sql` … `03_*.sql` with stdlib only.
Window: event_time <= order_time < event_time + 8 days.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date, datetime, timedelta
from typing import Any


AS_OF = date(2026, 6, 24)
WINDOW = timedelta(days=8)


def _blank(value: Any) -> bool:
    return value is None or value == ""


def _null_safe_eq(left: Any, right: Any) -> bool:
    if left is None and right is None:
        return True
    return left == right


def touch_order_bridge(
    orders: Iterable[dict[str, Any]],
    touches: Iterable[dict[str, Any]],
    as_of: date = AS_OF,
) -> list[dict[str, Any]]:
    valid_orders = [
        order
        for order in orders
        if order["status"] == "已完成"
        and not _blank(order["member_id"])
        and order["biz_date"] <= as_of
    ]
    valid_touches = [
        touch
        for touch in touches
        if touch["status"] == "已发送"
        and not _blank(touch["member_id"])
        and touch["biz_date"] <= as_of
    ]
    winners: dict[str, dict[str, Any]] = {}
    for order in valid_orders:
        candidates = [
            touch
            for touch in valid_touches
            if touch["member_id"] == order["member_id"]
            and touch["time"] <= order["time"] < touch["time"] + WINDOW
        ]
        if not candidates:
            continue
        touch = max(candidates, key=lambda item: (item["time"], item["touch_id"]))
        winners[order["order_id"]] = {
            "order_id": order["order_id"],
            "member_id": order["member_id"],
            "biz_date": order["biz_date"],
            "gmv": order["gmv"],
            "touch_id": touch["touch_id"],
            "touch_time": touch["time"],
        }
    return list(winners.values())


def coupon_order_bridge(
    orders: Iterable[dict[str, Any]],
    coupons: Iterable[dict[str, Any]],
    as_of: date = AS_OF,
) -> list[dict[str, Any]]:
    instance: dict[str, dict[str, Any]] = {}
    for coupon in coupons:
        if _blank(coupon["coupon_id"]) or coupon["issued"] is None or coupon["issued"] > as_of:
            continue
        current = instance.get(coupon["coupon_id"])
        key = (coupon["redeemed"] or coupon["issued"], coupon.get("order_id") or "")
        if current is None or key > (current["redeemed"] or current["issued"], current.get("order_id") or ""):
            instance[coupon["coupon_id"]] = coupon

    valid_orders = {
        order["order_id"]: order
        for order in orders
        if order["status"] == "已完成" and order["biz_date"] <= as_of
    }
    winners: dict[str, dict[str, Any]] = {}
    for coupon in instance.values():
        redeemed = coupon["redeemed"]
        expires = coupon["expires"] or date(9999, 12, 31)
        if redeemed is None or not (coupon["issued"] <= redeemed <= expires) or redeemed > as_of:
            continue
        order = valid_orders.get(coupon.get("order_id"))
        if order is None or not _null_safe_eq(coupon.get("member_id"), order.get("member_id")):
            continue
        candidate = {
            "order_id": order["order_id"],
            "coupon_id": coupon["coupon_id"],
            "biz_date": order["biz_date"],
            "gmv": order["gmv"],
            "discount": coupon.get("discount") or 0.0,
        }
        current = winners.get(order["order_id"])
        if current is None or _coupon_wins(candidate, current):
            winners[order["order_id"]] = candidate
    return list(winners.values())


def _coupon_wins(candidate: dict[str, Any], current: dict[str, Any]) -> bool:
    if candidate["discount"] != current["discount"]:
        return candidate["discount"] > current["discount"]
    return candidate["coupon_id"] < current["coupon_id"]


def participation_order_bridge(
    orders: Iterable[dict[str, Any]],
    participations: Iterable[dict[str, Any]],
    as_of: date = AS_OF,
) -> list[dict[str, Any]]:
    valid_orders = [
        order
        for order in orders
        if order["status"] == "已完成"
        and not _blank(order["member_id"])
        and order["biz_date"] <= as_of
    ]
    valid_parts = [
        item
        for item in participations
        if item["result"] == "成功"
        and not _blank(item["member_id"])
        and item["biz_date"] <= as_of
    ]
    winners: dict[str, dict[str, Any]] = {}
    for order in valid_orders:
        candidates = [
            item
            for item in valid_parts
            if item["member_id"] == order["member_id"]
            and item["time"] <= order["time"] < item["time"] + WINDOW
        ]
        if not candidates:
            continue
        item = max(candidates, key=lambda row: (row["time"], row["participation_id"]))
        winners[order["order_id"]] = {
            "order_id": order["order_id"],
            "member_id": order["member_id"],
            "biz_date": order["biz_date"],
            "gmv": order["gmv"],
            "participation_id": item["participation_id"],
        }
    return list(winners.values())


def assert_unique_orders(rows: Iterable[dict[str, Any]]) -> list[str]:
    seen: set[str] = set()
    duplicates: list[str] = []
    for row in rows:
        order_id = row["order_id"]
        if order_id in seen:
            duplicates.append(order_id)
        seen.add(order_id)
    return duplicates


def attributed_gmv(rows: Iterable[dict[str, Any]]) -> float:
    return sum(float(row["gmv"]) for row in rows)


def eligible_order_gmv(orders: Iterable[dict[str, Any]], as_of: date = AS_OF) -> float:
    return sum(
        float(order["gmv"])
        for order in orders
        if order["status"] == "已完成" and order["biz_date"] <= as_of
    )
