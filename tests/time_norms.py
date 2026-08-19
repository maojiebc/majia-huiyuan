"""Python oracle for store calendar, month skeleton, and SCD2 joins."""

from __future__ import annotations

import calendar
from collections.abc import Iterable
from datetime import date, timedelta
from typing import Any

from tests.fact_bridges import AS_OF


def _month_start(value: date) -> date:
    return value.replace(day=1)


def _add_months(value: date, months: int) -> date:
    index = value.year * 12 + value.month - 1 + months
    year, month0 = divmod(index, 12)
    return date(year, month0 + 1, 1)


def _last_day(value: date) -> date:
    return date(value.year, value.month, calendar.monthrange(value.year, value.month)[1])


def store_calendar(
    stores: Iterable[dict[str, Any]],
    as_of: date = AS_OF,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for store in stores:
        opened = store["opened"]
        closed = store.get("closed")
        if opened is None or opened > as_of:
            continue
        end = min(as_of, closed or as_of)
        if end < opened:
            continue
        current = opened
        while current <= end:
            rows.append({"store_id": store["store_id"], "biz_date": current})
            current += timedelta(days=1)
    return rows


def store_month_skeleton(
    stores: Iterable[dict[str, Any]],
    as_of: date = AS_OF,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for store in stores:
        opened = store["opened"]
        closed = store.get("closed")
        if opened is None or opened > as_of:
            continue
        end = min(as_of, closed or as_of)
        if end < opened:
            continue
        current = _month_start(opened)
        last = _month_start(end)
        while current <= last:
            rows.append(
                {
                    "store_id": store["store_id"],
                    "month": current,
                    "as_of_date": min(_last_day(current), as_of),
                }
            )
            current = _add_months(current, 1)
    return rows


def scd2_join(
    facts: Iterable[dict[str, Any]],
    versions: Iterable[dict[str, Any]],
    as_of: date = AS_OF,
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    version_list = list(versions)
    for fact in facts:
        if fact["biz_date"] > as_of:
            continue
        hits = [
            version
            for version in version_list
            if version["store_id"] == fact["store_id"]
            and version["start"] <= fact["biz_date"] <= (version["end"] or date(9999, 12, 31))
        ]
        winner = max(hits, key=lambda item: (item["start"], item["version_id"])) if hits else None
        row = dict(fact)
        row["version_id"] = None if winner is None else winner["version_id"]
        result.append(row)
    return result
