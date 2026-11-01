from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import polars as pl


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import backfill_kis_minutes as backfill  # noqa: E402


def test_backfill_workers_share_work_and_aggregate_stats(monkeypatch):
    active = 0
    maximum_active = 0
    calls: list[list[str]] = []

    async def fake_collect(
        _api, instruments, _fetcher, _date, _converter, _asset_type
    ):
        nonlocal active, maximum_active
        active += 1
        maximum_active = max(maximum_active, active)
        calls.append([item["ticker"] for item in instruments])
        await asyncio.sleep(0.01)
        active -= 1
        return {
            "data": pl.DataFrame({"ticker": [item["ticker"] for item in instruments]}),
            "success": len(instruments),
            "failed": 0,
            "no_data": 0,
            "expected_total": len(instruments),
            "unexpected_no_data": 0,
            "unexpected_no_data_codes": [],
            "failed_codes": [],
            "aborted": 0,
            "total": len(instruments),
        }

    monkeypatch.setattr(backfill, "KIS_BACKFILL_CONCURRENCY", 3)
    monkeypatch.setattr(backfill, "_collect_asset_group", fake_collect)
    instruments = [{"ticker": str(index)} for index in range(7)]

    result = asyncio.run(
        backfill.collect_asset_group(None, instruments, None, "20260402", None, "stock")
    )

    assert maximum_active == 3
    assert len(calls) == 3
    assert sorted(code for chunk in calls for code in chunk) == [str(index) for index in range(7)]
    assert result["success"] == 7
    assert result["expected_total"] == 7
    assert result["total"] == 7
    assert result["failed"] == 0
    assert len(result["data"]) == 7
