"""Tests for missing output detector."""

import shutil
from datetime import date
from pathlib import Path
from uuid import uuid4

import polars as pl

from lib import missing_detector


def test_only_one_minute_prices_are_required():
    assert missing_detector.PRICE_TIMEFRAMES == ["1m"]


def _test_root() -> Path:
    root = Path(__file__).resolve().parents[1] / "_cache" / "test_tmp" / uuid4().hex
    root.mkdir(parents=True)
    return root


def test_scan_recent_reports_missing_outputs(monkeypatch):
    root = _test_root()
    monkeypatch.setenv("DATA_DB_PATH", str(root))
    monkeypatch.setattr(missing_detector, "is_trading_day", lambda ts: True)

    try:
        result = missing_detector.scan_recent(end=date(2026, 4, 22), days=1)
    finally:
        shutil.rmtree(root, ignore_errors=True)

    assert result["dates"] == ["2026-04-22"]
    assert result["missing"]
    assert "krx_daily_batch" in result["by_dag"]
    assert "convert_raw_to_bt" in result["by_dag"]


def test_scan_recent_accepts_existing_bt_price_file(monkeypatch):
    root = _test_root()
    monkeypatch.setenv("DATA_DB_PATH", str(root))
    monkeypatch.setattr(missing_detector, "is_trading_day", lambda ts: True)

    target = date(2026, 4, 22)
    price_dir = root / "MFT_backtesting" / "KR" / "price" / "1m" / "stock"
    try:
        price_dir.mkdir(parents=True)
        pl.DataFrame(
            {
                "datetime": [target],
                "isin": ["KR7005930003"],
                "open": [1.0],
                "high": [1.0],
                "low": [1.0],
                "close": [1.0],
                "volume": [1],
            }
        ).write_parquet(price_dir / "2026-04.parquet")

        result = missing_detector.scan_recent(end=target, days=1)
        missing_paths = [item["path"] for item in result["missing"]]
    finally:
        shutil.rmtree(root, ignore_errors=True)

    assert str(price_dir / "2026-04.parquet") not in missing_paths


def test_missing_kis_metric_routes_repair_to_kis_stage(monkeypatch):
    root = _test_root()
    monkeypatch.setenv("DATA_DB_PATH", str(root))
    target = date(2026, 4, 22)
    try:
        missing = missing_detector._check_kis_collection_metric(target)
    finally:
        shutil.rmtree(root, ignore_errors=True)

    assert len(missing) == 1
    assert missing[0].dag_id == "kis_minute_batch"


def test_accepted_kis_metric_is_complete(monkeypatch):
    root = _test_root()
    monkeypatch.setenv("DATA_DB_PATH", str(root))
    target = date(2026, 4, 22)
    metric_dir = root / "logs" / "kis_minute_silent_skip"
    try:
        metric_dir.mkdir(parents=True)
        pl.DataFrame(
            {
                "date": [target.isoformat()] * 3,
                "asset": ["stock", "future", "option"],
                "total": [10, 10, 10],
                "failed": [0, 0, 0],
                "expected_total": [8, 8, 8],
                "unexpected_no_data": [0, 0, 1],
                "accepted": [True, True, True],
            }
        ).write_parquet(metric_dir / "2026-04.parquet")

        missing = missing_detector._check_kis_collection_metric(target)
    finally:
        shutil.rmtree(root, ignore_errors=True)

    assert missing == []


def test_kis_metric_applies_unexpected_limit_per_asset(monkeypatch):
    root = _test_root()
    monkeypatch.setenv("DATA_DB_PATH", str(root))
    target = date(2026, 4, 22)
    metric_dir = root / "logs" / "kis_minute_silent_skip"
    try:
        metric_dir.mkdir(parents=True)
        pl.DataFrame(
            {
                "date": [target.isoformat()] * 3,
                "asset": ["stock", "future", "option"],
                "total": [10, 10, 10],
                "failed": [0, 0, 0],
                "expected_total": [8, 8, 8],
                "unexpected_no_data": [0, 0, 2],
                "accepted": [True, True, True],
            }
        ).write_parquet(metric_dir / "2026-04.parquet")

        missing = missing_detector._check_kis_collection_metric(target)
    finally:
        shutil.rmtree(root, ignore_errors=True)

    assert len(missing) == 1
    assert missing[0].dag_id == "kis_minute_batch"
