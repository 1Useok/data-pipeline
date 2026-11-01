from __future__ import annotations

import polars as pl
import pytest
from pandas import Timestamp

import krx_daily_batch as batch


def _task_function(task):
    return getattr(task, "function", task)


def test_empty_required_dataset_is_failure_not_skip(monkeypatch):
    monkeypatch.setattr(
        batch,
        "CRAWL_TARGETS",
        [
            {
                "name": "required",
                "func": lambda date: None,
                "start_date": "2000-01-01",
            }
        ],
    )
    monkeypatch.setattr(batch, "resolve_target_date", lambda **kwargs: Timestamp("2026-07-31"))
    monkeypatch.setattr(batch, "setup_proxy", lambda: None)
    monkeypatch.setattr(batch, "safe_crawl", lambda *args, **kwargs: pl.DataFrame())

    with pytest.raises(RuntimeError, match="Required KRX dataset is empty"):
        _task_function(batch.crawl_item)(item_index=0, run_id="test")


def test_final_validator_runs_after_upstream_skip():
    assert batch.validate_and_trigger_adjustment.kwargs["trigger_rule"] == "all_done"
