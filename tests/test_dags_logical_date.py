"""Verify DAG entrypoints honor logical_date for backfill."""
from datetime import datetime
from unittest.mock import patch, MagicMock


def test_collect_investor_trading_time_uses_logical_date(tmp_path, monkeypatch):
    """collect_investor_trading_time must derive date_str from logical_date, not now()."""
    monkeypatch.setenv("DATA_DB_PATH", str(tmp_path))
    monkeypatch.setenv("PROD_KIS_APP_KEY", "fake")
    monkeypatch.setenv("PROD_KIS_APP_SECRET", "fake")

    import kis_investor_trading as mod
    import polars as pl

    target = datetime(2026, 4, 22)

    captured = {}
    def fake_save(df, item_name, date_str):
        captured["date_str"] = date_str
        return f"/fake/{item_name}/{date_str}.parquet"

    with patch.object(mod, "sync_fetch_all_markets",
                      return_value={"kospi": pl.DataFrame({"x": [1]})}), \
         patch.object(mod, "save_to_final", side_effect=fake_save):
        task_fn = getattr(
            mod.collect_investor_trading_time,
            "function",
            mod.collect_investor_trading_time,
        )
        task_fn(logical_date=target)

    assert captured["date_str"] == "2026-04-22", \
        f"Expected '2026-04-22', got {captured['date_str']!r}"


def test_collect_investor_trading_time_falls_back_to_now_when_logical_date_is_none(
    tmp_path, monkeypatch
):
    """When logical_date is None, date_str must come from pendulum.now('Asia/Seoul')."""
    monkeypatch.setenv("DATA_DB_PATH", str(tmp_path))
    monkeypatch.setenv("PROD_KIS_APP_KEY", "fake")
    monkeypatch.setenv("PROD_KIS_APP_SECRET", "fake")

    import kis_investor_trading as mod
    import pendulum
    import polars as pl

    # Freeze pendulum.now to a known value so we can assert the derived date_str.
    fake_now = pendulum.datetime(2026, 4, 23, 10, 0, tz="Asia/Seoul")
    monkeypatch.setattr(mod.pendulum, "now", lambda tz=None: fake_now)

    captured = {}
    def fake_save(df, item_name, date_str):
        captured["date_str"] = date_str
        return f"/fake/{item_name}/{date_str}.parquet"

    with patch.object(mod, "sync_fetch_all_markets",
                      return_value={"kospi": pl.DataFrame({"x": [1]})}), \
         patch.object(mod, "save_to_final", side_effect=fake_save):
        task_fn = getattr(
            mod.collect_investor_trading_time,
            "function",
            mod.collect_investor_trading_time,
        )
        task_fn(logical_date=None)

    assert captured["date_str"] == "2026-04-23", \
        f"Expected '2026-04-23' from frozen pendulum.now, got {captured['date_str']!r}"
