from pathlib import Path
from tests.compare_test_vs_server import expected_pairs


def test_expected_pairs_count_is_18():
    # 3 KIS + 11 investor + 3 converted 1m + 1 continuous = 18
    pairs = expected_pairs("2026-04-22")
    assert len(pairs) == 18


def test_expected_pairs_paths_use_target_month():
    pairs = expected_pairs("2026-04-22")
    labels = [p[0] for p in pairs]
    # KIS minute files use the month, not the date
    assert any("2026-04.parquet" in lbl and "MFT_raw/KR/price/1m/stock" in lbl for lbl in labels)
    # Investor trading uses the date
    assert any("2026-04-22.parquet" in lbl and "investor_trading_time_kospi" in lbl for lbl in labels)


def test_expected_pairs_no_duplicates():
    pairs = expected_pairs("2026-04-22")
    labels = [p[0] for p in pairs]
    assert len(set(labels)) == len(labels)


import polars as pl
from datetime import datetime
from tests.compare_test_vs_server import compare_one_pair


def _write(path, df):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)


def test_compare_one_pair_identical_data_passes(tmp_path):
    df = pl.DataFrame({
        "date": ["2026-04-22"] * 3,
        "ticker": ["005930", "000660", "035420"],
        "close": [70000.0, 130000.0, 200000.0],
    })
    a = tmp_path / "a.parquet"; _write(a, df)
    b = tmp_path / "b.parquet"; _write(b, df)
    overall, _ = compare_one_pair("test", a, b, "2026-04-22")
    assert overall == "PASS"


def test_compare_one_pair_missing_local_fails(tmp_path):
    df = pl.DataFrame({"date": ["2026-04-22"], "x": [1.0]})
    a = tmp_path / "a.parquet"
    b = tmp_path / "b.parquet"; _write(b, df)
    overall, metrics = compare_one_pair("test", a, b, "2026-04-22")
    assert overall == "FAIL"
    assert any(m.status == "FAIL" and m.name == "existence" for m in metrics)


def test_compare_one_pair_value_drift_fails(tmp_path):
    base = pl.DataFrame({
        "date": ["2026-04-22"] * 2,
        "close": [100.0, 200.0],
    })
    drift = pl.DataFrame({
        "date": ["2026-04-22"] * 2,
        "close": [100.0, 250.0],   # 25% change in second value
    })
    a = tmp_path / "a.parquet"; _write(a, base)
    b = tmp_path / "b.parquet"; _write(b, drift)
    overall, _ = compare_one_pair("test", a, b, "2026-04-22")
    assert overall == "FAIL"


from tests.compare_test_vs_server import render_report
from tests._compare_helpers import MetricResult


def test_compare_one_pair_both_empty_after_filter_warns(tmp_path):
    """When date filter reduces both DataFrames to 0 rows, overall should be WARN.

    Without this short-circuit, compare_row_count would PASS (0 == 0) and per-column
    stats would be silently skipped — a completely broken run would look green.
    """
    # Both frames have data, but for a DIFFERENT date than what we filter for.
    df = pl.DataFrame({
        "date": ["2026-04-21", "2026-04-21"],  # wrong date
        "close": [100.0, 200.0],
    })
    a = tmp_path / "a.parquet"; _write(a, df)
    b = tmp_path / "b.parquet"; _write(b, df)
    overall, metrics = compare_one_pair("test", a, b, "2026-04-22")
    assert overall == "WARN"
    assert any(m.name == "rows" and m.status == "WARN" for m in metrics)


def test_render_report_summary_counts():
    results = [
        ("a", "PASS", [MetricResult("rows", 10, 10, 0, "PASS")]),
        ("b", "WARN", [MetricResult("rows", 10, 11, 1, "WARN")]),
        ("c", "FAIL", [MetricResult("rows", 10, 50, -40, "FAIL")]),
    ]
    text = render_report("2026-04-22", results)
    assert "PASS 1 / WARN 1 / FAIL 1" in text
    assert "## Failures" in text
    assert "`c`" in text
