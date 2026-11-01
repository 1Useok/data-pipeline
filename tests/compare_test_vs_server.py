"""Compare local pipeline output (test_data_db) vs server output (compare_db).

Usage (both forms work):
    python tests/compare_test_vs_server.py --date 2026-04-22
    python -m tests.compare_test_vs_server --date 2026-04-22
"""
from __future__ import annotations
import argparse
from datetime import datetime, timedelta
from pathlib import Path
import sys
import polars as pl

# Self-bootstrap: put repo root on sys.path so `from tests.conftest import ...`
# works whether we're invoked as a script (`python tests/compare_test_vs_server.py`)
# or as a module (`python -m tests.compare_test_vs_server`).
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from tests.conftest import TEST_DATA_DB, COMPARE_DB, OUTPUT_DIR
from tests._compare_helpers import (
    MetricResult,
    compare_existence,
    compare_schema,
    compare_row_count,
    compare_numeric_column,
    compare_string_column,
    filter_to_date,
)

# Mirrors `MARKETS` in `dags/kis_investor_trading.py`.
# Mirrors the 2026-01 collection contract in ``dags/kis_investor_trading.py``.
INVESTOR_MARKETS = [
    "kospi",
    "kosdaq",
    "futures",
    "call_option",
    "put_option",
    "mini_futures",
    "mini_call_option",
    "mini_put_option",
    "kosdaq150_futures",
    "kosdaq150_call_option",
    "kosdaq150_put_option",
]

ASSETS = ["stock", "future", "option"]


def expected_pairs(target_date: str) -> list[tuple[str, Path, Path]]:
    """Return the 1-minute pipeline's local/server output pairs."""
    ym = target_date[:7]
    pairs: list[tuple[str, Path, Path]] = []

    # KIS minute (3): MFT_raw/KR/price/1m/{asset}/{ym}.parquet
    for asset in ASSETS:
        rel = Path("MFT_raw/KR/price/1m") / asset / f"{ym}.parquet"
        pairs.append((rel.as_posix(), TEST_DATA_DB / rel, COMPARE_DB / rel))

    # KIS investor trading (11): MFT_archive/KR/kis_api/investor_trading_time_{m}/{date}.parquet
    for m in INVESTOR_MARKETS:
        rel = Path("MFT_archive/KR/kis_api") / f"investor_trading_time_{m}" / f"{target_date}.parquet"
        pairs.append((rel.as_posix(), TEST_DATA_DB / rel, COMPARE_DB / rel))

    # Convert (3): MFT_backtesting/KR/price/1m/{asset}/{ym}.parquet
    for asset in ASSETS:
        rel = Path("MFT_backtesting/KR/price/1m") / asset / f"{ym}.parquet"
        pairs.append((rel.as_posix(), TEST_DATA_DB / rel, COMPARE_DB / rel))

    # Continuous future (1): the generator only writes a single 1m file
    # under MFT_raw — see commit d1708f4.
    cf_rel = Path(f"MFT_raw/KR/price/1m/continuous_future/{ym}.parquet")
    pairs.append((cf_rel.as_posix(), TEST_DATA_DB / cf_rel, COMPARE_DB / cf_rel))

    return pairs


def compare_one_pair(
    label: str, local_path: Path, server_path: Path, target_date: str
) -> tuple[str, list[MetricResult]]:
    """Run all comparisons for one file pair. Returns (overall_status, metrics)."""
    metrics: list[MetricResult] = []

    existence = compare_existence(local_path, server_path)
    metrics.append(existence)
    if existence.status == "FAIL":
        return "FAIL", metrics

    df_local = pl.read_parquet(local_path)
    df_server = pl.read_parquet(server_path)

    # Filter both to the target date so monthly files compare just one day.
    try:
        df_local = filter_to_date(df_local, target_date)
        df_server = filter_to_date(df_server, target_date)
    except ValueError as e:
        metrics.append(MetricResult("date_filter", str(e), str(e), "skipped", "WARN"))

    # Short-circuit if both sides are empty after filtering — otherwise compare_row_count
    # will silently PASS a completely broken run. (Addresses code-review issue I3 from Task 3.)
    if len(df_local) == 0 and len(df_server) == 0:
        metrics.append(MetricResult(
            "rows", 0, 0, 0,
            "WARN",  # treat "both empty for this date" as a warning — could be a non-trading day or a batch no-op
        ))
        return "WARN", metrics

    metrics.extend(compare_schema(df_local, df_server))
    metrics.append(compare_row_count(df_local, df_server))

    # Per-column statistics for shared columns only.
    common_cols = [c for c in df_local.columns if c in df_server.columns]
    for col in common_cols:
        dt = df_local.schema[col]
        if dt.is_numeric():
            metrics.extend(compare_numeric_column(df_local, df_server, col))
        elif dt == pl.Utf8:
            metrics.append(compare_string_column(df_local, df_server, col))

    overall = "PASS"
    for r in metrics:
        if r.status == "FAIL":
            overall = "FAIL"
            break
        if r.status == "WARN":
            overall = "WARN"
    return overall, metrics


def render_report(target_date: str, results: list[tuple[str, str, list[MetricResult]]]) -> str:
    """Render a markdown report from per-pair results."""
    n_pass = sum(1 for _, s, _ in results if s == "PASS")
    n_warn = sum(1 for _, s, _ in results if s == "WARN")
    n_fail = sum(1 for _, s, _ in results if s == "FAIL")

    lines = [
        f"# Comparison Report — {target_date}",
        "",
        "## Summary",
        f"- Files: {len(results)} (PASS {n_pass} / WARN {n_warn} / FAIL {n_fail})",
        f"- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
    ]

    if n_fail > 0:
        lines.append("## Failures")
        for label, overall, metrics in results:
            if overall != "FAIL":
                continue
            lines.append(f"- `{label}`")
            for m in metrics:
                if m.status == "FAIL":
                    lines.append(f"  - {m.name}: local={m.local} server={m.server} diff={m.diff}")
        lines.append("")

    lines.append("## Per-file detail")
    for label, overall, metrics in results:
        lines.append(f"### `{label}` — {overall}")
        lines.append("")
        lines.append("| metric | local | server | diff | status |")
        lines.append("|--------|------:|-------:|-----:|--------|")
        for m in metrics:
            lines.append(
                f"| {m.name} | {m.local} | {m.server} | {m.diff} | {m.status} |"
            )
        lines.append("")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    parser.add_argument("--date", default=yesterday)
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    pairs = expected_pairs(args.date)
    results: list[tuple[str, str, list[MetricResult]]] = []
    for label, local_path, server_path in pairs:
        print(f"Comparing {label} ...")
        try:
            overall, metrics = compare_one_pair(label, local_path, server_path, args.date)
        except Exception as e:
            overall = "FAIL"
            metrics = [MetricResult("exception", type(e).__name__, "", str(e)[:200], "FAIL")]
        results.append((label, overall, metrics))
        print(f"  -> {overall}")

    report = render_report(args.date, results)
    out_path = OUTPUT_DIR / f"compare_report_{args.date}.md"
    out_path.write_text(report, encoding="utf-8")
    print(f"\nReport: {out_path}")


if __name__ == "__main__":
    main()
