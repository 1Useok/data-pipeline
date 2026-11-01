"""Run the local pipeline DAG entrypoints against test_data_db for one date.

Usage (both forms work):
    python tests/run_yesterday_against_test_db.py --date 2026-04-22 [--clean] [--start-from STEP]
    python -m tests.run_yesterday_against_test_db --date 2026-04-22

Steps (in order): kis_minute, investor_trading, convert, continuous_future
"""
from __future__ import annotations
import argparse
import sys
import traceback
from datetime import datetime, timedelta
from pathlib import Path

# Force UTF-8 output on Windows so Korean path components and em-dashes don't
# raise UnicodeEncodeError when printed to a cp949 console.
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except (AttributeError, OSError):
    pass  # Older Python or non-reconfigurable stream — errors will be caught below.

# Self-bootstrap: put repo root on sys.path so `from tests.conftest import ...`
# works whether we're invoked as a script (`python tests/run_yesterday_...`)
# or as a module (`python -m tests.run_yesterday_...`).
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from tests.conftest import REPO_ROOT, DATA_DB, TEST_DATA_DB, OUTPUT_DIR
from tests._runner_helpers import (
    setup_env,
    reset_test_data_db,
    seed_convert_dependencies,
    verify_outputs,
)

ASSETS = ["stock", "future", "option"]
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

STEPS_ORDER = ["kis_minute", "investor_trading", "convert", "continuous_future"]


def expected_outputs(step: str, target: datetime) -> list[Path]:
    ym = target.strftime("%Y-%m")
    date_str = target.strftime("%Y-%m-%d")
    if step == "kis_minute":
        return [TEST_DATA_DB / f"MFT_raw/KR/price/1m/{a}/{ym}.parquet" for a in ASSETS]
    if step == "investor_trading":
        return [
            TEST_DATA_DB / f"MFT_archive/KR/kis_api/investor_trading_time_{m}/{date_str}.parquet"
            for m in INVESTOR_MARKETS
        ]
    if step == "convert":
        return [TEST_DATA_DB / f"MFT_backtesting/KR/price/1m/{a}/{ym}.parquet" for a in ASSETS]
    if step == "continuous_future":
        # The continuous-future generator writes a single 1m file under MFT_raw.
        return [
            TEST_DATA_DB / f"MFT_raw/KR/price/1m/continuous_future/{ym}.parquet",
        ]
    raise ValueError(f"unknown step: {step}")


def run_step(step: str, target: datetime) -> None:
    print(f"\n=== STEP: {step} ===")
    if step == "kis_minute":
        import kis_minute_batch
        kis_minute_batch.collect_minute_bars(logical_date=target)
    elif step == "investor_trading":
        import kis_investor_trading
        kis_investor_trading.collect_investor_trading_time(logical_date=target)
    elif step == "convert":
        import convert_raw_to_bt
        convert_raw_to_bt.convert_raw_to_backtesting(logical_date=target)
    elif step == "continuous_future":
        import convert_raw_to_bt
        convert_raw_to_bt.generate_continuous_futures(logical_date=target)
    else:
        raise ValueError(f"unknown step: {step}")
    verify_outputs(expected_outputs(step, target), step)


def main():
    parser = argparse.ArgumentParser()
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    parser.add_argument("--date", default=yesterday)
    parser.add_argument("--clean", action="store_true",
                        help="Wipe test_data_db before running")
    parser.add_argument("--start-from", choices=STEPS_ORDER, default=None,
                        help="Resume from this step (skip earlier steps)")
    args = parser.parse_args()

    target = datetime.strptime(args.date, "%Y-%m-%d")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    log_path = OUTPUT_DIR / f"run_log_{args.date}.txt"

    if args.clean:
        print(f"Wiping {TEST_DATA_DB}")
        reset_test_data_db(TEST_DATA_DB)
    TEST_DATA_DB.mkdir(parents=True, exist_ok=True)

    try:
        setup_env(TEST_DATA_DB)

        # Seed convert dependencies once (idempotent — overwrites)
        print("Seeding convert dependencies from data_db ...")
        for line in seed_convert_dependencies(TEST_DATA_DB, DATA_DB, target):
            print(line)

        start_idx = STEPS_ORDER.index(args.start_from) if args.start_from else 0
        steps_to_run = STEPS_ORDER[start_idx:]
        print(f"Running steps: {steps_to_run}")

        for step in steps_to_run:
            run_step(step, target)
    except Exception:
        # ASCII-only banner: Windows cp949 console can't encode em-dash.
        print("\n!!! FAILURE -- stopping. Traceback follows.")
        tb = traceback.format_exc()
        try:
            print(tb)
        except UnicodeEncodeError:
            # Console can't print some Unicode; fall back to ASCII-safe version.
            print(tb.encode("ascii", "replace").decode("ascii"))
        log_path.write_text(tb, encoding="utf-8")
        print(f"\nLog: {log_path}")
        sys.exit(1)

    print("\n=== ALL STEPS PASSED ===")
    log_path.write_text("All steps passed.\n", encoding="utf-8")


if __name__ == "__main__":
    main()
