"""Environment setup and dependency pre-seeding for the runner."""
from __future__ import annotations
import os
import shutil
from pathlib import Path
from datetime import datetime


# Files that the convert step reads but doesn't produce — must exist in test_data_db.
CONVERT_DEPENDENCIES_REL = [
    "MFT_backtesting/KR/info/stock_info.parquet",
    "MFT_backtesting/KR/info/future_info.parquet",
    "MFT_backtesting/KR/info/option_info.parquet",
]


def setup_env(test_data_db: Path) -> None:
    """Set DATA_DB_PATH and load KIS credentials from docker/.env."""
    os.environ["DATA_DB_PATH"] = str(test_data_db)
    # docker/.env loader (defined in conftest)
    from tests.conftest import load_docker_env
    load_docker_env()
    if not os.environ.get("PROD_KIS_APP_KEY"):
        raise RuntimeError("PROD_KIS_APP_KEY not set after loading docker/.env")


def reset_test_data_db(test_data_db: Path) -> None:
    """Wipe and recreate the test_data_db tree.

    Safety: refuses to operate on any path whose final component is not
    literally 'test_data_db' to prevent accidental wipes of production data.
    """
    if test_data_db.name != "test_data_db":
        raise RuntimeError(
            f"Refusing to wipe {test_data_db} — directory name is not 'test_data_db'"
        )
    if test_data_db.exists():
        shutil.rmtree(test_data_db)
    test_data_db.mkdir(parents=True, exist_ok=True)


# KRX crawled items that kis_minute_batch's universe loader reads (monthly parquet per item).
# Must be pre-seeded from data_db because KRX crawling is network-blocked locally.
KRX_CRAWLED_UNIVERSE_ITEMS = [
    "전종목등락률",      # used by convert step
    "전종목시세",         # used by universe (stock list)
    "ETF전종목시세",     # used by universe (ETF list)
    "파생전종목시세",    # used by universe (derivative list)
]


def seed_convert_dependencies(test_data_db: Path, data_db: Path, target_date: datetime) -> list[str]:
    """Copy info files + required monthly KRX crawled files from data_db.

    The local pipeline's universe loader reads several KRX-crawled directories
    that the pipeline itself can't produce (KRX crawling is blocked locally).
    We seed all of them from the production mirror so downstream DAG steps work.

    Returns a list of human-readable lines describing what was seeded.
    """
    log = []
    for rel in CONVERT_DEPENDENCIES_REL:
        src = data_db / rel
        dst = test_data_db / rel
        if not src.exists():
            raise FileNotFoundError(f"Convert dependency missing in data_db: {src}")
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        log.append(f"  seeded {rel}")

    ym = target_date.strftime("%Y-%m")
    for item in KRX_CRAWLED_UNIVERSE_ITEMS:
        krx_rel = f"MFT_backtesting/KR/krx_crawled/{item}/{ym}.parquet"
        src = data_db / krx_rel
        dst = test_data_db / krx_rel
        if not src.exists():
            raise FileNotFoundError(f"KRX {item} missing for {ym}: {src}")
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        log.append(f"  seeded {krx_rel}")
    return log


def verify_outputs(paths: list[Path], step_name: str) -> None:
    """Raise if any expected output is missing or empty."""
    import polars as pl
    missing = [p for p in paths if not p.exists()]
    if missing:
        raise RuntimeError(
            f"[{step_name}] missing expected outputs:\n  " + "\n  ".join(str(p) for p in missing)
        )
    empty = []
    for p in paths:
        try:
            n = pl.read_parquet(p).height
            if n == 0:
                empty.append(p)
        except Exception as e:
            raise RuntimeError(f"[{step_name}] cannot read {p}: {e}")
    if empty:
        raise RuntimeError(
            f"[{step_name}] outputs are empty (0 rows):\n  " + "\n  ".join(str(p) for p in empty)
        )
