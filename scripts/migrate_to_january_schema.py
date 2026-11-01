#!/usr/bin/env python3
"""Back up and atomically migrate recent pipeline files to January-2026 schemas."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from mft_common.storage import atomic_write_parquet, parquet_update_lock
from mft_koscom.continuous_future.generator import generate_continuous_future
from mft_koscom.convert.raw_to_backtesting import convert_to_backtesting
from mft_koscom.convert.schema_contract import (
    BT_SCHEMAS,
    RAW_SCHEMAS,
    normalize_raw_price_frame,
)
from mft_koscom.resample import rebuild_higher_timeframes
from mft_krx.convert import convert_archive_to_backtesting, full_recalculate
from mft_krx.convert.crawled import SCHEMAS


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def copy_verified(source: Path, destination: Path) -> dict:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if not destination.exists():
        temporary = destination.with_name(f".{destination.name}.{uuid.uuid4().hex}.tmp")
        try:
            shutil.copy2(source, temporary)
            os.replace(temporary, destination)
        finally:
            temporary.unlink(missing_ok=True)
    source_hash = sha256(source)
    destination_hash = sha256(destination)
    if source_hash != destination_hash:
        raise RuntimeError(f"Backup hash mismatch: {source} -> {destination}")
    return {
        "source": str(source),
        "destination": str(destination),
        "bytes": source.stat().st_size,
        "sha256": source_hash,
    }


def recent_targets(root: Path, months: list[str]) -> list[Path]:
    targets: set[Path] = set()
    krx = root / "MFT_backtesting" / "KR" / "krx_crawled"
    for month in months:
        targets.update(krx.rglob(f"{month}-??.parquet"))
        targets.update(krx.rglob(f"{month}.parquet"))
        for tree in (root / "MFT_raw" / "KR" / "price", root / "MFT_backtesting" / "KR" / "price"):
            targets.update(tree.rglob(f"{month}.parquet"))
    info = root / "MFT_backtesting" / "KR" / "info" / "stock_info.parquet"
    if info.exists():
        targets.add(info)
    # full_recalculate() rebuilds the complete adjustment-ratio history, not
    # just the selected recent months. Back up every file that it may replace.
    adjustment = krx / "전종목등락률"
    if adjustment.exists():
        targets.update(adjustment.glob("*.parquet"))
    return sorted(path for path in targets if path.is_file())


def legacy_krx_daily_files(root: Path, months: list[str]) -> list[Path]:
    krx = root / "MFT_backtesting" / "KR" / "krx_crawled"
    return sorted(
        path
        for month in months
        for path in krx.rglob(f"{month}-??.parquet")
        if path.is_file()
    )


def backup_targets(root: Path, backup_root: Path, paths: list[Path]) -> list[dict]:
    entries = []
    for path in paths:
        entries.append(copy_verified(path, backup_root / path.relative_to(root)))
    return entries


def archive_legacy_krx(root: Path, paths: list[Path]) -> list[dict]:
    bt_root = root / "MFT_backtesting" / "KR" / "krx_crawled"
    archive_root = root / "MFT_archive" / "KR" / "krx_crawled"
    entries = []
    for source in paths:
        destination = archive_root / source.relative_to(bt_root)
        if destination.exists() and sha256(destination) != sha256(source):
            raise RuntimeError(f"Refusing to overwrite different archived source: {destination}")
        entries.append(copy_verified(source, destination))
    return entries


def retire_legacy_krx(paths: list[Path]) -> list[dict]:
    """Reversibly remove legacy daily files from the active Parquet set."""
    entries = []
    for source in paths:
        retired = source.with_name(f"{source.name}.pre_january_schema")
        source_hash = sha256(source)
        if retired.exists():
            if sha256(retired) != source_hash:
                raise RuntimeError(f"Refusing to replace different retired file: {retired}")
            raise RuntimeError(f"Both active and retired copies exist: {source}")
        os.replace(source, retired)
        if sha256(retired) != source_hash:
            raise RuntimeError(f"Retired-file hash mismatch: {source} -> {retired}")
        entries.append({
            "source": str(source),
            "retired_path": str(retired),
            "bytes": retired.stat().st_size,
            "sha256": source_hash,
        })
    return entries


def normalize_raw_months(root: Path, months: list[str]) -> list[dict]:
    price_root = root / "MFT_raw" / "KR" / "price"
    entries = []
    for month in months:
        for asset in ("stock", "future", "option"):
            path = price_root / "1m" / asset / f"{month}.parquet"
            if not path.exists():
                continue
            frame = normalize_raw_price_frame(pl.read_parquet(path), asset)
            frame = frame.unique(subset=["sec_code", "trade_time"], keep="last").sort(["sec_code", "trade_time"])
            with parquet_update_lock(path):
                atomic_write_parquet(frame, path, compression="lz4")
            entries.append({"path": str(path), "rows": len(frame), "asset": asset, "month": month})
    return entries


def rebuild_price_outputs(root: Path, months: list[str]) -> dict:
    raw_price = root / "MFT_raw" / "KR" / "price"
    bt_price = root / "MFT_backtesting" / "KR" / "price"
    continuous = generate_continuous_future(
        source_dir=raw_price / "1m" / "future",
        target_dir=raw_price / "1m" / "continuous_future",
        year_months=months,
    )
    converted = convert_to_backtesting(
        raw_dir=str(raw_price),
        bt_dir=str(bt_price),
        info_path=str(root / "MFT_backtesting" / "KR" / "info" / "stock_info.parquet"),
        krx_change_dir=str(root / "MFT_backtesting" / "KR" / "krx_crawled" / "전종목등락률"),
        months=months,
        timeframes=["1m"],
        asset_types=["stock", "future", "option"],
    )
    failures = [item for item in converted if item.get("status") != "success"]
    if failures:
        raise RuntimeError(f"Backtesting 1m conversion failed: {failures}")
    resampled = rebuild_higher_timeframes(root, months=months)
    return {"continuous": continuous, "converted": converted, "resampled": resampled}


def validate_schemas(root: Path, months: list[str]) -> list[dict]:
    results = []
    for month in months:
        for timeframe in ("1m", "1h", "1d"):
            for asset, expected in RAW_SCHEMAS.items():
                # January has no higher-timeframe continuous-future collection.
                if asset == "continuous_future" and timeframe != "1m":
                    continue
                path = root / "MFT_raw" / "KR" / "price" / timeframe / asset / f"{month}.parquet"
                if not path.exists():
                    continue
                actual = pl.read_parquet_schema(path)
                if list(actual.items()) != list(expected):
                    raise RuntimeError(f"Raw schema mismatch after migration: {path}: {actual}")
                results.append({"path": str(path), "schema": "PASS"})
        for timeframe in ("1m", "5m", "15m", "30m", "1h", "1d"):
            for asset, expected in BT_SCHEMAS.items():
                path = root / "MFT_backtesting" / "KR" / "price" / timeframe / asset / f"{month}.parquet"
                if not path.exists():
                    continue
                actual = pl.read_parquet_schema(path)
                if list(actual.items()) != list(expected):
                    raise RuntimeError(f"Backtesting schema mismatch after migration: {path}: {actual}")
                results.append({"path": str(path), "schema": "PASS"})
        for item, expected in SCHEMAS.items():
            base = root / "MFT_backtesting" / "KR" / "krx_crawled" / item
            paths = list(base.rglob(f"{month}.parquet")) if item == "지수구성종목" else [base / f"{month}.parquet"]
            for path in paths:
                if not path.exists():
                    continue
                actual = pl.read_parquet_schema(path)
                if list(actual.items()) != list(expected.columns):
                    raise RuntimeError(f"KRX schema mismatch after migration: {path}: {actual}")
                results.append({"path": str(path), "schema": "PASS"})
    return results


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("/data_db"))
    parser.add_argument("--months", nargs="+", required=True)
    parser.add_argument("--backup-root", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    targets = recent_targets(args.root, args.months)
    legacy = legacy_krx_daily_files(args.root, args.months)
    preview = {
        "root": str(args.root),
        "months": args.months,
        "target_file_count": len(targets),
        "target_bytes": sum(path.stat().st_size for path in targets),
        "legacy_krx_daily_file_count": len(legacy),
        "backup_root": str(args.backup_root),
        "apply": args.apply,
    }
    if not args.apply:
        print(json.dumps(preview, ensure_ascii=False, indent=2))
        return 0

    args.backup_root.mkdir(parents=True, exist_ok=True)
    backup_entries = backup_targets(args.root, args.backup_root, targets)
    archive_entries = archive_legacy_krx(args.root, legacy)
    krx = convert_archive_to_backtesting(args.root, months=args.months)
    adjustment = full_recalculate(args.root)
    raw = normalize_raw_months(args.root, args.months)
    price = rebuild_price_outputs(args.root, args.months)
    validation = validate_schemas(args.root, args.months)

    # Keep every original byte recoverable while excluding the noncanonical
    # daily files from active *.parquet readers. Reversing this only requires
    # restoring the original suffix.
    retired = retire_legacy_krx(legacy)

    manifest = {
        **preview,
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "backup_entries": backup_entries,
        "archived_legacy_entries": archive_entries,
        "krx_conversion": krx,
        "adjustment": adjustment,
        "raw_normalization": raw,
        "price_rebuild": price,
        "schema_validation": validation,
        "retired_legacy_daily_files": retired,
    }
    manifest_path = args.backup_root / "migration_manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({
        "status": "success",
        "backup_files": len(backup_entries),
        "archived_legacy_files": len(archive_entries),
        "retired_legacy_files": len(retired),
        "validated_files": len(validation),
        "manifest": str(manifest_path),
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
