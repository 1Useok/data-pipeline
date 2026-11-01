#!/usr/bin/env python3
"""Quarantine known-invalid KIS stages while preserving bytes and hashes."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path


INVALID_RELATIVE_PATHS = (
    "2026-02-02/option.parquet.stage",
    "2026-02-03/future.parquet.stage",
    "2026-02-03/option.parquet.stage",
    "2026-02-04/future.parquet.stage",
    "2026-02-04/option.parquet.stage",
    "2026-02-05/option.parquet.stage",
    "2026-02-06/future.parquet.stage",
    "2026-02-09/future.parquet.stage",
)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("/data_db"))
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--reason", default="night_boundary_bug_20260814")
    parser.add_argument("--relative", action="append")
    args = parser.parse_args()

    work = args.root / "_backfill_work" / "kis_minute"
    quarantine = work / "_quarantine" / args.reason
    records = []
    relative_paths = tuple(args.relative or INVALID_RELATIVE_PATHS)
    for relative in relative_paths:
        source = work / relative
        target = quarantine / relative
        record = {"relative": relative, "source": str(source), "target": str(target)}
        if source.exists():
            record.update({"size": source.stat().st_size, "sha256_before": sha256(source)})
            if args.apply:
                target.parent.mkdir(parents=True, exist_ok=True)
                os.replace(source, target)
                record["sha256_after"] = sha256(target)
                record["preserved"] = record["sha256_before"] == record["sha256_after"]
                if not record["preserved"]:
                    raise RuntimeError(f"Quarantine hash mismatch: {relative}")
            record["status"] = "quarantined" if args.apply else "would_quarantine"
        elif target.exists():
            record.update({
                "status": "already_quarantined",
                "size": target.stat().st_size,
                "sha256_after": sha256(target),
                "preserved": True,
            })
        else:
            record["status"] = "missing"
        records.append(record)

    result = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "applied": args.apply,
        "quarantine": str(quarantine),
        "records": records,
    }
    if args.apply:
        quarantine.mkdir(parents=True, exist_ok=True)
        manifest = quarantine / "quarantine_manifest.json"
        temporary = manifest.with_suffix(".json.tmp")
        temporary.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(temporary, manifest)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if all(item["status"] != "missing" for item in records) else 1


if __name__ == "__main__":
    raise SystemExit(main())
