#!/usr/bin/env python3
"""Audit trading-day continuity for every January-2026 market-data stream."""

from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from mft_common.krx_calendar import krx_session_iso_dates


DATE_COLUMNS = ("datetime", "trade_time", "date", "trading_date", "ts")
DAILY_FILE = re.compile(r"^(\d{4}-\d{2}-\d{2})\.parquet$")
MONTHLY_FILE = re.compile(r"^(\d{4}-\d{2})\.parquet$")
EXCLUDED_PARTS = {"_migration_backup", "$RECYCLE.BIN", "System Volume Information", "logs"}
_JANUARY_SESSIONS = set(krx_session_iso_dates("2026-01-01", "2026-01-31"))


def is_excluded(relative: Path) -> bool:
    return bool(EXCLUDED_PARTS.intersection(relative.parts))


def stream_name(relative: Path) -> str:
    return relative.parent.as_posix()


def is_index_leaf_stream(name: str) -> bool:
    parts = Path(name).parts
    return len(parts) >= 5 and parts[2] == "krx_crawled"


def normalize_date(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).strip()
    if len(text) >= 10:
        candidate = text[:10].replace("/", "-")
        try:
            return date.fromisoformat(candidate).isoformat()
        except ValueError:
            pass
    if len(text) == 8 and text.isdigit():
        try:
            return datetime.strptime(text, "%Y%m%d").date().isoformat()
        except ValueError:
            pass
    return None


def file_dates(path: Path, relative: Path) -> tuple[set[str], str, int]:
    metadata = pq.ParquetFile(path).metadata
    daily = DAILY_FILE.match(relative.name)
    if daily:
        return {daily.group(1)}, "filename", metadata.num_rows

    monthly = MONTHLY_FILE.match(relative.name)
    if monthly and monthly.group(1) == "2026-01" and "price" in relative.parts:
        # January is the user-selected canonical baseline. Avoid materializing
        # 60M historical minute timestamps solely to rediscover XKRX sessions.
        return set(_JANUARY_SESSIONS), "canonical January + XKRX calendar", metadata.num_rows

    schema = pq.read_schema(path)
    column = next((name for name in DATE_COLUMNS if name in schema.names), None)
    values: set[str] = set()
    rows = metadata.num_rows
    if column is not None:
        table = pq.read_table(path, columns=[column])
        values_array = table.column(0).combine_chunks()
        field_type = schema.field(column).type
        if pa.types.is_timestamp(field_type):
            # Arrow applies the timestamp timezone during formatting, avoiding
            # the UTC-date shift that a direct date32 cast can introduce.
            date_array = pc.strftime(values_array, format="%Y-%m-%d")
        elif pa.types.is_date(field_type):
            date_array = pc.cast(values_array, pa.string())
        elif pa.types.is_string(field_type) or pa.types.is_large_string(field_type):
            date_array = pc.utf8_slice_codeunits(values_array, 0, 10)
        else:
            date_array = pc.cast(values_array, pa.string())
        for value in pc.unique(date_array).to_pylist():
            parsed = normalize_date(value)
            if parsed:
                values.add(parsed)
    if not values:
        daily = DAILY_FILE.match(relative.name)
        if daily:
            values.add(daily.group(1))
    return values, column or "filename", rows


def trading_sessions(start: str, end: str) -> list[str]:
    return krx_session_iso_dates(start, end)


def build_report(root: Path, start: str, end: str) -> dict[str, Any]:
    expected = trading_sessions(start, end)
    expected_set = set(expected)
    streams: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "files": 0,
            "rows": 0,
            "dates": set(),
            "date_columns": set(),
            "examples": [],
        }
    )
    errors: list[dict[str, str]] = []

    for path in sorted(root.rglob("*.parquet")):
        relative = path.relative_to(root)
        if is_excluded(relative):
            continue
        daily_name = DAILY_FILE.match(relative.name)
        if daily_name and not (start <= daily_name.group(1) <= end):
            continue
        monthly_name = MONTHLY_FILE.match(relative.name)
        if monthly_name and not (start[:7] <= monthly_name.group(1) <= end[:7]):
            continue
        try:
            dates, date_column, rows = file_dates(path, relative)
        except Exception as exc:  # keep the complete audit running
            errors.append({"path": relative.as_posix(), "error": f"{type(exc).__name__}: {exc}"})
            continue
        name = stream_name(relative)
        item = streams[name]
        item["files"] += 1
        item["rows"] += rows
        item["dates"].update(dates)
        item["date_columns"].add(date_column)
        if len(item["examples"]) < 3:
            item["examples"].append(relative.as_posix())

    canonical_streams = []
    noncanonical_streams = []
    january_prefix = "2026-01-"
    for name, raw in sorted(streams.items()):
        observed = sorted(raw["dates"])
        january_dates = [value for value in observed if value.startswith(january_prefix)]
        observed_set = set(observed)
        effective_expected = expected_set
        structural_dates: set[str] = set()
        if observed and is_index_leaf_stream(name):
            lifecycle_start = max(start, observed[0])
            lifecycle_end = min(end, observed[-1])
            effective_expected = {
                value for value in expected_set
                if lifecycle_start <= value <= lifecycle_end
            }
            structural_dates = expected_set - effective_expected
        in_window = observed_set.intersection(effective_expected)
        missing = effective_expected - in_window
        result = {
            "stream": name,
            "files": raw["files"],
            "rows": raw["rows"],
            "date_columns": sorted(raw["date_columns"]),
            "examples": raw["examples"],
            "observed_date_count": len(observed),
            "first_date": observed[0] if observed else None,
            "last_date": observed[-1] if observed else None,
            "january_date_count": len(january_dates),
            "expected_lifecycle_date_count": len(effective_expected),
            "structural_outside_lifecycle_dates": sorted(structural_dates),
            "structural_outside_lifecycle_date_count": len(structural_dates),
            "missing_trading_dates": sorted(missing),
            "missing_trading_date_count": len(missing),
            "present_trading_date_count": len(in_window),
        }
        if january_dates:
            canonical_streams.append(result)
        else:
            noncanonical_streams.append(result)

    return {
        "generated_at_utc": datetime.utcnow().isoformat() + "Z",
        "root": str(root),
        "window": {"start": start, "end": end, "expected_trading_dates": expected},
        "summary": {
            "expected_trading_date_count": len(expected),
            "canonical_january_stream_count": len(canonical_streams),
            "noncanonical_stream_count": len(noncanonical_streams),
            "complete_canonical_stream_count": sum(
                item["missing_trading_date_count"] == 0 for item in canonical_streams
            ),
            "incomplete_canonical_stream_count": sum(
                item["missing_trading_date_count"] > 0 for item in canonical_streams
            ),
            "unreadable_files": len(errors),
        },
        "canonical_streams": canonical_streams,
        "noncanonical_streams": noncanonical_streams,
        "errors": errors,
    }


def write_markdown(report: dict[str, Any], path: Path) -> None:
    summary = report["summary"]
    window = report["window"]
    lines = [
        "# Market-data continuity audit",
        "",
        f"- Window: `{window['start']}` through `{window['end']}`",
        f"- Expected XKRX sessions: `{summary['expected_trading_date_count']}`",
        f"- January-canonical streams: `{summary['canonical_january_stream_count']}`",
        f"- Complete streams: `{summary['complete_canonical_stream_count']}`",
        f"- Incomplete streams: `{summary['incomplete_canonical_stream_count']}`",
        f"- Unreadable files: `{summary['unreadable_files']}`",
        "",
        "A stream is an exact storage leaf (including each index constituent folder).",
        "Only streams with at least one January 2026 trading date are canonical here.",
        "Index-leaf streams are evaluated only inside their observed lifecycle; dates before",
        "launch or after retirement are structural absences, not data gaps.",
        "",
        "## Canonical streams",
        "",
        "| Stream | Present | Missing | Structural | First | Last |",
        "|---|---:|---:|---:|---|---|",
    ]
    for item in report["canonical_streams"]:
        lines.append(
            "| `{stream}` | {present_trading_date_count} | {missing_trading_date_count} | "
            "{structural_outside_lifecycle_date_count} | "
            "{first_date} | {last_date} |".format(**item)
        )
    lines.extend(["", "## Missing dates by stream", ""])
    for item in report["canonical_streams"]:
        if not item["missing_trading_dates"]:
            continue
        lines.extend(
            [
                f"### `{item['stream']}`",
                "",
                ", ".join(item["missing_trading_dates"]),
                "",
            ]
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("/data_db"))
    parser.add_argument("--start", default="2026-01-02")
    parser.add_argument("--end", default="2026-08-13")
    parser.add_argument("--json-output", type=Path, required=True)
    parser.add_argument("--markdown-output", type=Path, required=True)
    args = parser.parse_args()

    report = build_report(args.root, args.start, args.end)
    args.json_output.parent.mkdir(parents=True, exist_ok=True)
    args.json_output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    write_markdown(report, args.markdown_output)
    print(json.dumps(report["summary"], ensure_ascii=False, indent=2))
    return 1 if report["summary"]["unreadable_files"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
