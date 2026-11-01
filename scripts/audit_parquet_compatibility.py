#!/usr/bin/env python3
"""Audit January 2026 versus August-and-later 2026 Parquet compatibility."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq


DATE_RE = re.compile(r"2026-(\d{2})(?:-(\d{2}))?")
EXCLUDED_ROOT_NAMES = {"_migration_backup", "$RECYCLE.BIN", "System Volume Information"}
JANUARY = "2026-01"
AUGUST_PLUS = "2026-08+"


def stable_id(value: Any) -> str:
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def encode_metadata(metadata: dict[bytes, bytes] | None) -> dict[str, str]:
    if not metadata:
        return {}
    encoded: dict[str, str] = {}
    for key, value in sorted(metadata.items()):
        key_text = key.decode("utf-8", errors="backslashreplace")
        try:
            value_text = value.decode("utf-8")
        except UnicodeDecodeError:
            value_text = "base64:" + base64.b64encode(value).decode("ascii")
        encoded[key_text] = value_text
    return encoded


def period_for(path: Path) -> str | None:
    matches = DATE_RE.findall(path.name)
    if not matches:
        return None
    month = int(matches[-1][0])
    if month == 1:
        return JANUARY
    if month >= 8:
        return AUGUST_PLUS
    return None


def collection_kind(relative_path: Path) -> str:
    """Collapse parameterized index names into the underlying collection kind."""
    parent_parts = relative_path.parent.parts
    if "지수구성종목" in parent_parts:
        index = parent_parts.index("지수구성종목")
        return Path(*parent_parts[: index + 1]).as_posix()
    return relative_path.parent.as_posix()


@dataclass(frozen=True)
class FileRecord:
    relative_path: str
    dataset: str
    period: str
    size: int
    rows: int
    logical_schema_id: str
    physical_schema_id: str
    schema_metadata_id: str
    writer_id: str
    logical_schema: list[dict[str, Any]]
    physical_schema: str
    schema_metadata: dict[str, str]
    writer: dict[str, Any]


def inspect_file(root: Path, path: Path) -> FileRecord:
    relative = path.relative_to(root)
    period = period_for(path)
    if period is None:
        raise ValueError(f"No audit period for {relative}")

    parquet_file = pq.ParquetFile(path)
    arrow_schema = parquet_file.schema_arrow
    metadata = parquet_file.metadata

    logical_schema = [
        {
            "position": position,
            "name": field.name,
            "type": str(field.type),
            "nullable": field.nullable,
        }
        for position, field in enumerate(arrow_schema)
    ]
    # The first line contains a process-specific Python object address. It is not
    # part of the Parquet schema and must not participate in compatibility IDs.
    physical_schema = "\n".join(str(parquet_file.schema).splitlines()[1:])
    schema_metadata = encode_metadata(arrow_schema.metadata)

    compressions: set[str] = set()
    encodings: set[str] = set()
    for row_group_index in range(metadata.num_row_groups):
        row_group = metadata.row_group(row_group_index)
        for column_index in range(row_group.num_columns):
            column = row_group.column(column_index)
            compressions.add(str(column.compression))
            encodings.update(str(item) for item in column.encodings)

    writer = {
        "format_version": str(metadata.format_version),
        "created_by": metadata.created_by or "",
        "compressions": sorted(compressions),
        "encodings": sorted(encodings),
    }

    return FileRecord(
        relative_path=relative.as_posix(),
        dataset=collection_kind(relative),
        period=period,
        size=path.stat().st_size,
        rows=metadata.num_rows,
        logical_schema_id=stable_id(logical_schema),
        physical_schema_id=stable_id(physical_schema),
        schema_metadata_id=stable_id(schema_metadata),
        writer_id=stable_id(writer),
        logical_schema=logical_schema,
        physical_schema=physical_schema,
        schema_metadata=schema_metadata,
        writer=writer,
    )


def schema_variants(records: list[FileRecord], attribute: str) -> list[dict[str, Any]]:
    counts = Counter(getattr(record, attribute) for record in records)
    examples: dict[str, str] = {}
    for record in records:
        examples.setdefault(getattr(record, attribute), record.relative_path)
    return [
        {"id": variant_id, "files": count, "example": examples[variant_id]}
        for variant_id, count in counts.most_common()
    ]


def most_common_record(records: list[FileRecord], attribute: str) -> FileRecord:
    common_id = Counter(getattr(record, attribute) for record in records).most_common(1)[0][0]
    return next(record for record in records if getattr(record, attribute) == common_id)


def logical_schema_diff(january: list[dict[str, Any]], august: list[dict[str, Any]]) -> dict[str, Any]:
    january_by_name = {field["name"]: field for field in january}
    august_by_name = {field["name"]: field for field in august}
    shared_names = sorted(january_by_name.keys() & august_by_name.keys())
    changed = []
    for name in shared_names:
        jan_field = january_by_name[name]
        aug_field = august_by_name[name]
        if jan_field["type"] != aug_field["type"] or jan_field["nullable"] != aug_field["nullable"]:
            changed.append(
                {
                    "name": name,
                    "january_type": jan_field["type"],
                    "august_type": aug_field["type"],
                    "january_nullable": jan_field["nullable"],
                    "august_nullable": aug_field["nullable"],
                }
            )
    return {
        "january_order": [field["name"] for field in january],
        "august_order": [field["name"] for field in august],
        "only_january": sorted(january_by_name.keys() - august_by_name.keys()),
        "only_august": sorted(august_by_name.keys() - january_by_name.keys()),
        "changed_type_or_nullability": changed,
    }


def audit(root: Path, workers: int) -> dict[str, Any]:
    candidates: list[Path] = []
    undated_parquet: list[str] = []
    non_parquet: list[str] = []
    operational_period_files: Counter[str] = Counter()
    errors: list[dict[str, str]] = []

    def record_walk_error(error: OSError) -> None:
        errors.append({"path": error.filename or str(root), "error": f"{type(error).__name__}: {error}"})

    for directory, directory_names, file_names in os.walk(root, topdown=True, onerror=record_walk_error):
        directory_names[:] = [name for name in directory_names if name not in EXCLUDED_ROOT_NAMES]
        directory_path = Path(directory)
        for file_name in file_names:
            path = directory_path / file_name
            relative = path.relative_to(root)
            if path.suffix.lower() != ".parquet":
                non_parquet.append(relative.as_posix())
                continue
            if relative.parts and relative.parts[0] == "logs":
                period = period_for(path)
                if period:
                    operational_period_files[period] += 1
                else:
                    undated_parquet.append(relative.as_posix())
                continue
            if period_for(path) is None:
                undated_parquet.append(relative.as_posix())
                continue
            candidates.append(path)

    records: list[FileRecord] = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(inspect_file, root, path): path for path in candidates}
        for future in as_completed(futures):
            path = futures[future]
            try:
                records.append(future.result())
            except Exception as error:  # noqa: BLE001 - every unreadable file must be reported
                errors.append(
                    {
                        "path": path.relative_to(root).as_posix(),
                        "error": f"{type(error).__name__}: {error}",
                    }
                )

    records.sort(key=lambda record: record.relative_path)
    grouped: dict[str, dict[str, list[FileRecord]]] = defaultdict(lambda: defaultdict(list))
    for record in records:
        grouped[record.dataset][record.period].append(record)

    january_datasets = {dataset for dataset, periods in grouped.items() if periods[JANUARY]}
    august_datasets = {dataset for dataset, periods in grouped.items() if periods[AUGUST_PLUS]}
    shared_datasets = sorted(january_datasets & august_datasets)

    dataset_results: list[dict[str, Any]] = []
    for dataset in shared_datasets:
        january_records = grouped[dataset][JANUARY]
        august_records = grouped[dataset][AUGUST_PLUS]

        logical_union = {record.logical_schema_id for record in january_records + august_records}
        physical_union = {record.physical_schema_id for record in january_records + august_records}
        metadata_union = {record.schema_metadata_id for record in january_records + august_records}
        writer_union = {record.writer_id for record in january_records + august_records}

        january_logical_ids = {record.logical_schema_id for record in january_records}
        august_logical_ids = {record.logical_schema_id for record in august_records}
        january_writer_ids = {record.writer_id for record in january_records}
        august_writer_ids = {record.writer_id for record in august_records}

        january_example = most_common_record(january_records, "logical_schema_id")
        august_example = most_common_record(august_records, "logical_schema_id")

        result = {
            "dataset": dataset,
            "january_files": len(january_records),
            "august_files": len(august_records),
            "january_rows": sum(record.rows for record in january_records),
            "august_rows": sum(record.rows for record in august_records),
            "column_schema_identical_for_every_file": len(logical_union) == 1,
            "physical_schema_identical_for_every_file": len(physical_union) == 1,
            "schema_metadata_identical_for_every_file": len(metadata_union) == 1,
            "writer_format_identical_for_every_file": len(writer_union) == 1,
            "column_schema_sets_equal_between_periods": january_logical_ids == august_logical_ids,
            "writer_format_sets_equal_between_periods": january_writer_ids == august_writer_ids,
            "january_logical_variants": schema_variants(january_records, "logical_schema_id"),
            "august_logical_variants": schema_variants(august_records, "logical_schema_id"),
            "january_physical_variants": schema_variants(january_records, "physical_schema_id"),
            "august_physical_variants": schema_variants(august_records, "physical_schema_id"),
            "january_metadata_variants": schema_variants(january_records, "schema_metadata_id"),
            "august_metadata_variants": schema_variants(august_records, "schema_metadata_id"),
            "january_writer_variants": schema_variants(january_records, "writer_id"),
            "august_writer_variants": schema_variants(august_records, "writer_id"),
            "representative_january_schema": january_example.logical_schema,
            "representative_august_schema": august_example.logical_schema,
            "representative_january_physical_schema": january_example.physical_schema,
            "representative_august_physical_schema": august_example.physical_schema,
            "representative_january_metadata": january_example.schema_metadata,
            "representative_august_metadata": august_example.schema_metadata,
            "representative_january_writer": january_example.writer,
            "representative_august_writer": august_example.writer,
            "representative_schema_diff": logical_schema_diff(
                january_example.logical_schema,
                august_example.logical_schema,
            ),
        }
        result["strict_format_match"] = all(
            (
                result["column_schema_identical_for_every_file"],
                result["physical_schema_identical_for_every_file"],
                result["schema_metadata_identical_for_every_file"],
                result["writer_format_identical_for_every_file"],
            )
        )
        dataset_results.append(result)

    period_stats: dict[str, dict[str, int]] = {}
    for period in (JANUARY, AUGUST_PLUS):
        period_records = [record for record in records if record.period == period]
        period_stats[period] = {
            "files": len(period_records),
            "bytes": sum(record.size for record in period_records),
            "rows": sum(record.rows for record in period_records),
            "datasets": len({record.dataset for record in period_records}),
        }

    january_only = sorted(january_datasets - august_datasets)
    august_only = sorted(august_datasets - january_datasets)
    strict_mismatches = [result["dataset"] for result in dataset_results if not result["strict_format_match"]]
    column_mismatches = [
        result["dataset"]
        for result in dataset_results
        if not result["column_schema_identical_for_every_file"]
    ]

    overall_pass = not errors and not january_only and not august_only and not strict_mismatches
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "root": str(root),
        "period_definition": {
            JANUARY: "filenames containing 2026-01",
            AUGUST_PLUS: "filenames containing 2026-08 through 2026-12",
        },
        "excluded_roots": sorted(EXCLUDED_ROOT_NAMES),
        "verdict": "PASS" if overall_pass else "FAIL",
        "perfect_identity_requirements": {
            "same_dataset_set": not january_only and not august_only,
            "all_shared_datasets_strict_format_match": not strict_mismatches,
            "all_files_readable": not errors,
        },
        "period_stats": period_stats,
        "january_only_datasets": january_only,
        "august_only_datasets": august_only,
        "shared_dataset_count": len(shared_datasets),
        "strict_format_mismatch_datasets": strict_mismatches,
        "column_schema_mismatch_datasets": column_mismatches,
        "dataset_results": dataset_results,
        "unreadable_or_invalid_files": sorted(errors, key=lambda item: item["path"]),
        "undated_parquet_count": len(undated_parquet),
        "undated_parquet_examples": sorted(undated_parquet)[:100],
        "non_parquet_count": len(non_parquet),
        "non_parquet_examples": sorted(non_parquet)[:100],
        "operational_log_parquet_files_excluded_from_collection_verdict": {
            JANUARY: operational_period_files[JANUARY],
            AUGUST_PLUS: operational_period_files[AUGUST_PLUS],
        },
    }


def markdown_list(items: list[str]) -> str:
    if not items:
        return "- 없음"
    return "\n".join(f"- `{item}`" for item in items)


def render_markdown(report: dict[str, Any]) -> str:
    jan = report["period_stats"][JANUARY]
    aug = report["period_stats"][AUGUST_PLUS]
    lines = [
        "# 2026-01 vs 2026-08+ Parquet compatibility audit",
        "",
        f"- 판정: **{report['verdict']}**",
        f"- 생성 시각(UTC): `{report['generated_at_utc']}`",
        f"- 읽기 실패/손상 파일: `{len(report['unreadable_or_invalid_files'])}`",
        f"- 2026-01: `{jan['files']}` files, `{jan['rows']}` rows, `{jan['datasets']}` datasets",
        f"- 2026-08+: `{aug['files']}` files, `{aug['rows']}` rows, `{aug['datasets']}` datasets",
        f"- 양쪽 공통 수집 종류: `{report['shared_dataset_count']}`",
        "- `logs/` Parquet은 시장데이터 수집 종류가 아닌 운영 산출물이므로 종류 판정에서 제외",
        f"- 컬럼 스키마 불일치 수집 종류: `{len(report['column_schema_mismatch_datasets'])}`",
        f"- 엄격 포맷 불일치 수집 종류: `{len(report['strict_format_mismatch_datasets'])}`",
        "",
        "엄격 포맷 일치는 모든 파일에서 컬럼 이름/순서, Arrow 타입, nullable, Parquet 물리 스키마,",
        "스키마 메타데이터, writer version, compression/encoding이 하나로 동일한 경우만 통과합니다.",
        "",
        "## 2026-01에만 존재하는 수집 종류",
        "",
        markdown_list(report["january_only_datasets"]),
        "",
        "## 2026-08+에만 존재하는 수집 종류",
        "",
        markdown_list(report["august_only_datasets"]),
        "",
        "## 공통 수집 종류 비교",
        "",
        "| 수집 종류 | Jan files | Aug+ files | 컬럼 | 물리 스키마 | 메타데이터 | writer/압축 | 엄격 판정 |",
        "|---|---:|---:|---|---|---|---|---|",
    ]

    for result in report["dataset_results"]:
        mark = lambda value: "PASS" if value else "FAIL"  # noqa: E731
        lines.append(
            "| `{dataset}` | {january_files} | {august_files} | {columns} | {physical} | "
            "{metadata} | {writer} | {strict} |".format(
                dataset=result["dataset"],
                january_files=result["january_files"],
                august_files=result["august_files"],
                columns=mark(result["column_schema_identical_for_every_file"]),
                physical=mark(result["physical_schema_identical_for_every_file"]),
                metadata=mark(result["schema_metadata_identical_for_every_file"]),
                writer=mark(result["writer_format_identical_for_every_file"]),
                strict=mark(result["strict_format_match"]),
            )
        )

    mismatches = [result for result in report["dataset_results"] if not result["strict_format_match"]]
    lines.extend(["", "## 불일치 상세", ""])
    if not mismatches:
        lines.append("- 없음")
    for result in mismatches:
        diff = result["representative_schema_diff"]
        lines.extend(
            [
                f"### `{result['dataset']}`",
                "",
                f"- Jan logical variants: `{len(result['january_logical_variants'])}`",
                f"- Aug+ logical variants: `{len(result['august_logical_variants'])}`",
                f"- Jan physical variants: `{len(result['january_physical_variants'])}`",
                f"- Aug+ physical variants: `{len(result['august_physical_variants'])}`",
                f"- Jan metadata variants: `{len(result['january_metadata_variants'])}`",
                f"- Aug+ metadata variants: `{len(result['august_metadata_variants'])}`",
                f"- Jan writer variants: `{len(result['january_writer_variants'])}`",
                f"- Aug+ writer variants: `{len(result['august_writer_variants'])}`",
                f"- Jan-only columns: `{diff['only_january']}`",
                f"- Aug-only columns: `{diff['only_august']}`",
                f"- Type/nullability changes: `{diff['changed_type_or_nullability']}`",
                f"- Column order equal: `{diff['january_order'] == diff['august_order']}`",
                f"- Representative Jan writer: `{result['representative_january_writer']}`",
                f"- Representative Aug writer: `{result['representative_august_writer']}`",
                "",
            ]
        )

    if report["unreadable_or_invalid_files"]:
        lines.extend(["## 읽기 실패 또는 손상 파일", ""])
        for item in report["unreadable_or_invalid_files"]:
            lines.append(f"- `{item['path']}` — {item['error']}")

    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("/data_db"))
    parser.add_argument("--json-output", type=Path, required=True)
    parser.add_argument("--markdown-output", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args()

    report = audit(args.root, max(1, args.workers))
    args.json_output.parent.mkdir(parents=True, exist_ok=True)
    args.markdown_output.parent.mkdir(parents=True, exist_ok=True)
    args.json_output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    args.markdown_output.write_text(render_markdown(report), encoding="utf-8")

    print(json.dumps(
        {
            "verdict": report["verdict"],
            "period_stats": report["period_stats"],
            "shared_dataset_count": report["shared_dataset_count"],
            "january_only_datasets": report["january_only_datasets"],
            "august_only_datasets": report["august_only_datasets"],
            "column_schema_mismatch_datasets": report["column_schema_mismatch_datasets"],
            "strict_format_mismatch_datasets": report["strict_format_mismatch_datasets"],
            "unreadable_or_invalid_files": report["unreadable_or_invalid_files"],
        },
        ensure_ascii=False,
        indent=2,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
