#!/usr/bin/env python3
"""Compare live KIS investor-market output with the January Parquet contract."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

import kis_investor_trading as pipeline


def schema_items(schema) -> list[tuple[str, str]]:
    return [(name, str(dtype)) for name, dtype in schema.items()]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("/data_db"))
    parser.add_argument("--json-output", type=Path, required=True)
    parser.add_argument("--markdown-output", type=Path, required=True)
    args = parser.parse_args()

    live = pipeline.sync_fetch_all_markets(pipeline.load_config())
    items = []
    base = args.root / "MFT_archive" / "KR" / "kis_api"
    for market in pipeline.MARKETS:
        name = market["name"]
        january_files = sorted(
            (base / f"investor_trading_time_{name}").glob("2026-01-*.parquet")
        )
        if not january_files:
            items.append({"market": name, "status": "FAIL", "reason": "January file missing"})
            continue
        if name not in live:
            items.append({"market": name, "status": "FAIL", "reason": "live API response missing"})
            continue

        january_schema = schema_items(pl.read_parquet_schema(january_files[0]))
        live_schema = schema_items(live[name].schema)
        items.append({
            "market": name,
            "code": market["code"],
            "product_code": market["index_code"],
            "january_file": str(january_files[0]),
            "january_schema": january_schema,
            "live_schema": live_schema,
            "live_rows": len(live[name]),
            "status": "PASS" if live_schema == january_schema else "FAIL",
        })

    report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "baseline": "2026-01",
        "endpoint": pipeline.load_config()["kis_api_endpoints"][pipeline.TR_ID],
        "tr_id": pipeline.TR_ID,
        "markets": items,
        "passed": sum(item["status"] == "PASS" for item in items),
        "failed": sum(item["status"] != "PASS" for item in items),
    }
    args.json_output.parent.mkdir(parents=True, exist_ok=True)
    args.markdown_output.parent.mkdir(parents=True, exist_ok=True)
    args.json_output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        "# KIS 투자자 11종 2026-01 계약 검증",
        "",
        f"- 기준: `2026-01` Parquet 스키마",
        f"- TR ID: `{report['tr_id']}`",
        f"- endpoint: `{report['endpoint']}`",
        f"- 결과: PASS {report['passed']} / FAIL {report['failed']}",
        "",
        "| 데이터 종류 | KIS 코드 | 현재 응답 행 | 스키마 |",
        "|---|---|---:|---|",
    ]
    for item in items:
        code = f"{item.get('code', '-')} / {item.get('product_code', '-')}"
        lines.append(
            f"| `{item['market']}` | `{code}` | {item.get('live_rows', 0)} | {item['status']} |"
        )
    args.markdown_output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps({"passed": report["passed"], "failed": report["failed"]}, ensure_ascii=False))
    return 1 if report["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
