"""Convert archived Korean KRX crawl frames to the January-2026 schema."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import polars as pl

from mft_common.storage import atomic_write_parquet, parquet_update_lock


@dataclass(frozen=True)
class CrawledSchema:
    columns: tuple[tuple[str, pl.DataType], ...]
    rename: dict[str, str]
    keys: tuple[str, ...]
    compression: str = "zstd"


S = pl.String
F = pl.Float64
DT = pl.Datetime("ns")


SCHEMAS: dict[str, CrawledSchema] = {
    "ETF전종목등락률": CrawledSchema(
        columns=(("datetime", DT), ("ticker", S), ("isin", S), ("korean_name", S),
                 ("base_price", F), ("end_close", F), ("change", F), ("change_rate", F),
                 ("volume", F), ("trading_value", F), ("market", S)),
        rename={"date": "datetime", "종목코드": "ticker", "sec_code": "isin", "종목명": "korean_name",
                "시작일 기준가": "base_price", "종료일 종가": "end_close", "대비": "change",
                "등락률": "change_rate", "거래량": "volume", "거래대금": "trading_value"},
        keys=("datetime", "ticker"),
    ),
    "ETF전종목시세": CrawledSchema(
        columns=(("datetime", DT), ("ticker", S), ("isin", S), ("security_group_id", S),
                 ("korean_name", S), ("close", F), ("change", F), ("change_type", S),
                 ("change_rate", F), ("nav", F), ("open", F), ("high", F), ("low", F),
                 ("volume", F), ("trading_value", F), ("market_cap", F),
                 ("net_asset_value", F), ("shares_outstanding", F),
                 ("underlying_index_name", S), ("underlying_index_close", F),
                 ("underlying_index_change", F), ("underlying_index_change_type", S),
                 ("underlying_index_change_rate", F)),
        rename={"date": "datetime", "종목코드": "ticker", "sec_code": "isin",
                "증권그룹ID": "security_group_id", "종목명": "korean_name", "종가": "close",
                "대비": "change", "등락구분": "change_type", "등락률": "change_rate", "NAV": "nav",
                "시가": "open", "고가": "high", "저가": "low", "거래량": "volume",
                "거래대금": "trading_value", "시가총액": "market_cap", "순자산총액": "net_asset_value",
                "상장주식수": "shares_outstanding", "기초지수명": "underlying_index_name",
                "기초지수종가": "underlying_index_close", "기초지수대비": "underlying_index_change",
                "기초지수등락구분": "underlying_index_change_type",
                "기초지수등락률": "underlying_index_change_rate"},
        keys=("datetime", "ticker"),
    ),
    "대량매매전일": CrawledSchema(
        columns=(("datetime", DT), ("ticker", S), ("korean_name", S), ("close", F),
                 ("change", F), ("change_rate", F), ("volume", F),
                 ("block_trade_volume", F), ("block_trade_ratio", F)),
        rename={"date": "datetime", "종목코드": "ticker", "종목명": "korean_name", "종가": "close",
                "대비": "change", "등락률": "change_rate", "거래량": "volume",
                "대량매매수량": "block_trade_volume", "대량매매비율": "block_trade_ratio"},
        keys=("datetime", "ticker"),
    ),
    "업종별분포": CrawledSchema(
        columns=(("datetime", DT), ("sector_name", S), ("company_count", F), ("stock_count", F),
                 ("shares_outstanding", F), ("market_cap_amount", F), ("market_cap_weight", F),
                 ("volume_quantity", F), ("volume_weight", F), ("trading_value_amount", F),
                 ("trading_value_weight", F), ("market", S)),
        rename={"date": "datetime", "업종명": "sector_name", "회사수": "company_count",
                "종목수": "stock_count", "상장주식수": "shares_outstanding",
                "시가총액_금액": "market_cap_amount", "시가총액_비중": "market_cap_weight",
                "거래량_수량": "volume_quantity", "거래량_비중": "volume_weight",
                "거래대금_금액": "trading_value_amount", "거래대금_비중": "trading_value_weight"},
        keys=("datetime", "sector_name", "market"),
    ),
    "업종분류현황": CrawledSchema(
        columns=(("datetime", DT), ("isin", S), ("ticker", S), ("korean_name", S),
                 ("market_type", S), ("sector_name", S), ("close", F), ("change", F),
                 ("change_rate", F), ("market_cap", F)),
        rename={"date": "datetime", "sec_code": "isin", "종목코드": "ticker", "종목명": "korean_name",
                "시장구분": "market_type", "업종명": "sector_name", "종가": "close", "대비": "change",
                "등락률": "change_rate", "시가총액": "market_cap"},
        keys=("datetime", "isin"),
    ),
    "외국인보유량": CrawledSchema(
        columns=(("datetime", DT), ("isin", S), ("ticker", S), ("korean_name", S),
                 ("close", F), ("change", F), ("change_rate", F), ("shares_outstanding", F),
                 ("foreign_holding", F), ("foreign_ownership_rate", F), ("foreign_limit", F),
                 ("foreign_limit_exhaustion_rate", F), ("market", S)),
        rename={"date": "datetime", "sec_code": "isin", "종목코드": "ticker", "종목명": "korean_name",
                "종가": "close", "대비": "change", "등락률": "change_rate",
                "상장주식수": "shares_outstanding", "외국인 보유수량": "foreign_holding",
                "외국인 지분율": "foreign_ownership_rate", "외국인 한도수량": "foreign_limit",
                "외국인 한도소진율": "foreign_limit_exhaustion_rate"},
        keys=("datetime", "isin"),
    ),
    "전종목시세": CrawledSchema(
        columns=(("datetime", DT), ("isin", S), ("ticker", S), ("korean_name", S),
                 ("market_type", S), ("division", S), ("close", F), ("change", F),
                 ("change_rate", F), ("open", F), ("high", F), ("low", F), ("volume", F),
                 ("trading_value", F), ("market_cap", F), ("shares_outstanding", F)),
        rename={"date": "datetime", "sec_code": "isin", "종목코드": "ticker", "종목명": "korean_name",
                "시장구분": "market_type", "소속부": "division", "종가": "close", "대비": "change",
                "등락률": "change_rate", "시가": "open", "고가": "high", "저가": "low",
                "거래량": "volume", "거래대금": "trading_value", "시가총액": "market_cap",
                "상장주식수": "shares_outstanding"},
        keys=("datetime", "isin"),
    ),
    "지수구성종목": CrawledSchema(
        columns=(("datetime", DT), ("isin", S), ("ticker", S), ("korean_name", S),
                 ("close", F), ("change", F), ("change_rate", F), ("market_cap", F)),
        rename={"date": "datetime", "sec_code": "isin", "종목코드": "ticker", "종목명": "korean_name",
                "종가": "close", "대비": "change", "등락률": "change_rate", "상장시가총액": "market_cap"},
        keys=("datetime", "isin"),
    ),
    "코스피지수시세추이": CrawledSchema(
        columns=(("datetime", DT), ("close", F), ("change", F), ("change_rate", F),
                 ("open", F), ("high", F), ("low", F), ("volume", F),
                 ("trading_value", F), ("market_cap", F)),
        rename={"date": "datetime", "종가": "close", "대비": "change", "등락률": "change_rate",
                "시가": "open", "고가": "high", "저가": "low", "거래량": "volume",
                "거래대금": "trading_value", "상장시가총액": "market_cap"},
        keys=("datetime",),
    ),
    "투자자별거래실적개별종목": CrawledSchema(
        columns=(("datetime", DT), ("isin", S), ("ticker", S), ("financial_investment", F),
                 ("insurance", F), ("investment_trust", F), ("private_equity", F), ("bank", F),
                 ("other_financial", F), ("pension_fund", F), ("other_corporation", F),
                 ("individual", F), ("foreigner", F), ("other_foreigner", F), ("total", F)),
        rename={"date": "datetime", "sec_code": "isin", "종목코드": "ticker",
                "금융투자": "financial_investment", "보험": "insurance", "투신": "investment_trust",
                "사모": "private_equity", "은행": "bank", "기타금융": "other_financial",
                "연기금": "pension_fund", "기타법인": "other_corporation", "개인": "individual",
                "외국인": "foreigner", "기타외국인": "other_foreigner", "전체": "total"},
        keys=("datetime", "isin"),
    ),
    "파생전종목시세": CrawledSchema(
        columns=(("datetime", DT), ("isin", S), ("ticker", S), ("korean_name", S),
                 ("close", F), ("change_type", S), ("change", F), ("open", F), ("high", F),
                 ("low", F), ("spot_price", F), ("settlement_price", F), ("volume", F),
                 ("trading_value", F), ("open_interest", F), ("security_group_id", S)),
        rename={"date": "datetime", "sec_code": "isin", "종목코드": "ticker", "종목명": "korean_name",
                "종가": "close", "등락구분": "change_type", "대비": "change", "시가": "open",
                "고가": "high", "저가": "low", "현물가": "spot_price", "정산가": "settlement_price",
                "거래량": "volume", "거래대금": "trading_value", "미결제약정": "open_interest",
                "증권그룹ID": "security_group_id"},
        keys=("datetime", "isin"),
    ),
    "전종목등락률": CrawledSchema(
        columns=(("datetime", pl.Datetime("ns", "Asia/Seoul")), ("isin", S), ("ticker", S),
                 ("korean_name", S), ("base_price", F), ("end_close", F), ("change", F),
                 ("change_rate", F), ("volume", F), ("trading_value", F), ("market", S),
                 ("adj_ratio", F), ("cum_adj_ratio", F)),
        rename={"date": "datetime", "sec_code": "isin", "종목코드": "ticker", "종목명": "korean_name",
                "시작일 기준가": "base_price", "종료일 종가": "end_close", "대비": "change",
                "등락률": "change_rate", "거래량": "volume", "거래대금": "trading_value"},
        keys=("datetime", "isin"),
        compression="lz4",
    ),
}


def _datetime_expression(df: pl.DataFrame, name: str, target: pl.Datetime) -> pl.Expr:
    source = df.schema[name]
    expr = pl.col(name)
    if source == pl.Date:
        expr = expr.cast(pl.Datetime(target.time_unit))
    elif isinstance(source, pl.Datetime):
        expr = expr.dt.cast_time_unit(target.time_unit)
    else:
        expr = expr.cast(pl.String).str.to_datetime(strict=False).dt.cast_time_unit(target.time_unit)

    source_zone = source.time_zone if isinstance(source, pl.Datetime) else None
    if target.time_zone:
        expr = expr.dt.convert_time_zone(target.time_zone) if source_zone else expr.dt.replace_time_zone(target.time_zone)
    elif source_zone:
        expr = expr.dt.convert_time_zone("Asia/Seoul").dt.replace_time_zone(None)
    return expr.alias(name)


def normalize_crawled_frame(item: str, df: pl.DataFrame) -> pl.DataFrame:
    """Return one raw or already-normalized frame in the January schema."""
    schema = SCHEMAS[item]
    rename = {
        source: target
        for source, target in schema.rename.items()
        if source in df.columns and target not in df.columns
    }
    if rename:
        df = df.rename(rename)

    additions = [pl.lit(None, dtype=dtype).alias(name) for name, dtype in schema.columns if name not in df.columns]
    if additions:
        df = df.with_columns(additions)

    expressions: list[pl.Expr] = []
    for name, dtype in schema.columns:
        if isinstance(dtype, pl.Datetime):
            expressions.append(_datetime_expression(df, name, dtype))
        else:
            expressions.append(pl.col(name).cast(dtype, strict=False).alias(name))
    return df.select(expressions)


def _merge_month(item: str, frames: list[pl.DataFrame], output: Path) -> int:
    schema = SCHEMAS[item]
    if output.exists():
        frames.insert(0, normalize_crawled_frame(item, pl.read_parquet(output)))
    combined = pl.concat(frames, how="vertical_relaxed")
    combined = combined.unique(subset=list(schema.keys), keep="last", maintain_order=True)
    combined = combined.sort(list(schema.keys))
    output.parent.mkdir(parents=True, exist_ok=True)
    with parquet_update_lock(output):
        atomic_write_parquet(combined, output, compression=schema.compression)
    return len(combined)


def _months_from_sources(source_root: Path) -> list[str]:
    months = {
        path.name[:7]
        for path in source_root.rglob("????-??-??.parquet")
        if len(path.name) >= 10
    }
    return sorted(months)


def convert_archive_to_backtesting(
    data_root: str | Path | None = None,
    *,
    months: Iterable[str] | None = None,
    items: Iterable[str] | None = None,
) -> dict:
    """Merge archived daily files into canonical monthly backtesting files."""
    root_value = data_root or os.environ.get("DATA_DB_PATH")
    if not root_value:
        raise ValueError("DATA_DB_PATH is required")
    root = Path(root_value)
    archive = root / "MFT_archive" / "KR" / "krx_crawled"
    target = root / "MFT_backtesting" / "KR" / "krx_crawled"
    selected_items = list(items or (name for name in SCHEMAS if name != "전종목등락률"))
    selected_months = list(months or _months_from_sources(archive))
    results: list[dict] = []

    for item in selected_items:
        if item == "전종목등락률":
            continue  # recalculated across all months by daily_adj_ratio.full_recalculate
        source_dir = archive / item
        if item == "지수구성종목":
            leaves = sorted(path for path in source_dir.iterdir() if path.is_dir()) if source_dir.exists() else []
            for leaf in leaves:
                for month in selected_months:
                    paths = sorted(leaf.glob(f"{month}-??.parquet"))
                    if not paths:
                        continue
                    frames = [normalize_crawled_frame(item, pl.read_parquet(path)) for path in paths]
                    output = target / item / leaf.name / f"{month}.parquet"
                    rows = _merge_month(item, frames, output)
                    results.append({"item": item, "stream": leaf.name, "month": month, "rows": rows, "path": str(output)})
        else:
            for month in selected_months:
                paths = sorted(source_dir.glob(f"{month}-??.parquet"))
                if not paths:
                    continue
                frames = [normalize_crawled_frame(item, pl.read_parquet(path)) for path in paths]
                output = target / item / f"{month}.parquet"
                rows = _merge_month(item, frames, output)
                results.append({"item": item, "month": month, "rows": rows, "path": str(output)})
    return {"status": "success", "files": results, "file_count": len(results)}
