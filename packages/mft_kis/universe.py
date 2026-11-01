"""
Universe extraction from KRX crawled data.

Extracts stock/ETF/ELW/futures/options universes from daily KRX data,
mapping each instrument to its KIS API market code and ticker.

Note: Derivative tickers from 파생전종목시세 use KOSCOM format (e.g. A0166000)
which differs from KIS API format (e.g. A01606). We download KIS master files
to build the ISIN → KIS short code mapping.
"""

import io
import logging
import os
import zipfile
from datetime import date
from pathlib import Path
from typing import Optional

import polars as pl
import requests

from mft_common.storage import atomic_write_parquet

logger = logging.getLogger(__name__)

# KIS master file URLs for derivative code mapping
# Pipe-delimited: field[1]=short_code, field[2]=ISIN
_KIS_PIPE_MASTER_URLS = {
    "index_fo": "https://new.real.download.dws.co.kr/common/master/fo_idx_code_mts.mst.zip",
    "stock_fo": "https://new.real.download.dws.co.kr/common/master/fo_stk_code_mts.mst.zip",
}
# Fixed-width: [2:11]=short_code, [11:23]=ISIN
_KIS_FIXED_MASTER_URLS = {
    "commodity_fo": "https://new.real.download.dws.co.kr/common/master/fo_com_code.mst.zip",
}

# Cache: ISIN -> KIS short code
_isin_to_kis_code: dict[str, str] = {}
_master_download_attempted = False
_MIN_DERIVATIVE_MASTER_CODES = 1000
_historical_derivative_cache_root: Optional[Path] = None
_historical_derivative_codes: dict[str, str] = {}


def _load_derivative_master_cache(*, current_only: bool = False) -> dict[str, str]:
    cache_dir = _get_data_root() / "logs" / "kis_master"
    files = sorted(cache_dir.glob("derivative_codes_*.parquet"), reverse=True)
    if current_only:
        today_name = f"derivative_codes_{date.today().isoformat()}.parquet"
        files = [path for path in files if path.name == today_name]
    for path in files:
        try:
            frame = pl.read_parquet(path)
            if len(frame) >= _MIN_DERIVATIVE_MASTER_CODES:
                return dict(zip(frame["isin"].to_list(), frame["ticker"].to_list()))
        except Exception as exc:
            logger.warning("Ignoring unreadable derivative master cache %s: %s", path, exc)
    return {}


def _save_derivative_master_cache(mapping: dict[str, str]) -> None:
    cache_dir = _get_data_root() / "logs" / "kis_master"
    cache_dir.mkdir(parents=True, exist_ok=True)
    path = cache_dir / f"derivative_codes_{date.today().isoformat()}.parquet"
    frame = pl.DataFrame(
        {"isin": list(mapping), "ticker": list(mapping.values())}
    ).sort("isin")
    atomic_write_parquet(frame, path)


def _lookup_historical_derivative_cache(isin: str) -> Optional[str]:
    """Return an expired contract code retained in an older daily master."""
    global _historical_derivative_cache_root, _historical_derivative_codes
    cache_dir = _get_data_root() / "logs" / "kis_master"
    if _historical_derivative_cache_root != cache_dir:
        merged: dict[str, str] = {}
        # Newest cache wins. Load each file only once per process instead of
        # scanning every Parquet again for every expired contract.
        for path in sorted(cache_dir.glob("derivative_codes_*.parquet"), reverse=True):
            try:
                frame = pl.read_parquet(path, columns=["isin", "ticker"])
                for cached_isin, ticker in frame.iter_rows():
                    merged.setdefault(str(cached_isin), str(ticker))
            except Exception as exc:
                logger.warning("Ignoring unreadable derivative master cache %s: %s", path, exc)
        _historical_derivative_cache_root = cache_dir
        _historical_derivative_codes = merged
        logger.info("Loaded %s historical derivative code mappings", len(merged))
    return _historical_derivative_codes.get(isin)

# KIS instrument-master URLs. Each is a fixed-width text dump in cp949
# encoding. We extract ISIN→ticker pairs; each master contributes a
# different slice of the KRX universe that 전종목시세/ETF전종목시세 miss:
#
#   elw_code.mst    — KRA5* foreign-underlying ELW (market_div_code = "W")
#   kospi_code.mst  — KRG5* Korean-underlying ELW + KRG6* ETN + a few
#                     foreign stocks (market_div_code = "J")
#
# Together they fill the universe gap observed against server pcap data.
_KIS_ELW_MASTER_URL = "https://new.real.download.dws.co.kr/common/master/elw_code.mst.zip"
_KIS_KOSPI_MASTER_URL = "https://new.real.download.dws.co.kr/common/master/kospi_code.mst.zip"

# Per-process cache of {isin: ticker} for each master file.
_elw_isin_to_ticker: Optional[dict] = None
_kospi_isin_to_ticker: Optional[dict] = None


def _download_kis_master_codes() -> dict[str, str]:
    """Download KIS master files and build ISIN → KIS short code mapping.

    Returns dict mapping ISIN (e.g. KR4A01660005) to KIS code (e.g. A01606).
    """
    global _isin_to_kis_code, _master_download_attempted
    if _isin_to_kis_code:
        return _isin_to_kis_code

    current_cache = _load_derivative_master_cache(current_only=True)
    if current_cache:
        _isin_to_kis_code = current_cache
        logger.info("Loaded %s derivative codes from today's local cache", len(current_cache))
        return _isin_to_kis_code
    if _master_download_attempted:
        raise RuntimeError("KIS derivative master mapping is unavailable")
    _master_download_attempted = True

    fallback_cache = _load_derivative_master_cache()
    mapping = {}

    # Pipe-delimited masters (index + stock futures/options)
    for name, url in _KIS_PIPE_MASTER_URLS.items():
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code != 200:
                logger.warning(f"Failed to download {name} master: HTTP {resp.status_code}")
                continue
            z = zipfile.ZipFile(io.BytesIO(resp.content))
            content = z.read(z.namelist()[0])
            lines = content.decode("cp949", errors="replace").strip().split("\n")
            for line in lines:
                fields = line.split("|")
                if len(fields) >= 3:
                    kis_code = fields[1].strip()
                    isin = fields[2].strip()
                    if isin and kis_code:
                        mapping[isin] = kis_code
            logger.info(f"Loaded {len(lines)} codes from {name} master")
        except Exception as e:
            logger.warning(f"Error downloading {name} master: {e}")

    # Fixed-width masters (commodity: currency/bond futures)
    for name, url in _KIS_FIXED_MASTER_URLS.items():
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code != 200:
                logger.warning(f"Failed to download {name} master: HTTP {resp.status_code}")
                continue
            z = zipfile.ZipFile(io.BytesIO(resp.content))
            content = z.read(z.namelist()[0])
            lines = content.decode("cp949", errors="replace").strip().split("\n")
            for line in lines:
                if len(line) < 23:
                    continue
                kis_code = line[2:11].strip()
                isin = line[11:23].strip()
                if isin and kis_code:
                    mapping[isin] = kis_code
            logger.info(f"Loaded {len(lines)} codes from {name} master")
        except Exception as e:
            logger.warning(f"Error downloading {name} master: {e}")

    if len(mapping) < _MIN_DERIVATIVE_MASTER_CODES:
        if fallback_cache:
            _isin_to_kis_code = fallback_cache
            logger.warning(
                "Today's KIS derivative master is incomplete (%s codes); "
                "using the latest historical cache (%s codes)",
                len(mapping),
                len(fallback_cache),
            )
            return _isin_to_kis_code
        raise RuntimeError(
            f"Incomplete KIS derivative master mapping: {len(mapping)} codes"
        )
    _save_derivative_master_cache(mapping)
    _isin_to_kis_code = mapping
    logger.info(f"Total ISIN->KIS code mappings: {len(mapping)}")
    return mapping


def _derive_historical_kis_ticker(isin: str, raw_ticker: str) -> Optional[str]:
    """Rebuild KIS codes whose expired contracts disappeared from masters.

    Both rules are checked against every matching contract in the current
    KIS master (576 weekly options and 1,830 futures on 2026-08-05), and were
    also verified with historical minute requests for expired contracts.
    """
    raw_ticker = str(raw_ticker or "").strip()
    if len(raw_ticker) != 8:
        return None

    if isin.startswith(("KR4B09", "KR4C09", "KR4BAF", "KR4CAF")):
        return f"{raw_ticker[:5]}W{raw_ticker[5:]}"

    # Monthly index options use a nine-character KIS code. For example, the
    # persisted KIS masters map KR4B01687007/B0168700 -> B01608700 and
    # KR4B01686272/B0168627 -> B01608627.
    if isin[:6] in _INDEX_OPTION_PREFIXES:
        return f"{raw_ticker[:4]}0{raw_ticker[4:]}"

    if isin.startswith("KR4A"):
        month_codes = {
            "1": "01", "2": "02", "3": "03", "4": "04",
            "5": "05", "6": "06", "7": "07", "8": "08",
            "9": "09", "A": "10", "B": "11", "C": "12",
        }
        month = month_codes.get(raw_ticker[4])
        if month:
            return f"{raw_ticker[:4]}{month}"
    return None


def _get_kis_ticker(isin: str, fallback_ticker: str) -> str:
    """Get a verified or deterministically reconstructed KIS ticker."""
    mapping = _download_kis_master_codes()
    ticker = mapping.get(isin)
    if ticker:
        return ticker

    ticker = _lookup_historical_derivative_cache(isin)
    if ticker:
        return ticker

    ticker = _derive_historical_kis_ticker(isin, fallback_ticker)
    if ticker:
        logger.debug(
            "Using reconstructed historical KIS code %s for expired ISIN %s",
            ticker,
            isin,
        )
        return ticker
    raise RuntimeError(f"No KIS derivative code mapping for ISIN {isin}")


def _parse_kis_isin_master(url: str, label: str) -> dict:
    """Download a KIS master.zip and extract {isin: ticker}. Robust to
    fixed-width layout variations: locate the ISIN via regex, take the
    last whitespace-delimited token immediately before it as the ticker."""
    import re as _re
    out: dict = {}
    try:
        resp = requests.get(url, timeout=15)
        if resp.status_code != 200:
            logger.warning(f"{label} master download failed: HTTP {resp.status_code}")
            return out
        z = zipfile.ZipFile(io.BytesIO(resp.content))
        content = z.read(z.namelist()[0]).decode("cp949", errors="replace")
        isin_re = _re.compile(r"(?:KR|HK)[A-Z0-9]{10}")
        for line in content.splitlines():
            m = isin_re.search(line)
            if not m:
                continue
            isin = m.group(0)
            prefix = line[: m.start()].strip()
            if not prefix:
                continue
            out[isin] = prefix.split()[-1]
        logger.info(f"Loaded {len(out)} ISIN->ticker pairs from {label}")
    except Exception as e:
        logger.warning(f"{label} master parse failed: {e}")
    return out


def _load_elw_master() -> dict:
    """Cached parse of elw_code.mst — KRA5* foreign-underlying ELW."""
    global _elw_isin_to_ticker
    if _elw_isin_to_ticker is None:
        _elw_isin_to_ticker = _parse_kis_isin_master(
            _KIS_ELW_MASTER_URL, "elw_code.mst"
        )
    return _elw_isin_to_ticker


def _load_kospi_master() -> dict:
    """Cached parse of kospi_code.mst — covers KRG5* Korean ELW, KRG6*
    ETN, and a few foreign stocks listed on KOSPI that 전종목시세 omits."""
    global _kospi_isin_to_ticker
    if _kospi_isin_to_ticker is None:
        _kospi_isin_to_ticker = _parse_kis_isin_master(
            _KIS_KOSPI_MASTER_URL, "kospi_code.mst"
        )
    return _kospi_isin_to_ticker


def _elw_market_code(isin: str) -> str:
    """KIS market_div_code for an ELW ISIN. KRG5*-class ELW use 'J' (same
    venue as equities); KRA5*-class foreign-underlying ELW use 'W'. Verified
    empirically against the FHKST03010230 endpoint on 2026-04-22."""
    return "W" if isin.startswith("KRA") else "J"


def _get_data_root() -> Path:
    """Get data root from environment."""
    path = os.environ.get("DATA_DB_PATH")
    if not path:
        raise ValueError("DATA_DB_PATH environment variable not set")
    return Path(path)


def _has_positive_volume(value) -> bool:
    try:
        return float(value or 0) > 0
    except (TypeError, ValueError):
        return False


def _has_trade_evidence(row: dict) -> bool:
    """Require positive volume and a real daily OHLC value when available."""
    volume = row.get("volume", row.get("거래량", 0))
    if not _has_positive_volume(volume):
        return False
    price_keys = (
        "close_price", "open_price", "high_price", "low_price",
        "종가", "시가", "고가", "저가",
    )
    values = [row[key] for key in price_keys if key in row]
    if not values:
        return True
    for value in values:
        try:
            float(str(value).replace(",", ""))
            return True
        except (TypeError, ValueError):
            continue
    return False


def _load_krx_parquet(item_name: str, date: str, data_root: Optional[Path] = None) -> pl.DataFrame:
    """Load KRX crawled parquet for a given date.

    Prefer the immutable daily archive, then the converted monthly file. The
    archive is authoritative during a historical backfill because monthly
    conversion is intentionally deferred until all crawl dates complete.
    """
    data_root = data_root or _get_data_root()
    base = data_root / "MFT_backtesting" / "KR" / "krx_crawled" / item_name

    archive_path = (
        data_root
        / "MFT_archive"
        / "KR"
        / "krx_crawled"
        / item_name
        / f"{date}.parquet"
    )
    if archive_path.exists():
        return pl.read_parquet(archive_path)

    # Legacy converted daily format (kept for backwards compatibility).
    daily_path = base / f"{date}.parquet"
    if daily_path.exists():
        return pl.read_parquet(daily_path)

    # Try monthly file format with date filter
    year_month = date[:7]
    monthly_path = base / f"{year_month}.parquet"
    if monthly_path.exists():
        from datetime import date as date_type
        target_date = date_type.fromisoformat(date)
        schema = pl.read_parquet_schema(monthly_path)
        for dt_col in ["date", "datetime"]:
            if dt_col in schema:
                return (
                    pl.scan_parquet(monthly_path)
                    .filter(pl.col(dt_col).dt.date() == target_date)
                    .collect()
                )
        return pl.read_parquet(monthly_path)

    raise FileNotFoundError(f"No data for {item_name} on {date} in {base}")


def load_stock_universe(date: str, data_root: Optional[Path] = None) -> list[dict]:
    """Extract stock + ETF + ELW universe from KRX crawled data.

    Args:
        date: Date string "YYYY-MM-DD"
        data_root: Optional data root path override

    Returns:
        List of dicts with keys: ticker, isin, kis_market, name, asset_type
    """
    universe = []

    # 1. 전종목시세 -> stocks + ELW
    # Column names vary: server raw uses sec_code/종목코드/종목명,
    # but converted data uses isin/ticker/korean_name
    # 1. 전종목시세 -> stocks. Use "UN" so KIS returns KRX+NXT unified
    # volume; matches the multi-venue volume seen in koscom pcap data.
    # NXT-ineligible stocks get the same result as "J" (NXT contributes 0).
    try:
        stock_df = _load_krx_parquet("전종목시세", date, data_root)
        for row in stock_df.iter_rows(named=True):
            isin = row.get("isin", row.get("sec_code", ""))
            ticker = row.get("ticker", row.get("종목코드", ""))
            name = row.get("korean_name", row.get("종목명", ""))
            asset_type = "elw" if isin.startswith("KRA5") else "stock"
            volume = row.get("volume", row.get("거래량", 0))
            universe.append({
                "ticker": ticker,
                "isin": isin,
                # KRX daily snapshots define this universe.  Query KRX (J)
                # directly so ETFs and KRX-only equities are not silently
                # omitted by the unified/NXT market endpoint.
                "kis_market": _elw_market_code(isin) if asset_type == "elw" else "J",
                "name": name,
                "asset_type": asset_type,
                "expects_data": _has_positive_volume(volume),
            })
    except FileNotFoundError:
        pass

    # 2. ETF전종목시세 -> ETFs (also use "UN" for any NXT-eligible ETFs).
    try:
        etf_df = _load_krx_parquet("ETF전종목시세", date, data_root)
        for row in etf_df.iter_rows(named=True):
            volume = row.get("volume", row.get("거래량", 0))
            universe.append({
                "ticker": row.get("ticker", row.get("종목코드", "")),
                "isin": row.get("isin", row.get("sec_code", "")),
                "kis_market": "J",
                "name": row.get("korean_name", row.get("종목명", "")),
                "asset_type": "etf",
                "expects_data": _has_positive_volume(volume),
            })
    except FileNotFoundError:
        pass

    # 3. KIS-master fill-ins for instrument types absent from KRX 전종목시세:
    #    - elw_code.mst: KRA5* foreign-underlying ELW (kis_market = "W")
    #    - kospi_code.mst: KRG5* Korean ELW, KRG6* ETN, plus a handful of
    #      KR5*/KRA0* foreign equities on KOSPI (kis_market = "J")
    # De-dup against ISINs already present from KRX sources.
    seen_isins = {entry["isin"] for entry in universe}

    for isin, ticker in _load_elw_master().items():
        if isin in seen_isins:
            continue
        universe.append({
            "ticker": ticker, "isin": isin,
            "kis_market": _elw_market_code(isin),
            "name": "", "asset_type": "elw",
            "expects_data": False,
        })
        seen_isins.add(isin)

    for isin, ticker in _load_kospi_master().items():
        if isin in seen_isins:
            continue
        # Heuristic: KRG5* and KRG6* are warrants/notes, not common stock.
        # Everything else from kospi_code.mst we don't already have is
        # treated as a foreign stock listed on KOSPI.
        if isin.startswith("KRG5"):
            asset_type = "elw"
        elif isin.startswith("KRG6"):
            asset_type = "etn"
        else:
            asset_type = "stock"
        universe.append({
            "ticker": ticker, "isin": isin,
            "kis_market": "J",
            "name": "", "asset_type": asset_type,
            "expects_data": False,
        })
        seen_isins.add(isin)

    return universe


# Actual ISIN prefix mapping (verified against 2026-04 파생전종목시세 data)
# Index futures: KOSPI200=KR4A01, mini-KOSPI=KR4A05, KOSDAQ150=KR4A06, KRX300=KR4A08
_INDEX_FUTURES_PREFIXES = {"KR4A01", "KR4A05", "KR4A06", "KR4A08"}
# Currency futures: USD=KR4A75, JPY=KR4A76, EUR=KR4A77, CNY=KR4A78
_CURRENCY_FUTURES_PREFIXES = {"KR4A75", "KR4A76", "KR4A77", "KR4A78"}
# Bond futures: 3Y=KR4A65, 5Y=KR4A66, 10Y=KR4A67
_BOND_FUTURES_PREFIXES = {"KR4A65", "KR4A66", "KR4A67"}
# Volatility futures: KR4A04. KR4A88 is gold and belongs to CF.
_VOLATILITY_FUTURES_PREFIXES = {"KR4A04"}
_COMMODITY_FUTURES_PREFIXES = {"KR4A88"}
# Spread futures (KR4D*) and gold (KR41GN, KR44GN) — excluded from collection
_SPREAD_PREFIXES = {"KR4D", "KR41G", "KR44G"}

# Index options (KOSPI200 call/put, weekly, KOSDAQ150)
_INDEX_OPTION_PREFIXES = {
    "KR4B01", "KR4C01",  # KOSPI200
    "KR4B05", "KR4C05",  # Mini KOSPI200
    "KR4B09", "KR4C09",  # Weekly KOSPI200
    "KR4B06", "KR4C06",  # KOSDAQ150
    "KR4BAF", "KR4CAF",  # Weekly Mini KOSPI200
}


def _classify_derivative(isin: str) -> tuple[str, str]:
    """Classify a derivative ISIN into (category, kis_market_code)."""
    p6 = isin[:6]

    # Index futures
    if p6 in _INDEX_FUTURES_PREFIXES:
        return ("index_futures", "F")
    # Volatility futures
    if p6 in _VOLATILITY_FUTURES_PREFIXES:
        return ("index_futures", "F")
    if p6 in _COMMODITY_FUTURES_PREFIXES:
        return ("index_futures", "CF")
    # Currency futures
    if p6 in _CURRENCY_FUTURES_PREFIXES:
        return ("currency_futures", "CF")
    # Bond futures
    if p6 in _BOND_FUTURES_PREFIXES:
        return ("bond_futures", "CF")

    # Index options (check 6-char prefix)
    if p6 in _INDEX_OPTION_PREFIXES:
        return ("index_options", "O")

    # Spreads and gold — skip
    p4 = isin[:4]
    if any(isin.startswith(sp) for sp in _SPREAD_PREFIXES):
        return ("unknown", "")

    # Remaining KR4A* = stock futures
    if p4 == "KR4A":
        # KRX sector-index futures use AA* short codes and the index-futures
        # API market. Individual-equity futures use JF.
        if str(isin[3:6]).startswith("AA"):
            return ("index_futures", "F")
        return ("stock_futures", "JF")

    # Remaining KR4B*/KR4C* = stock options
    if p4 in ("KR4B", "KR4C"):
        return ("stock_options", "JO")

    return ("unknown", "")


def load_derivative_universe(date: str, data_root: Optional[Path] = None) -> dict[str, list[dict]]:
    """Extract derivative universe from 파생전종목시세.

    Args:
        date: Date string "YYYY-MM-DD"
        data_root: Optional data root path override

    Returns:
        Dict mapping category to list of instruments.
    """
    deriv_df = _load_krx_parquet("파생전종목시세", date, data_root)

    result: dict[str, list[dict]] = {
        "index_futures": [], "stock_futures": [],
        "currency_futures": [], "bond_futures": [],
        "index_options": [], "stock_options": [],
    }
    seen_sessions: set[tuple[str, str]] = set()

    for row in deriv_df.iter_rows(named=True):
        isin = row.get("isin", row.get("sec_code", ""))
        raw_ticker = row.get("ticker", row.get("종목코드", ""))
        name = row.get("korean_name", row.get("종목명", ""))
        volume = row.get("volume", row.get("거래량", 0))

        category, kis_market = _classify_derivative(isin)
        if category == "unknown":
            continue

        # KRX emits distinct day/night rows for the same ISIN. They use
        # different KIS markets and both must be collected when traded.
        if "(야간)" in str(name):
            kis_market = "EU" if category in ("index_options", "stock_options") else "CM"

        if category in ("index_options", "stock_options", "stock_futures"):
            if not _has_trade_evidence(row):
                continue

        # KRX derivative snapshots may contain the same contract more than
        # once. Calling KIS repeatedly for that ISIN duplicates every minute
        # row and wastes API quota, so keep one accepted instrument per ISIN.
        session_key = (isin, kis_market)
        if not isin or session_key in seen_sessions:
            continue
        seen_sessions.add(session_key)

        # Map KOSCOM ticker only after the liquidity and duplicate filters.
        # Zero-volume or repeated day/night rows must not require a code.
        ticker = _get_kis_ticker(isin, raw_ticker)

        result[category].append({
            "ticker": ticker, "isin": isin,
            "kis_market": kis_market,
            "name": name,
            "expects_data": _has_trade_evidence(row),
        })

    return result
