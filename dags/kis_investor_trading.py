# -*- coding: utf-8 -*-
"""KIS API 투자자별 매매동향 실시간 수집 DAG (1분 주기)

구조:
1) 장중(09:00~15:30) 1분마다 API 호출하여 스냅샷 수집
2) 2026-01 기준 11개 시장(현물/선물/옵션/미니/코스닥150) 순차 수집
3) polars DataFrame으로 변환 후 일별 Parquet 파일에 누적 저장
4) 저장 경로: {DATA_DB}/MFT_archive/KR/kis_api/investor_trading_time_{market}/{YYYY-MM-DD}.parquet

수집 대상:
- 시장별 투자자매매동향 (시간대별) - TR_ID: FHPTJ04030000
- 시장: KOSPI, KOSDAQ, KOSPI200 선물/콜/풋, 미니 선물/콜/풋,
  KOSDAQ150 선물/콜/풋
- 투자자 구분: 외국인, 개인, 기관, 증권, 투신, PEF, 은행, 보험, 연기금, 펀드, 기타기관, 기타법인
- 지표: 매도량, 매수량, 순매수량, 매도금액, 매수금액, 순매수금액 (각 투자자별)

데이터 특성:
- API는 장 시작부터 현재까지 누적 스냅샷 반환 (1행)
- 1분봉 생성은 후처리에서 diff 계산 필요
"""
from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path
from typing import List, Dict

import pendulum
import polars as pl
import yaml
from airflow.exceptions import AirflowSkipException
from airflow import DAG
from airflow.decorators import task

# 프로젝트 경로 설정
DAGS_DIR = Path(__file__).parent
PROJECT_ROOT = DAGS_DIR.parent
PACKAGES_DIR = PROJECT_ROOT / "packages"
if str(PACKAGES_DIR) not in sys.path:
    sys.path.insert(0, str(PACKAGES_DIR))

# .env 파일 로드
def _load_env():
    env_path = PROJECT_ROOT / ".env"
    if env_path.exists():
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    value = value.strip('"').strip("'")
                    if key not in os.environ:
                        os.environ[key] = value

_load_env()

sys.path.insert(0, str(Path(__file__).parent))

from common import (
    DEFAULT_ARGS,
    is_market_hours,
    is_trading_day,
    resolve_run_timestamp,
)
from common import get_data_db_path
from mft_common.storage import atomic_write_parquet, parquet_update_lock


def save_to_final(df, item_name: str, date_str: str) -> str:
    """Save to MFT_archive/KR/kis_api/{item}/{date}.parquet with append."""
    import polars as pl
    if df is None or len(df) == 0:
        return f"skip: no data for {date_str}"
    base = get_data_db_path() / "MFT_archive" / "KR" / "kis_api" / item_name
    file_path = base / f"{date_str}.parquet"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with parquet_update_lock(file_path):
        if file_path.exists():
            existing = pl.read_parquet(file_path)
            df = pl.concat([existing, df], how="diagonal_relaxed")
            if "ts" in df.columns:
                df = df.unique(subset=["ts"], keep="last")
        atomic_write_parquet(df, file_path)
    return f"saved: {file_path} ({len(df)} rows)"


# ---------------------------------------------------------------------------
# 수집 대상 시장 설정
# ---------------------------------------------------------------------------
MARKETS = [
    {"name": "kospi", "code": "KSP", "index_code": "0001", "desc": "코스피"},
    {"name": "kosdaq", "code": "KSQ", "index_code": "1001", "desc": "코스닥"},
    {"name": "futures", "code": "K2I", "index_code": "F001", "desc": "코스피200 선물"},
    {"name": "call_option", "code": "K2I", "index_code": "OC01", "desc": "코스피200 콜옵션"},
    {"name": "put_option", "code": "K2I", "index_code": "OP01", "desc": "코스피200 풋옵션"},
    {"name": "mini_futures", "code": "MKI", "index_code": "F004", "desc": "미니코스피200 선물"},
    {"name": "mini_call_option", "code": "MKI", "index_code": "OC02", "desc": "미니코스피200 콜옵션"},
    {"name": "mini_put_option", "code": "MKI", "index_code": "OP02", "desc": "미니코스피200 풋옵션"},
    {"name": "kosdaq150_futures", "code": "KQI", "index_code": "F002", "desc": "코스닥150 선물"},
    {"name": "kosdaq150_call_option", "code": "KQI", "index_code": "OC03", "desc": "코스닥150 콜옵션"},
    {"name": "kosdaq150_put_option", "code": "KQI", "index_code": "OP03", "desc": "코스닥150 풋옵션"},
]

TR_ID = "FHPTJ04030000"
API_DELAY_SEC = 0.5  # API 호출 간격 (rate limit 방지)


# ---------------------------------------------------------------------------
# 설정 로드
# ---------------------------------------------------------------------------
def load_config() -> dict:
    """환경변수에서 직접 설정 로드"""
    return {
        "prod_api": {
            "app_key": os.environ.get("PROD_KIS_APP_KEY", ""),
            "app_secret": os.environ.get("PROD_KIS_APP_SECRET", ""),
            "base_url": os.environ.get("PROD_BASE_URL", "https://openapi.koreainvestment.com:9443")
        },
        "kis_api_endpoints": {
            TR_ID: "/uapi/domestic-stock/v1/quotations/inquire-investor-time-by-market",
        }
    }


# ---------------------------------------------------------------------------
# 데이터 수집 함수
# ---------------------------------------------------------------------------
async def fetch_single_market(api_client, market: dict, now: pendulum.DateTime) -> pl.DataFrame:
    """단일 시장 데이터 수집"""
    from mft_kis.feature.investor_trading_time import InvestorBuyTimeFetcher

    fetcher = InvestorBuyTimeFetcher(
        api_client,
        tr_id=TR_ID,
        fid_input_iscd=market["code"],
        fid_input_iscd_2=market["index_code"],
    )

    pd_df = await fetcher.fetch_data()

    if pd_df is None or pd_df.empty:
        return pl.DataFrame()

    # pandas -> polars 변환
    df = pl.from_pandas(pd_df)

    # 메타 컬럼 추가
    df = df.with_columns([
        pl.lit(now.to_datetime_string()).alias("ts"),
        pl.lit(now.strftime("%Y-%m-%d")).alias("trading_date"),
        pl.lit(market["name"]).alias("market"),
        pl.lit("kis_api").alias("data_source"),
    ])

    return df


async def fetch_all_markets(config: dict) -> Dict[str, pl.DataFrame]:
    """모든 시장 데이터 수집"""
    from mft_kis.api.rest_api import RestApi

    api_client = RestApi(config, context='data')
    await api_client.is_connected()

    now = pendulum.now("Asia/Seoul")
    results = {}

    try:
        for market in MARKETS:
            print(f"  Fetching {market['desc']} ({market['code']})...")
            df = await fetch_single_market(api_client, market, now)

            if not df.is_empty():
                results[market["name"]] = df
                print(f"    -> {len(df)} rows collected")
            else:
                print(f"    -> No data")

            # API rate limit 방지
            await asyncio.sleep(API_DELAY_SEC)

    finally:
        await api_client.close()

    return results


def sync_fetch_all_markets(config: dict) -> Dict[str, pl.DataFrame]:
    """동기 래퍼"""
    return asyncio.run(fetch_all_markets(config))


# ---------------------------------------------------------------------------
# DAG 정의
# ---------------------------------------------------------------------------
@task.short_circuit
def check_market_hours(logical_date=None, data_interval_end=None, dag_run=None) -> bool:
    """장중 시간 체크 - 장외 시간이면 스킵"""
    ts = resolve_run_timestamp(
        logical_date=logical_date,
        data_interval_end=data_interval_end,
        dag_run=dag_run,
        now=pendulum.now("Asia/Seoul"),
    )

    if not is_trading_day(ts):
        print(f"Skip: {ts.date()} is not a trading day")
        return False

    if not is_market_hours(ts):
        print(f"Skip: {ts} is not market hours (09:00~15:45)")
        return False

    print(f"Market hours confirmed: {ts}")
    return True


@task
def collect_investor_trading_time(
    logical_date=None,
    data_interval_end=None,
    dag_run=None,
    run_id=None,
) -> str:
    """2026-01 기준 11개 시장의 투자자별 매매동향 수집 및 저장."""
    config = load_config()
    ts = resolve_run_timestamp(
        logical_date=logical_date,
        data_interval_end=data_interval_end,
        dag_run=dag_run,
        now=pendulum.now("Asia/Seoul"),
    )
    date_str = ts.strftime("%Y-%m-%d")

    print(f"Collecting investor trading time for {date_str}")

    # 모든 시장 데이터 수집
    market_data = sync_fetch_all_markets(config)

    if not market_data:
        raise AirflowSkipException("No data collected from any market")

    # 시장별 저장
    results = []
    for market_name, df in market_data.items():
        feature_name = f"investor_trading_time_{market_name}"
        result = save_to_final(df, feature_name, date_str)
        results.append(f"{market_name}: {result}")
        print(f"  {market_name}: {result}")

    return " | ".join(results)


with DAG(
    dag_id="kis_investor_trading",
    schedule="* 9-15 * * 1-5",  # 평일 09:00~15:59 매분 실행
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["kis", "realtime", "investor"],
    doc_md=__doc__,
    default_args=DEFAULT_ARGS,
) as dag:
    # 1. 장중 시간 체크
    market_check = check_market_hours()

    # 2. 데이터 수집 (2026-01 기준 11개 시장)
    collect_task = collect_investor_trading_time()

    market_check >> collect_task
