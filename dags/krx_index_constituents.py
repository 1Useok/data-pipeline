"""KRX 지수구성종목 크롤링 DAG

- 거래일 체크 후 비거래일이면 전체 스킵 (KRX 요청 안 함)
- 163개 지수를 순차적으로 크롤링 (IP 차단 방지)
- 각 지수 조회 후 3초 sleep
- 지수별로 별도 폴더에 저장

저장 구조:
  krx_crawled/지수구성종목/{지수명}/YYYY-MM-DD.parquet

- 지수구성종목 (11006): 2004-01-02~
"""
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from airflow.exceptions import AirflowSkipException
from airflow.decorators import dag, task
from pandas import Timestamp

from common import (
    DEFAULT_ARGS,
    get_krx_crawled_path,
    is_trading_day,
    resolve_target_date,
    safe_crawl,
    setup_proxy,
)
from mft_common.storage import atomic_write_parquet
from mft_krx.crawlers.core import 지수구성종목_11006, 지수목록_조회

# 설정
TARGET_NAME = "지수구성종목"
START_DATE = "2004-01-02"
MIN_START_DATE = datetime(2004, 1, 2)
SLEEP_SECONDS = 3  # KRX 서버 부하 방지


def save_index_data(df, index_name: str, date: Timestamp) -> str:
    """지수별 폴더에 저장

    경로: krx_crawled/지수구성종목/{지수명}/YYYY-MM-DD.parquet
    """
    # 파일명에 사용할 수 없는 문자 처리
    safe_index_name = index_name.replace("/", "_").replace("\\", "_")

    date_str = date.strftime("%Y-%m-%d")
    file_path = get_krx_crawled_path() / TARGET_NAME / safe_index_name / f"{date_str}.parquet"
    file_path.parent.mkdir(parents=True, exist_ok=True)

    atomic_write_parquet(df, file_path)
    return str(file_path)


@task.short_circuit
def check_trading_day(logical_date=None, data_interval_end=None, dag_run=None) -> bool:
    """거래일 체크 - 비거래일이면 전체 DAG 스킵 (ShortCircuit)

    False 반환 시 모든 downstream task가 자동 스킵됨
    """
    date = resolve_target_date(
        logical_date=logical_date,
        data_interval_end=data_interval_end,
        dag_run=dag_run,
    )
    target_start = Timestamp(START_DATE)

    # 시작 날짜 이전 체크
    if date < target_start:
        print(f"Skip: {date.date()} < start_date {target_start.date()}")
        return False

    # 거래일 체크
    if not is_trading_day(date):
        print(f"Skip: {date.date()} is not a trading day")
        return False

    print(f"Trading day confirmed: {date.date()}")
    return True


def _resolve_duplicate_names(indices: list[dict]) -> list[dict]:
    """동일한 codeName을 가진 지수들에 시장 구분(KOSPI/KOSDAQ) prefix 추가

    예: 코스피 "화학"과 코스닥 "화학"이 있으면
        → "코스피 화학", "코스닥 화학"으로 구분
    """
    from collections import Counter

    # codeName별 출현 횟수 계산
    name_counts = Counter(idx.get("codeName", "") for idx in indices)

    # 중복되는 이름 목록
    duplicate_names = {name for name, count in name_counts.items() if count > 1}

    result = []
    for idx in indices:
        code_name = idx.get("codeName", idx.get("full_code"))
        market_name = idx.get("marketName", "")  # KOSPI, KOSDAQ, KRX 등

        # 중복되는 이름인 경우 시장 구분 prefix 추가
        if code_name in duplicate_names and market_name:
            # marketName을 한글로 변환 (KOSPI→코스피, KOSDAQ→코스닥)
            market_prefix_map = {
                "KOSPI": "코스피",
                "KOSDAQ": "코스닥",
                "KRX": "KRX",
            }
            market_prefix = market_prefix_map.get(market_name, market_name)
            resolved_name = f"{market_prefix} {code_name}"
        else:
            resolved_name = code_name

        result.append({
            "name": resolved_name,
            "market": idx.get("full_code", "1"),
            "ind_id": idx.get("short_code", ""),
        })

    return result


@task
def get_index_list(logical_date=None, data_interval_end=None, dag_run=None) -> list[dict]:
    """지수 목록 조회

    동일한 codeName을 가진 지수(예: 코스피 화학, 코스닥 화학)는
    시장 구분 prefix를 붙여서 구분합니다.
    """
    date = resolve_target_date(
        logical_date=logical_date,
        data_interval_end=data_interval_end,
        dag_run=dag_run,
    )

    setup_proxy()
    try:
        indices = 지수목록_조회()
        if not indices:
            raise RuntimeError("empty index list")

        # 중복 이름 해결
        return _resolve_duplicate_names(indices)
    except Exception as e:
        print(f"Failed to get index list: {e}")
        raise


@task
def extract_and_save(
    indices: list[dict],
    logical_date=None,
    data_interval_end=None,
    dag_run=None,
) -> dict:
    """모든 지수 순차 크롤링 및 지수별 저장"""
    if not indices:
        raise RuntimeError("index list is empty")

    date = resolve_target_date(
        logical_date=logical_date,
        data_interval_end=data_interval_end,
        dag_run=dag_run,
    )
    setup_proxy()

    saved_count = 0
    failed_count = 0
    skipped_count = 0

    for i, idx in enumerate(indices):
        try:
            df = safe_crawl(
                지수구성종목_11006,
                date=date,
                index_name=idx["name"],
                market=idx["market"],
                ind_id=idx["ind_id"],
            )

            if len(df) > 0:
                # 지수별 폴더에 저장
                file_path = save_index_data(df, idx["name"], date)
                print(f"[{i+1}/{len(indices)}] {idx['name']}: {len(df)} rows → {file_path}")
                saved_count += 1
            else:
                print(f"[{i+1}/{len(indices)}] {idx['name']}: no data")
                skipped_count += 1

        except Exception as e:
            print(f"[{i+1}/{len(indices)}] {idx['name']}: failed - {e}")
            failed_count += 1

        # KRX 서버 부하 방지
        time.sleep(SLEEP_SECONDS)

    if saved_count == 0:
        raise RuntimeError(f"no data saved for {date.date()}")
    if failed_count:
        raise RuntimeError(
            f"index constituents incomplete for {date.date()}: "
            f"saved={saved_count}, skipped={skipped_count}, failed={failed_count}"
        )

    from mft_krx.convert import convert_archive_to_backtesting

    converted = convert_archive_to_backtesting(
        months=[date.strftime("%Y-%m")],
        items=[TARGET_NAME],
    )

    return {
        "date": str(date.date()),
        "saved": saved_count,
        "skipped": skipped_count,
        "failed": failed_count,
        "converted": converted["file_count"],
    }


# =============================================================================
# DAG 정의
# =============================================================================


@dag(
    dag_id="krx_index_constituents",
    schedule="0 22 * * 1-5",  # 매일 22시 월~금
    start_date=MIN_START_DATE,
    catchup=False,
    max_active_runs=1,
    tags=["krx", "daily", "index"],
    doc_md=__doc__,
    default_args=DEFAULT_ARGS,
)
def krx_index_constituents():
    """KRX 지수구성종목 크롤링 DAG."""

    # 1. 거래일 체크 (비거래일이면 전체 스킵)
    trading_day_check = check_trading_day()

    # 2. 지수 목록 조회
    indices = get_index_list()

    # 3. 순차 크롤링 및 지수별 저장 (163개 × 3초 = ~8분)
    result = extract_and_save(indices)

    # 의존성 설정
    trading_day_check >> indices >> result


# Instantiate the DAG
krx_index_constituents()
