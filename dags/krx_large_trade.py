"""KRX 대량매매전일 크롤링 DAG

대량매매전일 데이터는 "전일" 데이터만 조회 가능하므로 별도 DAG로 분리.
- 화~토 실행 (전일 영업일 데이터 수집)
- catchup=False (과거 데이터 조회 불가)
- API가 항상 "당일 기준 전일" 데이터를 제공

스케줄:
- 화요일 실행 → 월요일 데이터
- 수요일 실행 → 화요일 데이터
- ...
- 토요일 실행 → 금요일 데이터
"""
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from airflow.exceptions import AirflowSkipException
from airflow.decorators import dag, task
from pandas import Timestamp

from common import (
    DEFAULT_ARGS,
    is_trading_day,
    resolve_target_date,
    safe_crawl,
    save_to_final,
    setup_proxy,
)
from mft_krx.crawlers.core import 대량매매전일_12011


def get_previous_trading_day(date: Timestamp) -> Timestamp:
    """전 영업일 계산 (주말/공휴일 제외)"""
    prev_day = date - timedelta(days=1)
    # 최대 7일까지 거슬러 올라감 (연휴 대비)
    for _ in range(7):
        if is_trading_day(prev_day):
            return prev_day
        prev_day -= timedelta(days=1)
    return prev_day


@task
def crawl_large_trade(
    logical_date=None,
    data_interval_end=None,
    dag_run=None,
    run_id=None,
) -> str:
    """대량매매전일 크롤링

    API가 "당일 기준 전일" 데이터를 제공하므로,
    실제 저장 날짜는 전 영업일로 계산
    """
    today = resolve_target_date(
        logical_date=logical_date,
        data_interval_end=data_interval_end,
        dag_run=dag_run,
    )

    # 전 영업일 계산 (실제 데이터 날짜)
    data_date = get_previous_trading_day(today)

    print(f"실행일: {today.date()}, 데이터 날짜: {data_date.date()}")

    setup_proxy()

    # 크롤링 (날짜는 data_date로 저장)
    df = safe_crawl(대량매매전일_12011, data_date)

    if len(df) == 0:
        raise AirflowSkipException(f"No data for {data_date.date()}")

    # 최종 저장
    result = save_to_final(df, "대량매매전일", data_date)
    from mft_krx.convert import convert_archive_to_backtesting

    converted = convert_archive_to_backtesting(
        months=[data_date.strftime("%Y-%m")],
        items=["대량매매전일"],
    )

    print(f"Completed: 대량매매전일 - {result}; converted={converted['file_count']}")
    return result


@dag(
    dag_id="krx_large_trade",
    schedule="0 9 * * 2-6",  # 화~토 09시 (전일 영업일 데이터 수집)
    start_date=datetime(2024, 1, 1),
    catchup=False,  # 과거 데이터 조회 불가
    max_active_runs=1,
    tags=["krx", "daily", "large_trade"],
    doc_md=__doc__,
    default_args=DEFAULT_ARGS,
)
def krx_large_trade():
    """KRX 대량매매전일 크롤링 DAG."""
    crawl_large_trade()


# Instantiate the DAG
krx_large_trade()
