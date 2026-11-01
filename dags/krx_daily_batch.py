"""KRX 일일 배치 크롤링 DAG

KRX 크롤링 항목을 하나의 DAG로 통합.
- 거래일 체크 후 비거래일이면 전체 스킵 (KRX 요청 안 함)
- 거래일이면 각 항목 순차 실행 (IP 차단 방지)
- 필수 항목이 비면 실패·재시도하고 후속 KIS 파이프라인은 시작하지 않음

크롤링 항목 (순차 실행):
1. 지수시세추이 (11003): 1980-01-04~
2. 전종목등락률 (12002): 1995-05-02~
3. 업종별분포 (12024): 1995-05-02~
4. 업종분류현황 (12025): 2001-01-02~
5. ETF전종목시세 (13101): 2002-10-14~
6. ETF전종목등락률 (13102): 2002-10-14~
7. 투자자별거래대금 (12010 bulk): 2003-01-02~
8. 외국인보유량 (12023): 2005-10-04~
9. 전종목시세 (12001): 2007-01-02~
10. 파생상품시세 (12501): 2015-01-02~ (선물/옵션 전체, 주식선물 포함)

※ 지수구성종목은 별도 DAG (krx_index_constituents)로 분리
"""
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from airflow.exceptions import AirflowSkipException
from airflow.decorators import dag, task
from pandas import Timestamp

from common import (
    DEFAULT_ARGS,
    cleanup_temp,
    get_parquet_path,
    is_trading_day,
    load_from_temp,
    safe_crawl,
    save_to_final,
    save_to_temp,
    setup_proxy,
    resolve_target_date,
)
from lib.pipeline import trigger_next_dag
from mft_krx.crawlers.core import (
    etf전종목등락률_13102,
    etf전종목시세_13101,
    업종별분포_12024,
    업종분류현황_12025,
    외국인보유량_개별_12023,
    전종목등락률_12002,
    전종목시세_12001,
    파생전종목시세_12501,
    코스피지수시세추이_11003,
    투자자별거래실적개별종목_12010,
)

# =============================================================================
# 크롤링 대상 메타데이터 (시작 날짜 순으로 정렬)
# =============================================================================
CRAWL_TARGETS = [
    {"name": "코스피지수시세추이", "func": 코스피지수시세추이_11003, "start_date": "1980-01-04", "range_query": True},
    {"name": "전종목등락률", "func": 전종목등락률_12002, "start_date": "1995-05-02"},
    {"name": "업종별분포", "func": 업종별분포_12024, "start_date": "1995-05-02"},
    {"name": "업종분류현황", "func": 업종분류현황_12025, "start_date": "2001-01-02"},
    {"name": "ETF전종목시세", "func": etf전종목시세_13101, "start_date": "2002-10-14"},
    {"name": "ETF전종목등락률", "func": etf전종목등락률_13102, "start_date": "2002-10-14"},
    {"name": "투자자별거래실적개별종목", "func": 투자자별거래실적개별종목_12010, "start_date": "2003-01-02"},
    {"name": "외국인보유량", "func": 외국인보유량_개별_12023, "start_date": "2005-10-04"},
    {"name": "전종목시세", "func": 전종목시세_12001, "start_date": "2007-01-02"},
    {"name": "파생전종목시세", "func": 파생전종목시세_12501, "start_date": "2015-01-02"},
]

# DAG start_date는 가장 오래된 데이터 기준
MIN_START_DATE = datetime(1980, 1, 4)


# =============================================================================
# Task 정의
# =============================================================================

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

    if not is_trading_day(date):
        print(f"Skip: {date.date()} is not a trading day")
        return False

    print(f"Trading day confirmed: {date.date()}")
    return True


@task
def crawl_item(
    item_index: int,
    logical_date=None,
    data_interval_end=None,
    dag_run=None,
    run_id=None,
) -> str:
    """단일 크롤링 항목 처리

    A failed or skipped prerequisite stops the remaining sequence.  This keeps
    downstream KIS collection from using an incomplete KRX universe.
    """
    # Task 간 3초 sleep (IP 차단 방지)
    if item_index > 0:
        time.sleep(3)

    target = CRAWL_TARGETS[item_index]
    date = resolve_target_date(
        logical_date=logical_date,
        data_interval_end=data_interval_end,
        dag_run=dag_run,
    )
    target_start = Timestamp(target["start_date"])

    # 시작 날짜 이전이면 스킵
    if date < target_start:
        raise AirflowSkipException(
            f"Skip {target['name']}: {date.date()} < start_date {target_start.date()}"
        )

    setup_proxy()

    # 범위 조회 함수 (지수시세추이)는 당일만 조회
    if target.get("range_query"):
        df = safe_crawl(target["func"], date, date)
    else:
        df = safe_crawl(target["func"], date)

    if len(df) == 0:
        raise RuntimeError(
            f"Required KRX dataset is empty: {target['name']} for {date.date()}"
        )

    # 임시 파일 저장
    temp_path = save_to_temp(df, target["name"], date, run_id)

    # 최종 저장
    df_loaded = load_from_temp(temp_path)
    result = save_to_final(df_loaded, target["name"], date)
    cleanup_temp(temp_path)

    print(f"Completed: {target['name']} - {result}")
    return result


@task(trigger_rule="all_done")
def validate_and_trigger_adjustment(
    logical_date=None,
    data_interval_end=None,
    dag_run=None,
    run_id=None,
) -> dict:
    """Verify all nightly KRX outputs before launching ratio conversion."""
    date = resolve_target_date(
        logical_date=logical_date,
        data_interval_end=data_interval_end,
        dag_run=dag_run,
    )
    missing = [
        str(get_parquet_path(target["name"], date))
        for target in CRAWL_TARGETS
        if not get_parquet_path(target["name"], date).exists()
    ]
    if missing:
        raise RuntimeError(f"KRX batch incomplete for {date.date()}: {missing}")

    return trigger_next_dag(
        "krx_convert_adj_ratio",
        date.date().isoformat(),
        source_dag_id="krx_daily_batch",
        source_run_id=run_id or "manual",
    )


# =============================================================================
# DAG 정의
# =============================================================================


@dag(
    dag_id="krx_daily_batch",
    schedule="0 23 * * 1-5",  # 매일 23시 월~금
    start_date=MIN_START_DATE,
    catchup=False,
    max_active_runs=1,
    tags=["krx", "daily", "batch"],
    doc_md=__doc__,
    default_args=DEFAULT_ARGS,
)
def krx_daily_batch():
    """KRX 일일 배치 크롤링 DAG."""

    # 1. 거래일 체크 (비거래일이면 전체 스킵)
    trading_day_check = check_trading_day()

    # 2. 순차 실행: check >> crawl_1 >> crawl_2 >> ... >> crawl_N
    previous_task = trading_day_check
    for i in range(len(CRAWL_TARGETS)):
        current_task = crawl_item.override(task_id=f"crawl_{CRAWL_TARGETS[i]['name']}")(item_index=i)
        previous_task >> current_task
        previous_task = current_task

    trigger_adjustment = validate_and_trigger_adjustment()
    previous_task >> trigger_adjustment


# Instantiate the DAG
krx_daily_batch()
