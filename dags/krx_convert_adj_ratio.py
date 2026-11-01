"""Normalize KRX adjustment-ratio history after the daily KRX crawl.

The DAG has no independent cron schedule: ``krx_daily_batch`` triggers it only
after every required daily KRX file is present.  On pipeline-triggered runs it
launches ``kis_minute_batch`` after a successful conversion.  A direct manual
run remains repair-only unless ``continue_pipeline=true`` is supplied in the
DagRun configuration.
"""

from datetime import datetime, timedelta
from pathlib import Path
import sys

from airflow.decorators import dag, task

sys.path.insert(0, str(Path(__file__).parent))

from common import DEFAULT_ARGS, resolve_target_date
from lib.pipeline import trigger_next_dag


@task(execution_timeout=timedelta(hours=2))
def run_full_recalculate(logical_date=None, data_interval_end=None, dag_run=None) -> dict:
    from mft_krx.convert import convert_archive_to_backtesting
    from mft_krx.convert.daily_adj_ratio import full_recalculate

    target = resolve_target_date(
        logical_date=logical_date,
        data_interval_end=data_interval_end,
        dag_run=dag_run,
    )
    monthly = convert_archive_to_backtesting(months=[target.strftime("%Y-%m")])
    result = full_recalculate()
    if result.get("status") != "success" or not result.get("files"):
        raise RuntimeError(f"Adjustment-ratio recalculation failed: {result}")
    return {**result, "converted_files": monthly.get("file_count", 0)}


def _should_continue_pipeline(dag_run) -> bool:
    """Return whether this conversion run is part of the automated chain."""
    conf = getattr(dag_run, "conf", None)
    if not isinstance(conf, dict):
        return False
    return conf.get("triggered_by") in {"krx_daily_batch", "catch_up"} or bool(
        conf.get("continue_pipeline", False)
    )


@task
def trigger_kis_after_conversion(
    conversion_result: dict,
    logical_date=None,
    data_interval_end=None,
    dag_run=None,
    run_id=None,
) -> dict:
    """Launch KIS only for an automated or explicitly continued run."""
    if conversion_result.get("status") != "success" or not conversion_result.get("files"):
        raise RuntimeError(f"Adjustment-ratio conversion is incomplete: {conversion_result}")
    if not _should_continue_pipeline(dag_run):
        return {
            "status": "repair_only",
            "reason": "manual run did not request pipeline continuation",
        }

    target = resolve_target_date(
        logical_date=logical_date,
        data_interval_end=data_interval_end,
        dag_run=dag_run,
    )
    return trigger_next_dag(
        "kis_minute_batch",
        target.date().isoformat(),
        source_dag_id="krx_convert_adj_ratio",
        source_run_id=run_id or "manual",
    )


@dag(
    dag_id="krx_convert_adj_ratio",
    schedule=None,
    start_date=datetime(1995, 5, 2),
    catchup=False,
    max_active_runs=1,
    tags=["krx", "daily", "adj-ratio"],
    doc_md=__doc__,
    default_args=DEFAULT_ARGS,
)
def krx_convert_adj_ratio():
    conversion = run_full_recalculate()
    conversion >> trigger_kis_after_conversion(conversion)


krx_convert_adj_ratio()
