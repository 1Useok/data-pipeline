"""Catch up missing outputs after downtime.

Scans recent trading days and triggers idempotent backfill runs for DAGs whose
expected parquet/log outputs are missing.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import DagRunAlreadyExists
from airflow.api.common.trigger_dag import trigger_dag

from common import DEFAULT_ARGS
from lib import missing_detector
from lib.pipeline import INDEX_DAG_ID, earliest_missing_stage
from notify import send

KST = pendulum.timezone("Asia/Seoul")
MAX_ATTEMPTS_PER_OUTPUT = 3


def _summarize(result: dict) -> str:
    lines = []
    for dag_id, items in sorted(result["by_dag"].items()):
        dates = sorted({item["date"] for item in items})
        lines.append(f"{dag_id}: {', '.join(dates)}")
    return "\n".join(lines)


@task(execution_timeout=timedelta(minutes=10), retries=0)
def scan_and_trigger() -> dict:
    # At 09:00 today's nightly outputs are not due yet, so scan only fully
    # completed trading days.
    end_date = pendulum.now("Asia/Seoul").subtract(days=1).date()
    result = missing_detector.scan_recent(end=end_date, days=7)
    missing = result["missing"]
    
    if not missing:
        send("INFO", "catch_up: no missing outputs in last 7 trading days")
        return {"triggered": [], "missing": []}

    send("WARN", "catch_up detected missing outputs:\n" + _summarize(result))
    triggered = []
    dates = sorted({item["date"] for item in missing})

    for date_str in dates:
        missing_dags = {
            item["dag_id"] for item in missing if item["date"] == date_str
        }
        index_missing = INDEX_DAG_ID in missing_dags
        if index_missing:
            triggered.append(_trigger_dagrun(INDEX_DAG_ID, date_str))

        # Trigger only the earliest missing main-pipeline stage.  Its successful
        # completion launches every downstream stage with the same date.
        stage = earliest_missing_stage(missing_dags)
        if stage:
            triggered.append(_trigger_dagrun(stage, date_str))

    queued = [run for run in triggered if run["status"] in {"triggered", "retry_after_failed", "retry_after_success"}]
    active = [run for run in triggered if run["status"] == "already_active"]
    limited = [run for run in triggered if run["status"] == "attempt_limit_reached"]
    send(
        "INFO",
        "catch_up queued missing-output runs: "
        f"queued={len(queued)}, already_active={len(active)}, attempt_limited={len(limited)}",
    )
    if limited:
        summary = ", ".join(f"{r['dag_id']}@{r['date']}={r['state']}" for r in limited)
        send("WARN", f"catch_up attempt limit reached while outputs are still missing: {summary}")
    return {"triggered": triggered, "missing": missing}


def _trigger_dagrun(dag_id: str, date_str: str) -> dict:
    """Trigger one backfill DagRun if there is no active run for the output."""
    base_run_id = f"catch_up__{dag_id}__{date_str}"
    logical_date = pendulum.datetime(
        int(date_str[0:4]),
        int(date_str[5:7]),
        int(date_str[8:10]),
        0,
        0,
        0,
        tz=KST,
    )

    run_id, status, state = _select_or_trigger_dagrun(dag_id, date_str, base_run_id, logical_date)

    send("INFO", f"catch_up {dag_id} {date_str}: {status} ({state})")
    return {"dag_id": dag_id, "date": date_str, "run_id": run_id, "status": status, "state": state}


def _target_date_from_run(run) -> str | None:
    """Return the explicit pipeline date stored in a DagRun configuration."""
    conf = getattr(run, "conf", None)
    if isinstance(conf, dict) and conf.get("date"):
        return str(conf["date"])
    return None


def _active_run_for_date(runs, date_str: str):
    """Find an active run for a date regardless of which component created it."""
    return next(
        (
            run
            for run in runs
            if str(getattr(run, "state", "")) in {"queued", "running"}
            and _target_date_from_run(run) == date_str
        ),
        None,
    )


def _select_or_trigger_dagrun(
    dag_id: str,
    date_str: str,
    base_run_id: str,
    logical_date: pendulum.DateTime,
) -> tuple[str, str, str]:
    """Use an active catch-up run, or create a bounded retry for missing output."""
    from airflow.models.dagrun import DagRun
    from airflow.settings import Session

    session = Session()
    try:
        active_runs = (
            session.query(DagRun)
            .filter(
                DagRun.dag_id == dag_id,
                DagRun.state.in_(("queued", "running")),
            )
            .order_by(DagRun.id.desc())
            .all()
        )
        existing_runs = (
            session.query(DagRun)
            .filter(DagRun.dag_id == dag_id, DagRun.run_id.like(f"{base_run_id}%"))
            .order_by(DagRun.id.desc())
            .all()
        )
    finally:
        session.close()

    active = _active_run_for_date(active_runs, date_str)
    if active is not None:
        return active.run_id, "already_active", str(active.state)

    if len(existing_runs) >= MAX_ATTEMPTS_PER_OUTPUT:
        latest = existing_runs[0]
        return latest.run_id, "attempt_limit_reached", str(latest.state)

    if existing_runs:
        retry_stamp = pendulum.now("UTC").strftime("%Y%m%dT%H%M%S%f")
        run_id = f"{base_run_id}__retry_{retry_stamp}"
        execution_date = logical_date.add(seconds=len(existing_runs))
        latest_state = str(existing_runs[0].state)
        status = "retry_after_success" if latest_state == "success" else "retry_after_failed"
    else:
        run_id = base_run_id
        execution_date = logical_date
        status = "triggered"

    try:
        trigger_dag(
            dag_id=dag_id,
            run_id=run_id,
            conf={"date": date_str, "triggered_by": "catch_up"},
            execution_date=execution_date,
            replace_microseconds=False,
        )
    except DagRunAlreadyExists:
        status = "already_exists"

    return run_id, status, "queued"


@dag(
    dag_id="catch_up",
    schedule="0 9 * * 1-5",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["ops", "catch-up"],
    doc_md=__doc__,
    default_args={**DEFAULT_ARGS, "retries": 0},
)
def catch_up():
    scan_and_trigger()


catch_up()
