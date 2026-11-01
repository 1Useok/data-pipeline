"""Cross-DAG orchestration helpers for the nightly market data pipeline."""

from __future__ import annotations

import hashlib
from datetime import date

import pendulum

PIPELINE_STAGES = (
    "krx_daily_batch",
    "krx_convert_adj_ratio",
    "kis_minute_batch",
    "convert_raw_to_bt",
    "data_quality_check",
)
INDEX_DAG_ID = "krx_index_constituents"


def earliest_missing_stage(missing_dag_ids: set[str]) -> str | None:
    """Return only the first missing stage in dependency order."""
    return next((dag_id for dag_id in PIPELINE_STAGES if dag_id in missing_dag_ids), None)


def find_active_run_for_date(runs, target_date: str):
    """Return a queued/running DagRun for the explicit pipeline date."""
    return next(
        (
            run
            for run in runs
            if str(getattr(run, "state", "")) in {"queued", "running"}
            and isinstance(getattr(run, "conf", None), dict)
            and str(run.conf.get("date")) == target_date
        ),
        None,
    )


def trigger_next_dag(
    dag_id: str,
    target_date: str | date,
    *,
    source_dag_id: str,
    source_run_id: str,
) -> dict[str, str]:
    """Idempotently trigger the next DAG with an explicit target date.

    The deterministic run id prevents a cleared/retried trigger task from
    creating duplicate downstream runs.  A new upstream DagRun gets a distinct
    digest and can therefore launch a new recovery attempt.
    """
    from airflow.api.common.trigger_dag import trigger_dag
    from airflow.exceptions import DagRunAlreadyExists
    from airflow.models.dagrun import DagRun
    from airflow.settings import Session

    date_str = date.fromisoformat(str(target_date)).isoformat()
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
    finally:
        session.close()

    active = find_active_run_for_date(active_runs, date_str)
    if active is not None:
        return {
            "dag_id": dag_id,
            "date": date_str,
            "run_id": active.run_id,
            "status": "already_active",
        }

    digest = hashlib.sha256(source_run_id.encode("utf-8")).hexdigest()[:12]
    run_id = f"pipeline__{date_str}__from_{source_dag_id}__{digest}"

    try:
        trigger_dag(
            dag_id=dag_id,
            run_id=run_id,
            conf={
                "date": date_str,
                "triggered_by": source_dag_id,
                "source_run_id": source_run_id,
            },
            execution_date=pendulum.now("UTC"),
            replace_microseconds=False,
        )
        status = "triggered"
    except DagRunAlreadyExists:
        status = "already_exists"

    return {
        "dag_id": dag_id,
        "date": date_str,
        "run_id": run_id,
        "status": status,
    }
