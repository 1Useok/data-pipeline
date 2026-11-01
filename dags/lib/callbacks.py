"""Airflow callbacks routed to the project notifier."""

from __future__ import annotations

import logging

from notify import send

logger = logging.getLogger(__name__)


def on_failure_callback(context) -> None:
    """Notify only after Airflow has marked the task as failed."""
    task_instance = context.get("task_instance")
    dag = context.get("dag")
    dag_id = getattr(dag, "dag_id", context.get("dag_id", "unknown"))
    task_id = getattr(task_instance, "task_id", context.get("task_id", "unknown"))
    run_id = getattr(task_instance, "run_id", context.get("run_id", "unknown"))
    exception = context.get("exception")

    message = (
        "Airflow task failed\n"
        f"dag={dag_id}\n"
        f"task={task_id}\n"
        f"run_id={run_id}\n"
        f"error={exception}"
    )
    try:
        send("ERROR", message)
    except Exception:
        logger.exception("Failed to send Airflow failure notification")
