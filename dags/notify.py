"""Notification helpers for Airflow DAG alerts."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass

import requests

logger = logging.getLogger(__name__)


@dataclass
class TelegramNotifier:
    """Plain-text Telegram notifier.

    Telegram is optional in local/dev runs. Missing credentials disable the
    network send while still logging the message.
    """

    token: str | None = None
    chat_id: str | None = None
    timeout: int = 10

    def __post_init__(self) -> None:
        self.token = self.token if self.token is not None else os.environ.get("TELEGRAM_BOT_TOKEN")
        self.chat_id = self.chat_id if self.chat_id is not None else os.environ.get("TELEGRAM_CHAT_ID")

    @property
    def enabled(self) -> bool:
        return bool(self.token and self.chat_id)

    def send(self, level: str, message: str) -> bool:
        level_name = level.upper()
        log_func = getattr(logger, level.lower(), logger.info)
        log_func("%s: %s", level_name, message)

        if not self.enabled:
            logger.info("Telegram disabled: TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID not set")
            return False

        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        try:
            response = requests.post(
                url,
                data={"chat_id": self.chat_id, "text": f"[{level_name}] {message}"},
                timeout=self.timeout,
            )
            response.raise_for_status()
        except Exception as exc:
            logger.error("Telegram send failed: %s: %s", type(exc).__name__, exc)
            return False
        return True


notifier = TelegramNotifier()


def send(level: str, message: str) -> bool:
    """Send a notification via the default notifier."""
    return notifier.send(level, message)


def send_failure_dm(context) -> None:
    """Compatibility callback used by older KRX DAGs."""
    dag_id = getattr(context.get("dag"), "dag_id", context.get("dag_id", "unknown"))
    task_id = getattr(context.get("task_instance"), "task_id", context.get("task_id", "unknown"))
    exception = context.get("exception")
    send("ERROR", f"Airflow task failed: dag={dag_id} task={task_id} error={exception}")
