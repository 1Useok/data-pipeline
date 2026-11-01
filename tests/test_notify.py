"""Tests for Telegram notifier behavior."""

from unittest.mock import patch

from notify import TelegramNotifier


def test_notifier_disabled_without_credentials(monkeypatch):
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)
    notifier = TelegramNotifier()

    with patch("notify.requests.post") as post:
        assert notifier.send("INFO", "hello") is False
        post.assert_not_called()


def test_notifier_sends_plain_text(monkeypatch):
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "chat")
    notifier = TelegramNotifier()

    with patch("notify.requests.post") as post:
        post.return_value.raise_for_status.return_value = None
        assert notifier.send("ERROR", "dag_failed * [ `") is True

    _, kwargs = post.call_args
    assert kwargs["data"]["chat_id"] == "chat"
    assert kwargs["data"]["text"] == "[ERROR] dag_failed * [ `"
    assert "parse_mode" not in kwargs["data"]


def test_notifier_swallows_send_failure(monkeypatch):
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "chat")
    notifier = TelegramNotifier()

    with patch("notify.requests.post", side_effect=RuntimeError("network down")):
        assert notifier.send("ERROR", "must not break DAG") is False
