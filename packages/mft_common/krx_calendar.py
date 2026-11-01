"""KRX session calendar with announced one-off market holidays."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from functools import lru_cache

import exchange_calendars as xcals


# exchange_calendars cannot know every holiday announced after its release.
# KRX closed for the 9th nationwide local election on 2026-06-03. It also
# closed on 2026-07-17 after Constitution Day was restored as a public
# holiday in 2026.
_ANNOUNCED_EXTRA_HOLIDAYS = frozenset({
    date(2026, 6, 3),
    date(2026, 7, 17),
})


def krx_session_dates(start: str, end: str) -> list[date]:
    """Return KRX sessions, including announced one-off holiday overrides."""
    calendar = xcals.get_calendar("XKRX")
    return [
        session.date()
        for session in calendar.sessions_in_range(start, end)
        if session.date() not in _ANNOUNCED_EXTRA_HOLIDAYS
    ]


def krx_session_iso_dates(start: str, end: str) -> list[str]:
    return [value.isoformat() for value in krx_session_dates(start, end)]


@lru_cache(maxsize=512)
def previous_krx_session_date(value: str) -> date:
    """Return the KRX session immediately preceding YYYYMMDD/ISO date."""
    target = date.fromisoformat(value) if "-" in value else datetime.strptime(value, "%Y%m%d").date()
    candidates = krx_session_dates(
        (target - timedelta(days=14)).isoformat(),
        (target - timedelta(days=1)).isoformat(),
    )
    if not candidates:
        raise ValueError(f"No previous KRX session found for {value}")
    return candidates[-1]
