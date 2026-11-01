"""Regression tests for announced KRX one-off holidays."""

from mft_common.krx_calendar import krx_session_iso_dates, previous_krx_session_date


def test_2026_local_election_day_is_not_a_krx_session():
    sessions = krx_session_iso_dates("2026-06-01", "2026-06-05")

    assert sessions == ["2026-06-01", "2026-06-02", "2026-06-04", "2026-06-05"]


def test_previous_session_skips_weekend_and_election_holiday():
    assert previous_krx_session_date("20260209").isoformat() == "2026-02-06"
    assert previous_krx_session_date("20260604").isoformat() == "2026-06-02"


def test_2026_restored_constitution_day_is_not_a_krx_session():
    sessions = krx_session_iso_dates("2026-07-16", "2026-07-20")

    assert sessions == ["2026-07-16", "2026-07-20"]
    assert previous_krx_session_date("20260720").isoformat() == "2026-07-16"
