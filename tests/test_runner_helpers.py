from datetime import datetime
from pathlib import Path
import polars as pl
import pytest

from tests._runner_helpers import (
    seed_convert_dependencies,
    verify_outputs,
    CONVERT_DEPENDENCIES_REL,
    KRX_CRAWLED_UNIVERSE_ITEMS,
)


def _make_fake_data_db(root: Path, target: datetime):
    for rel in CONVERT_DEPENDENCIES_REL:
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({"x": [1]}).write_parquet(p)
    ym = target.strftime("%Y-%m")
    for item in KRX_CRAWLED_UNIVERSE_ITEMS:
        krx = root / "MFT_backtesting/KR/krx_crawled" / item / f"{ym}.parquet"
        krx.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({"x": [1]}).write_parquet(krx)


def test_seed_convert_dependencies_copies_all(tmp_path):
    data_db = tmp_path / "data_db"
    test_db = tmp_path / "test_db"
    target = datetime(2026, 4, 22)
    _make_fake_data_db(data_db, target)

    log = seed_convert_dependencies(test_db, data_db, target)
    for rel in CONVERT_DEPENDENCIES_REL:
        assert (test_db / rel).exists()
    for item in KRX_CRAWLED_UNIVERSE_ITEMS:
        assert (test_db / f"MFT_backtesting/KR/krx_crawled/{item}/2026-04.parquet").exists()
    assert len(log) == len(CONVERT_DEPENDENCIES_REL) + len(KRX_CRAWLED_UNIVERSE_ITEMS)


def test_seed_convert_dependencies_raises_on_missing(tmp_path):
    data_db = tmp_path / "data_db"  # empty
    test_db = tmp_path / "test_db"
    with pytest.raises(FileNotFoundError):
        seed_convert_dependencies(test_db, data_db, datetime(2026, 4, 22))


def test_verify_outputs_passes_for_nonempty_files(tmp_path):
    p = tmp_path / "x.parquet"
    pl.DataFrame({"v": [1, 2]}).write_parquet(p)
    verify_outputs([p], "step")  # no exception


def test_verify_outputs_raises_for_missing(tmp_path):
    with pytest.raises(RuntimeError, match="missing expected outputs"):
        verify_outputs([tmp_path / "nope.parquet"], "step")


def test_verify_outputs_raises_for_empty(tmp_path):
    p = tmp_path / "empty.parquet"
    pl.DataFrame({"v": []}, schema={"v": pl.Int64}).write_parquet(p)
    with pytest.raises(RuntimeError, match="empty"):
        verify_outputs([p], "step")


def test_reset_test_data_db_refuses_wrong_name(tmp_path):
    """reset_test_data_db must refuse to wipe any path not named 'test_data_db'."""
    from tests._runner_helpers import reset_test_data_db
    import pytest as _pytest

    wrong = tmp_path / "data_db"  # note: not 'test_data_db'
    wrong.mkdir()
    sentinel = wrong / "important.txt"
    sentinel.write_text("do not delete")

    with _pytest.raises(RuntimeError, match="Refusing to wipe"):
        reset_test_data_db(wrong)

    assert sentinel.exists(), "safety guard failed — sentinel was deleted"


def test_reset_test_data_db_succeeds_with_correct_name(tmp_path):
    """reset_test_data_db must wipe and recreate test_data_db normally."""
    from tests._runner_helpers import reset_test_data_db

    target = tmp_path / "test_data_db"
    target.mkdir()
    (target / "junk.txt").write_text("remove me")

    reset_test_data_db(target)

    assert target.exists()
    assert not (target / "junk.txt").exists()
