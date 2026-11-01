"""Crash-safe parquet storage helpers.

Parquet files are written to a temporary file in the destination directory and
then installed with :func:`os.replace`.  Keeping the temporary file on the same
filesystem is important: only then can the final replacement be atomic.
"""

from __future__ import annotations

import os
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import polars as pl


def atomic_write_parquet(
    df: pl.DataFrame,
    destination: str | Path,
    *,
    compression: str = "lz4",
) -> Path:
    """Atomically replace ``destination`` with a validated parquet file.

    The previous destination remains intact if serialization or validation
    fails.  ``os.replace`` is atomic for files on the same filesystem.
    """
    path = Path(destination)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")

    try:
        df.write_parquet(temp_path, compression=compression)
        # Force bytes to stable storage before the rename where supported.
        # Opened r+b because Windows rejects fsync on a read-only handle (EBADF).
        with temp_path.open("r+b") as handle:
            os.fsync(handle.fileno())
        # A successful schema read catches truncated/invalid parquet files.
        pl.read_parquet_schema(temp_path)
        os.replace(temp_path, path)
    finally:
        temp_path.unlink(missing_ok=True)

    return path


@contextmanager
def parquet_update_lock(
    destination: str | Path,
    *,
    timeout_seconds: float = 300.0,
    stale_after_seconds: float = 21_600.0,
    poll_seconds: float = 0.2,
) -> Iterator[None]:
    """Serialize read/merge/write updates to a parquet destination.

    The lock uses exclusive file creation, which works across Airflow worker
    processes and across the host/container bind mount.  A stale lock left by a
    killed worker is removed after six hours by default.
    """
    path = Path(destination)
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_name(f".{path.name}.lock")
    deadline = time.monotonic() + timeout_seconds
    fd: int | None = None

    while fd is None:
        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(fd, f"pid={os.getpid()} created={time.time()}\n".encode())
        except FileExistsError:
            try:
                age = time.time() - lock_path.stat().st_mtime
                if age > stale_after_seconds:
                    lock_path.unlink(missing_ok=True)
                    continue
            except FileNotFoundError:
                continue
            if time.monotonic() >= deadline:
                raise TimeoutError(f"Timed out waiting for parquet lock: {lock_path}")
            time.sleep(poll_seconds)

    try:
        yield
    finally:
        if fd is not None:
            os.close(fd)
        lock_path.unlink(missing_ok=True)
