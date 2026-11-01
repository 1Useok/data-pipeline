"""Shared pytest fixtures and path constants for pipeline tests."""
from __future__ import annotations
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
PACKAGES_DIR = REPO_ROOT / "packages"
DAGS_DIR = REPO_ROOT / "dags"

# Make project packages importable in tests and ad-hoc scripts.
for p in (str(PACKAGES_DIR), str(DAGS_DIR)):
    if p not in sys.path:
        sys.path.insert(0, p)

# Filesystem layout expected by the validation framework.
# Root folder for local data used by the comparison/runner tools.
# Override with the PIPELINE_DATA_ROOT env var.
DATA_ROOT = Path(os.environ.get("PIPELINE_DATA_ROOT", Path(__file__).resolve().parents[1]))
DATA_DB = DATA_ROOT / "data_db"
TEST_DATA_DB = DATA_ROOT / "test_data_db"
COMPARE_DB = DATA_ROOT / "compare_db"
OUTPUT_DIR = REPO_ROOT / "tests" / "output"


def load_docker_env() -> None:
    """Load `docker/.env` into os.environ without overwriting existing keys."""
    env_path = REPO_ROOT / "docker" / ".env"
    if not env_path.exists():
        return
    with open(env_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            if k not in os.environ:
                os.environ[k] = v


# --- Airflow stub ------------------------------------------------------
# Airflow is not installed in the local test env, but DAG modules under
# `dags/` do `import airflow`. We stub it here so tests can import DAG
# modules without an Airflow runtime. The stub preserves enough API for
# DAG bodies to succeed at module load (decorators, DAG() context manager).

def _install_airflow_stub() -> None:
    """Install a minimal airflow stub into sys.modules if airflow is unavailable."""
    try:
        import airflow  # noqa: F401
        return  # Real airflow is installed — no stub needed.
    except ImportError:
        pass

    from unittest.mock import MagicMock

    # Flag that tells task wrappers to skip execution during module-level DAG wiring.
    _state = {"loading_dag": False}

    def _make_task_wrapper(fn, decorator_kwargs=None):
        """Return a wrapper that skips execution during DAG-loading phase."""
        def _wrapper(*args, **kwargs):
            if _state["loading_dag"]:
                return MagicMock()  # task registration no-op
            return fn(*args, **kwargs)
        _wrapper.__wrapped__ = fn
        _wrapper.__name__ = fn.__name__
        # Like Airflow's decorated task: `.function` and the decorator `.kwargs`
        # (e.g. trigger_rule) are inspectable, and `.override(task_id=...)` works.
        _wrapper.function = fn
        _wrapper.kwargs = dict(decorator_kwargs or {})
        _wrapper.override = lambda *a, **kw: _wrapper
        return _wrapper

    def _task_decorator(*args, **kwargs):
        if len(args) == 1 and callable(args[0]) and not kwargs:
            # @task without parens
            return _make_task_wrapper(args[0])
        # @task(...) with params — return a decorator
        def _wrap(fn):
            return _make_task_wrapper(fn, kwargs)
        return _wrap

    # Support @task.short_circuit, @task.branch, etc.
    _task_decorator.short_circuit = _task_decorator
    _task_decorator.branch = _task_decorator

    def _dag_decorator(*args, **kwargs):
        def _wrap(fn):
            def _wrapped(*fn_args, **fn_kwargs):
                _state["loading_dag"] = True
                try:
                    return fn(*fn_args, **fn_kwargs)
                finally:
                    _state["loading_dag"] = False
            _wrapped.__wrapped__ = fn
            _wrapped.__name__ = fn.__name__
            return _wrapped
        if len(args) == 1 and callable(args[0]) and not kwargs:
            return _wrap(args[0])
        return _wrap

    # DAG used as `with DAG(...) as dag:` — set loading flag during wiring
    class _FakeDAG:
        def __init__(self, *a, **kw):
            pass
        def __enter__(self):
            _state["loading_dag"] = True
            return self
        def __exit__(self, *a):
            _state["loading_dag"] = False
            return False

    # airflow.exceptions
    airflow_exceptions = MagicMock()
    airflow_exceptions.AirflowSkipException = type(
        "AirflowSkipException", (Exception,), {}
    )
    airflow_exceptions.AirflowException = type(
        "AirflowException", (Exception,), {}
    )
    airflow_exceptions.DagRunAlreadyExists = type(
        "DagRunAlreadyExists", (Exception,), {}
    )

    # airflow.sdk — DAG is used as a context manager; task is a decorator
    airflow_sdk = MagicMock()
    airflow_sdk.task = _task_decorator
    airflow_sdk.DAG = _FakeDAG
    airflow_decorators = MagicMock()
    airflow_decorators.task = _task_decorator
    airflow_decorators.dag = _dag_decorator

    airflow_mod = MagicMock()
    airflow_mod.exceptions = airflow_exceptions
    airflow_mod.sdk = airflow_sdk
    airflow_mod.decorators = airflow_decorators
    airflow_mod.DAG = _FakeDAG
    airflow_trigger = MagicMock()
    airflow_trigger.trigger_dag = MagicMock()

    sys.modules.setdefault("airflow", airflow_mod)
    sys.modules.setdefault("airflow.exceptions", airflow_exceptions)
    sys.modules.setdefault("airflow.sdk", airflow_sdk)
    sys.modules.setdefault("airflow.decorators", airflow_decorators)
    sys.modules.setdefault("airflow.api", MagicMock())
    sys.modules.setdefault("airflow.api.common", MagicMock())
    sys.modules.setdefault("airflow.api.common.trigger_dag", airflow_trigger)


_install_airflow_stub()
