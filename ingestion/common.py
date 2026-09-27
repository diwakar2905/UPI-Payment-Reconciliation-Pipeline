"""Shared helpers for the extract / validate / load pipeline steps."""
import time
from contextlib import contextmanager
from pathlib import Path

from ingestion.db import get_db_cursor

DATA_DIR = Path("data")


def run_dir(run_date):
    d = DATA_DIR / run_date
    d.mkdir(parents=True, exist_ok=True)
    return d


def staged_path(run_date, name):
    return run_dir(run_date) / f"staged_{name}.csv"


def valid_path(run_date, name):
    return run_dir(run_date) / f"valid_{name}.csv"


def raw_path(run_date, name):
    """Original generator output, e.g. data/<run_date>/gateway.csv."""
    return DATA_DIR / run_date / f"{name}.csv"


@contextmanager
def timed_step():
    start = time.monotonic()
    result = {}
    yield result
    result["duration_seconds"] = round(time.monotonic() - start, 2)


def log_pipeline_run(run_date, step, rows_in=0, rows_out=0, rows_rejected=0, duration_seconds=0.0):
    with get_db_cursor(commit=True) as cur:
        cur.execute(
            """
            INSERT INTO raw.pipeline_runs (run_date, step, rows_in, rows_out, rows_rejected, duration_seconds)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (run_date, step, rows_in, rows_out, rows_rejected, duration_seconds),
        )
