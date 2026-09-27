# UPI Payment Reconciliation Pipeline Makefile
include .env
export

DATE ?= $(shell date +%Y-%m-%d)
START ?= 2026-09-01
END ?= 2026-09-10

.PHONY: help setup init-db generate run backfill test dbt-snapshot dbt-run dbt-test clean

help:
	@echo "Available commands:"
	@echo "  make setup          - Install dependencies"
	@echo "  make init-db        - Initialize Postgres schemas and tables"
	@echo "  make generate       - Generate 10 days of synthetic data"
	@echo "  make run DATE=...   - Ingest and process a specific date"
	@echo "  make backfill       - Run ingestion across a date range"
	@echo "  make test           - Run unit tests with pytest"
	@echo "  make dbt-run        - Execute dbt models"
	@echo "  make dbt-test       - Execute dbt tests"

setup:
	pip install -r requirements.txt

init-db:
	psql -h $(PG_HOST) -p $(PG_PORT) -U $(PG_USER) -d $(PG_DB) -f sql/init.sql

generate:
	python generator/generate_data.py --days 10 --orders-per-day 20000

run:
	python ingestion/extract.py --run-date $(DATE)
	python ingestion/validate.py --run-date $(DATE)
	python ingestion/load.py --run-date $(DATE)
	cd dbt_upi && dbt snapshot && dbt run --vars '{"run_date": "$(DATE)"}' && dbt test

backfill:
	python -c "import datetime, subprocess, sys; s = datetime.date.fromisoformat('$(START)'); e = datetime.date.fromisoformat('$(END)'); d = s; [subprocess.run(['make', 'run', f'DATE={d.isoformat()}'], check=True) or (d := d + datetime.timedelta(days=1)) while d <= e]"

test:
	pytest tests/

dbt-snapshot:
	cd dbt_upi && dbt snapshot

dbt-run:
	cd dbt_upi && dbt run

dbt-test:
	cd dbt_upi && dbt test

clean:
	python -c "import pathlib, shutil; [shutil.rmtree(p) for p in pathlib.Path('.').rglob('__pycache__')]"
