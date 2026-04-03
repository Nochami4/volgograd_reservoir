PYTHON ?= python3
VENV ?= .venv
ACTIVATE = . $(VENV)/bin/activate

.PHONY: install build-datasets qc clean

install:
	$(PYTHON) -m venv $(VENV)
	$(ACTIVATE) && pip install --upgrade pip
	$(ACTIVATE) && pip install -e ".[dev]"

build-datasets:
	$(ACTIVATE) && python -m src.pipeline

qc:
	$(ACTIVATE) && python -m src.qc.run_qc

clean:
	rm -rf data/interim/* data/processed/* reports/tables/qc_summary.md
