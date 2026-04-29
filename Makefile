PYTHON ?= python3
VENV ?= .venv
ACTIVATE = . $(VENV)/bin/activate

.PHONY: install build-datasets build-delivery qc clean

install:
	$(PYTHON) -m venv $(VENV)
	$(ACTIVATE) && pip install --upgrade pip
	$(ACTIVATE) && pip install -e ".[dev]"

build-datasets:
	$(ACTIVATE) && python -m src.pipeline

build-delivery: build-datasets
	$(ACTIVATE) && python -m src.analysis.first_stage_analysis
	$(ACTIVATE) && python -m src.export.build_delivery_exports

qc:
	$(ACTIVATE) && python -m src.qc.run_qc

clean:
	rm -rf data/interim/* data/processed/* reports/tables/qc_summary.md
