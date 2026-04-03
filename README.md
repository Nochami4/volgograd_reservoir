# volgograd_reservoir

Reproducible ETL pipeline for analytical datasets on abrasion-prone shores of the Volgograd Reservoir.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -e ".[dev]"
```

Or with `make`:

```bash
make install
```

## Main Commands

```bash
make build-datasets
make qc
pytest
```

The main dataset build entrypoint is:

```bash
python -m src.pipeline
```

## Principles

- Raw files in `data/raw/` are read-only.
- Derived tables are rebuilt from scripts, not notebooks.
- Missing raw values are preserved as missing and flagged instead of being imputed.
- Intermediate and final datasets include provenance and QC metadata where applicable.
