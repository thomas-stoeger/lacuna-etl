# lacuna-etl

Extract, transform, and load pipeline for versioned scientific datasets downloaded by [data_downloader](../data_downloader).

## Setup

**1. Create a Python 3.13 virtual environment and install:**

```bash
uv venv --python 3.13 .venv
uv pip install -e .
```

**2. Configure the data root (where data_downloader stores its files):**

```bash
.venv/bin/etl configure /path/to/your/data
```

This is saved to `~/.config/lacuna_etl/config.toml`. You can also set `DL_DATA_ROOT` as an environment variable instead.

Intermediate files default to `./intermediate/` in this repo. Override with `ETL_INTERMEDIATE_ROOT` or pass `--intermediate-root` to configure.

## Usage

```bash
# List registered datasets
.venv/bin/etl list

# Run a dataset pipeline (extract → transform → load)
.venv/bin/etl run <dataset>
```
