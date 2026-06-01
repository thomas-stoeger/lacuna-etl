# Working in lacuna-etl

This is the working contract for an AI agent in this repo. Read
[docs/DESIGN.md](docs/DESIGN.md) first; it is the authoritative description of
what this repo produces and the guarantees those outputs carry.

## Role

This repo is the transformation half of a two-repo system: `data_downloader`
acquires and versions raw scientific datasets, and `lacuna-etl` cleans them into
typed, validated Parquet with schema sidecars. **This repo owns the canonical
output schema and contract.** The inspection and MCP repos consume these outputs
and reference this repo's schema; changes here ripple to them.

## Runtime environment

This repo runs inside a devcontainer that mounts only this repository plus the
data directory. The data directory holds both the raw snapshots read in
(`DL_DATA_ROOT`) and the cleaned datasets written out (`ETL_OUTPUT_ROOT`). You
will not have the sibling repos (`data_downloader`, inspection, MCP) on disk, so
do not assume their contents; rely on docs/DESIGN.md for the contract instead.
Run datasets with `etl run <name>` and list them with `etl list`.

## What you may do

- Add or modify dataset pipelines under `src/lacuna_etl/datasets/`, following the
  existing `DatasetPipeline` extract/transform/load structure.
- Add identifier types and `ColumnSpec`-based validation in `src/lacuna_etl/core/`.
- Run pipelines against the mounted data directory to verify changes.
- Write cleaned outputs to the data directory (`ETL_OUTPUT_ROOT`); this is the
  intended product of a run.

## What you may not do

- **Do not change the output schema casually.** Adding, removing, renaming, or
  retyping an output column, changing an identifier's canonical form, or changing
  a table's grain is a design change. It requires updating docs/DESIGN.md in the
  same change and is breaking for the inspection and MCP repos.
- Do not weaken or skip identifier validation to make a run pass. If real source
  data violates a contract, fix the transform or the contract deliberately, not
  the check.
- Do not add data acquisition or downloading here; that belongs to
  `data_downloader`. This repo only transforms what is already on disk.
- Do not commit anything from the data directory, `intermediate/`, `scratch/`, or
  the virtual environments.

## Conventions

- One module per dataset (or per OpenAlex entity); register it with `@register`
  and import it in `src/lacuna_etl/datasets/__init__.py` so the registry sees it.
- Define the schema once per module as `SCHEMA` / `TABLES_DOC` dicts of
  `ColumnSpec`; the sidecar YAML and validation both derive from it. Keep these
  near the top of the module.
- Column names are snake_case; map source headers explicitly via a `_RENAME` dict.
- Cast identifier columns through their `Identifier` type, then validate. Treat
  empty/whitespace strings as null.
- Prefer pandas for in-memory datasets and polars with sharded
  `ProcessPoolExecutor` for the large streaming ones (OpenAlex, PubMed); match
  the shape of the dataset you are touching.
- Make streaming pipelines restartable (done-markers or shard-exists checks).
- Match the surrounding code's style; keep comments at the existing density and
  explain *why*, not *what*.

## Working notes

Working notes live in the gitignored `scratch/` directory, and machine-specific
context lives in `CLAUDE.local.md` (also gitignored). Neither is committed.
