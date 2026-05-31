# lacuna-etl design and contract

This document is the authoritative description of what this repo produces and the
guarantees those outputs carry. The inspection and MCP repos that consume these
datasets should treat the contract sections below (Output layout, Identifier
contract, Invariants) as canonical and reference them rather than re-deriving the
shape of the data.

## Purpose

`lacuna-etl` turns raw scientific bibliographic datasets, as downloaded and
versioned by the sibling `data_downloader` repo, into cleaned, typed, validated
Parquet tables with machine-readable schema sidecars.

The split is deliberate. `data_downloader` owns acquisition and versioning and
nothing else; it never reshapes data. This repo owns transformation and the
output contract and never fetches anything. Keeping download and transform in
separate repos means a re-download cannot silently change cleaning logic, and a
cleaning change can be re-run against an already-downloaded snapshot without
touching the network.

## Inputs and outputs

There are three roots, resolved in this order: environment variable, then
`~/.config/lacuna_etl/config.toml` (written by `etl configure`), then a built-in
default where one exists. See `src/lacuna_etl/config.py`.

- **Data root** (`DL_DATA_ROOT`): where `data_downloader` writes raw snapshots.
  Required; no default. Each dataset lives at `<data_root>/<name>/<version>/...`
  and the pipeline always reads the lexicographically latest `<version>`
  directory, so the ETL operates on the newest downloaded snapshot.
- **Output root** (`ETL_OUTPUT_ROOT`): where cleaned datasets are written.
  Required; no default. Outputs go to `<output_root>/<name>/`.
- **Intermediate root** (`ETL_INTERMEDIATE_ROOT`): staging area between pipeline
  stages. Defaults to `./intermediate/` in this repo. Intermediates are
  disposable; they exist for restartability, not for consumption.

Outputs are Parquet (snappy-compressed) plus a YAML sidecar per table. Parquet
is chosen for typed, nullable, columnar, compressed storage that round-trips
pandas and polars dtypes; the sidecar carries the human- and machine-readable
column contract (identifier type, description, allowed values, required flag)
that bare Parquet cannot express. The sidecar is generated from the same
`ColumnSpec` objects used to validate the data, so documentation and enforcement
cannot drift.

## Architecture

Every dataset is a `DatasetPipeline` subclass (`src/lacuna_etl/core/pipeline.py`)
with three stages run in order by `run()`:

1. **extract** — read raw files, rename source columns to snake_case, cast to
   identifier dtypes, write intermediate Parquet (or, for streaming pipelines,
   final shards directly).
2. **transform** — apply cross-dataset corrections, explode multi-valued fields,
   deduplicate, and validate identifier columns.
3. **load** — write final Parquet to the output root and emit the schema sidecar.

Pipelines self-register with the `@register` decorator into `REGISTRY`
(`src/lacuna_etl/datasets/registry.py`); importing a dataset module is what
registers it, so every dataset is imported in `datasets/__init__.py`. The CLI
(`etl list`, `etl run <name>`) is a thin shell over the registry.

`depends_on` declares cross-dataset dependencies (for example, the gene tables
depend on `ncbi_gene_history`). **Dependencies are declared, not orchestrated:**
`run()` executes a single dataset, and a dependent reads its prerequisite's
*output*, so the operator must run prerequisites first. This keeps the runner
trivial and makes each stage independently restartable, at the cost of manual
ordering. `etl list` prints the dependencies to make ordering visible.

### Two execution shapes

- **In-memory (pandas).** Used for the NCBI gene tables and iCite, which fit in
  memory. Read whole, cast, validate, write.
- **Streaming, sharded, parallel (polars + `ProcessPoolExecutor`).** Used for
  OpenAlex, PubMed, and PubTator3, whose snapshots are far too large for memory.
  Each input file is processed independently into per-file Parquet shards, with a
  done-marker (PubMed, PubTator3) or shard-exists check (OpenAlex) so an
  interrupted run resumes instead of restarting. Peak memory is bounded by one
  batch/shard, not the dataset. For OpenAlex the snapshot is already immutable per
  version, so `extract` writes final shards directly, `transform` only validates
  across shards, and `load` only writes sidecars. PubTator3's unit of work is one
  whole `BioCXML.<n>.tar.gz` archive per worker; because a single archive holds
  millions of documents, the worker flushes per-table shards every N documents to
  keep its memory bounded.

### Filling in missing values

When a field is sometimes null in raw data and you want to populate it for more
rows, the right shape depends on where the substitute value comes from.

- **Same-record fallback is normal extract/transform.** Deriving a column from
  another field on the same record — falling back from `PubDate.Year` to
  `ArticleDate.Year`, parsing a year out of a `MedlineDate` free-text string,
  stripping URL prefixes off an identifier — stays inside that record's own
  authoritative source. Populate the column directly; the column's meaning is
  unchanged and no provenance column is needed.
- **Cross-dataset heuristic linkage gets its own table.** Inferring
  `openalex_works.pmid` for records OpenAlex lacks one for, by matching against
  PubMed on DOI / title / journal / year, is a different operation: the value
  comes from outside the source record, the match is heuristic, and a false
  match links an entirely wrong paper. Such linkages belong in their own dataset
  — a crosswalk table such as `(work_id, pmid, match_source)` with `depends_on`
  on both upstreams. The upstream tables stay unchanged; consumers join the
  crosswalk when they want the enriched view. This preserves the provenance of
  every value, lets the matching heuristic be re-tuned without rewriting the
  heavy upstream outputs, and keeps each artifact answering exactly one
  question.

The rule: same column, broader population is fine when the new values come from
within the same record's own authoritative source. It becomes a contract
problem when the new values come from outside that source or from heuristic
matching across datasets — those cases need their own table so provenance,
confidence, and iteration speed are preserved.

## Identifier contract

Identifier types (`src/lacuna_etl/core/identifiers.py`) centralize the canonical
form and validation of every cross-referenced ID, so that any dataset emitting a
DOI, PMID, ORCID, and so on enforces the *same* shape. This is the core of why
downstream repos can join across datasets safely. Canonical forms:

- **DOI** — URL prefix stripped, lowercase scheme removed; stored as `10.x/...`,
  never as a `https://doi.org/` URL.
- **PMID, NCBI Gene ID, NCBI Tax ID** — positive `Int64`, never zero or negative.
- **ORCID** — canonical hyphenated `XXXX-XXXX-XXXX-XXXX`, ISO 7064 MOD 11-2 check
  digit recovered or verified; values that fail the checksum become null rather
  than propagating a wrong ID.
- **ISSN-L** — canonical hyphenated `XXXX-XXXC`, checksum verified/recovered.
- **GO ID** — `GO:` followed by 7 digits.
- **ROR ID** — canonical full URL `https://ror.org/...` (the registry's own
  canonical form).
- **Wikidata ID** — bare `Q\d+`, URL forms stripped.
- **Country code** — ISO 3166-1 alpha-2 (and alpha-3 where noted).
- **OpenAlex IDs** — short form (`W2741809807`, `A...`, `I...`, etc.), URL prefix
  stripped, to normalize and to save space across hundreds of millions of rows.

Patterns are stored unanchored on each identifier and anchored at validation
time. Empty and whitespace-only strings are normalized to null (an OpenAlex
convention applied everywhere). A pipeline that produces a value violating its
identifier's pattern fails loudly; it does not emit bad data.

## Output layout and table inventory

`<output_root>/<dataset>/<table>.parquet` with a sibling `<table>.yml`.
Column-level detail (names, identifier types, descriptions, allowed values) is
defined once in each dataset module's `SCHEMA` / `TABLES_DOC` dict and emitted
verbatim to the sidecar. **Those `*.yml` sidecars, generated from the module
`ColumnSpec`s, are the authoritative column-level schema.** This file documents
the table set and grain; it does not duplicate every column.

NCBI gene tables (pandas; all `depends_on = ncbi_gene_history`):

- `ncbi_gene_history` → `gene_history` (discontinued/replacement Gene ID map).
- `ncbi_gene_info` → `gene_info` (one row per gene).
- `ncbi_gene2go` → `gene2go` (one row per gene/GO/PubMed annotation; the source
  pipe-joined PubMed list is exploded to one PMID per row).
- `ncbi_gene2pubmed` → `gene2pubmed` (one row per gene/PubMed link).
- `ncbi_generifs` → `gene_rif` (Gene Reference into Function statements).

iCite (pandas):

- `icite` → `icite` (one row per PMID, citation metrics) and
  `open_citation_collection` (citing → referenced PMID pairs).

PubMed / MEDLINE (`ncbi_pubmed`, streaming): one parent table `articles` (one
current row per PMID) plus child tables keyed by PMID — `authors`,
`affiliations`, `mesh_headings`, `chemicals`, `publication_types`, `grants`,
`keywords`, `article_ids`, `references` — and `deleted_pmids`.

OpenAlex (21 datasets, streaming): `openalex_works` produces `works`,
`works_authorships`, `works_topics`, and `works_refs`; the other 20 entities
(`openalex_authors`, `openalex_sources`, `openalex_institutions`, the topic
hierarchy, and the controlled-vocabulary lookups) each produce a single
similarly named table.

PubTator3 (`ncbi_pubtator3`, streaming): the BioC-XML archives carry NCBI's
text-mined bio-entity annotations and concept-concept relations over PubMed and
the PMC full-text subset. Three tables, all keyed by PMID:

- `articles` — one row per PMID (document), with the article-level metadata
  PubTator carries (pmc_id, doi, year, volume/issue/pages, license) plus
  `has_full_text` and `n_passages`. Body text is intentionally not stored.
  Metadata is read only from the front/title passage, never from reference
  passages (which describe cited works). A small fraction of documents (~0.1%)
  are PMC full-text articles keyed by a `PMC…` id with no PMID; because the whole
  dataset is PMID-keyed, those are excluded and their count is reported at the end
  of `extract`.
- `annotations` — one row per tagged entity mention, with its `section_type`,
  `offset`, `length`, surface `mention`, `entity_type`, and the verbatim concept
  `identifier`. Mentions tagged inside reference-list passages are excluded.
- `relations` — one row per extracted relation, with `relation_type`, `score`,
  and the two roles split into `roleN_type` / `roleN_identifier`.

`entity_type`, `section_type`, and `relation_type` are documented free strings,
not `allowed_values`-constrained: they are machine-generated and the value set
can grow between PubTator releases, so a new value should not abort a run. The
concept `identifier` is stored exactly as PubTator emits it (heterogeneous by
type, and a packed composite for variants), so it is a plain string rather than a
single canonical identifier type.

Crosswalks (pandas; derived from already-produced ETL outputs, not from raw
snapshots):

- `pmid_openalex` → `pmid_openalex` (one row per `(pmid, work_id)` link, with
  `match_source` recording which rule produced it: `pmid`, `doi`,
  `doi_versioned`, `pmcid`, or `title_year`). `depends_on` both
  `ncbi_pubmed` and `openalex_works`; unmatched PMIDs are not in the table, so
  consumers left-join from `articles` when they need them.

Research-integrity lists (pandas; small in-memory CSV sources):

- `predatory_journals` → `predatory_journals` and `predatory_publishers` →
  `predatory_publishers` (Beall's-list-style name lists). The source is a
  headerless two-column CSV (`row number, name`); the cleaned output is a single
  `title` column, one row per distinct name.
- `retractionwatch_hijackedjournals` → `hijacked_journals` (one row per
  hijacked/clone record: `record_id`, hijacked vs. original titles and URLs) and
  `hijacked_journal_issns` (one row per `(record_id, role, issn)`; the source's
  comma-separated ISSN fields are split and normalized to canonical ISSN form,
  with `role` ∈ {`hijacked`, `original`}). The source CSV's first record is a
  donation banner, so the real header is its second record.
- `retractionwatch_retractiondatabase` → parent `retractions` (one row per
  Retraction Watch `record_id`: titles, journal/publisher, dates, retraction and
  original-paper DOIs/PMIDs, `retraction_nature`, `article_type`, `paywalled`,
  `notes`) plus child tables that explode the source's `;`-delimited fields, each
  keyed by `record_id`: `retraction_reasons`, `retraction_subjects`,
  `retraction_authors`, `retraction_countries`, `retraction_institutions`,
  `retraction_urls`. PubMed IDs use `0`/blank for "missing" (mapped to null);
  DOIs use `unavailable`/blank for "missing" and are repaired to canonical form,
  with unrecoverable values set to null. `record_id` is a positive `Int64`
  (`RetractionWatchId`), a dataset-internal key not cross-referenced elsewhere;
  a small fraction of source rows (~0.4%) carry no Record ID and, since it is the
  parent key and every child's join key, those rows are excluded with the count
  reported at transform time rather than dropped silently or given a fabricated id.

## Invariants the ETL guarantees

A consumer may rely on all of the following for any successfully produced table:

- **Identifier columns conform to their canonical form.** Validation runs before
  load; a violation aborts the run, so malformed identifiers never reach output.
- **`required` columns contain no nulls.**
- **Numeric IDs are positive `Int64`.**
- **Entrez Gene IDs are current.** Discontinued IDs are remapped to their
  replacement via `ncbi_gene_history`; an ID discontinued *without* a replacement
  is a hard error, never a silent drop. So a Gene ID in any gene output is a live
  ID, and prerequisites must be run first.
- **DOIs carry no URL prefix; ORCIDs are checksum-valid or null; OpenAlex IDs are
  short form.** (See the identifier contract.)
- **iCite PMIDs are unique**, and the output column set matches `SCHEMA` exactly
  (an unexpected source column aborts the run rather than passing through).
- **PubMed reflects "latest wins."** NLM's load order is encoded in the filename
  integer `NNNN`; every row is tagged with that `file_seq`, and transform keeps
  only each PMID's max-`file_seq` version. Child-table rows are versioned with
  their parent, so a revised article fully replaces its prior children. PMIDs
  listed in any `DeleteCitation` are anti-joined out of every table.
- **Categorical columns honor their `allowed_values`** (for example GO aspect,
  gene nomenclature status, OpenAlex authorship position).
- **Every output table has a sidecar `.yml`** describing its columns.

## Assumptions

- The latest `<version>` directory under each dataset is the one to process;
  versions sort lexicographically into chronological order (as `data_downloader`
  names them).
- Source layouts are stable per provider: NCBI tab files use `-` for null;
  OpenAlex is gzipped JSONL under `data/<entity>/updated_date=*/part_*.gz`;
  PubMed is gzipped MEDLINE XML in `baseline/` then `updatefiles/`.
- Validation is inline in the pipelines, by design: a run that completes is a
  run whose contract held. There is no separate unit-test suite asserting the
  contract; the pipelines assert it on every run.

## Changing the contract

The schema is the product. Adding, removing, renaming, or retyping an output
column, changing an identifier's canonical form, or changing a table's grain is a
**contract change** that downstream repos depend on. Update the relevant module's
`ColumnSpec`/`SCHEMA`/`TABLES_DOC` (which regenerates the sidecar) *and* this
document in the same change, and treat it as a breaking change for consumers.
