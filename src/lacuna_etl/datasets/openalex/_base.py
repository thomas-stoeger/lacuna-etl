"""
Shared base class and worker function for OpenAlex per-entity pipelines.

Each entity pipeline streams gzipped JSONL records from
  <data_root>/openalex/<latest>/data/<entity>/updated_date=*/part_*.gz
through a `transform_batch` function that returns a dict of polars DataFrames,
and writes them out as Parquet shards keyed by gz file + batch index.

Concurrency:
  - extract() drives a ProcessPoolExecutor.
  - workers per-pipeline:    OPENALEX_WORKERS    (default: 2)
  - records per batch:       OPENALEX_BATCH_SIZE (default: 50000)

Backfilling new tables:
  - restrict a run to a subset:  OPENALEX_ONLY_TABLES=works_funders,works_awards

    When a code change adds a table to an entity that has already been run, a
    full re-run would rewrite tens of GB of unchanged output just to gain it.
    With this set, transform and per-batch verification still run over the whole
    record (so the checks see every table), but only the named tables are
    written, and the restart check keys off the first *named* table instead of
    `first_table` — so list the densest one first. Sparse link tables emit no
    shard for a gz file that yields no rows, so such files are re-read on a
    subsequent restart; that repeats work but cannot produce wrong output.

Output layout:
  <output_root>/<dataset_name>/<table>/<date>__part_NNNN__batch_NNN.parquet
  <output_root>/<dataset_name>/_done/<selection>/<date>__part_NNNN

Restartability:
  A gz file yields several batches, so the presence of its batch_000 shard does
  not mean the file finished — an interrupted run would leave later batches
  unwritten and be skipped on restart, silently truncating the table. A file is
  therefore marked done only after all of its batches are written, and the
  marker is what the restart check consults. Markers are namespaced by the table
  selection, so a filtered backfill and a full run do not read each other's.
  Outputs produced before markers existed fall back to the batch_000 check.
"""

import importlib
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import yaml
from tqdm import tqdm

from lacuna_etl.config import get_data_root
from lacuna_etl.core.pipeline import DatasetPipeline
from lacuna_etl.core.schema import ColumnSpec

_DEFAULT_WORKERS = 2
_DEFAULT_BATCH_SIZE = 50_000


def _only_tables() -> tuple[str, ...] | None:
    """Tables this run is restricted to, or None when writing every table.

    Order is preserved: the first name doubles as the restart key, so put a
    densely populated table first when backfilling alongside a sparse one.
    """
    raw = os.environ.get("OPENALEX_ONLY_TABLES", "")
    selected = tuple(t.strip() for t in raw.split(",") if t.strip())
    return selected or None


def _cleaned_expr(col: str, dtype):
    """Polars expression that strips whitespace and maps empty-string -> null for string columns;
    for non-string columns, returns the column unchanged."""
    import polars as pl
    if dtype == pl.String:
        stripped = pl.col(col).str.strip_chars()
        return pl.when(stripped == "").then(None).otherwise(stripped)
    return pl.col(col)


def _selection_tag() -> str:
    """Directory name identifying which table selection a done-marker belongs to."""
    only = _only_tables()
    return "all" if only is None else "+".join(sorted(only))


def _done_marker(output_dir: Path, gz_path: Path) -> Path:
    date_part = gz_path.parent.name.removeprefix("updated_date=")
    stem = gz_path.name.removesuffix(".gz")
    return output_dir / "_done" / _selection_tag() / f"{date_part}__{stem}"


def _shard_name(gz_path: Path, batch_idx: int) -> str:
    date_part = gz_path.parent.name.removeprefix("updated_date=")
    stem = gz_path.name.removesuffix(".gz")
    return f"{date_part}__{stem}__batch_{batch_idx:03d}.parquet"


def _process_file(args: tuple[str, str, str]) -> tuple[str, int, int]:
    """Worker: process one gz file into Parquet shards for the named entity.

    args = (gz_path_str, output_dir_str, transform_module_name)
    """
    from lacuna_etl.datasets.openalex._extract import iter_batches

    gz_path_str, output_dir_str, module_name = args
    gz_path = Path(gz_path_str)
    output_dir = Path(output_dir_str)

    mod = importlib.import_module(module_name)
    transform_batch = mod.transform_batch
    verify_batch = getattr(mod, "verify_batch", None)
    batch_size = int(os.environ.get("OPENALEX_BATCH_SIZE", _DEFAULT_BATCH_SIZE))

    only = _only_tables()
    records_written = 0
    batches_written = 0
    for batch_idx, batch in enumerate(iter_batches(gz_path, batch_size)):
        tables = transform_batch(batch)
        if verify_batch is not None:
            verify_batch(tables, gz_path_str, batch_idx)

        name = _shard_name(gz_path, batch_idx)
        for table_name, df in tables.items():
            if only is not None and table_name not in only:
                continue
            if df.is_empty():
                continue
            out_dir = output_dir / table_name
            out_dir.mkdir(parents=True, exist_ok=True)
            df.write_parquet(out_dir / name, compression="snappy")

        records_written += len(batch)
        batches_written += 1

    marker = _done_marker(output_dir, gz_path)
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.touch()

    return gz_path_str, records_written, batches_written


class OpenAlexEntityPipeline(DatasetPipeline):
    """Base class for OpenAlex per-entity pipelines.

    Subclasses set:
      name              - registered dataset name (e.g. "openalex_works")
      raw_dirname       - subdir under <snapshot>/data (e.g. "works", "institution-types")
      transform_module  - module path with transform_batch() (and optional verify_batch())
      first_table       - first key returned by transform_batch (for already-done check)

    The pipeline writes Parquet shards directly to output_path() in extract();
    transform() and load() are no-ops because the snapshot is already in its
    final, immutable form (no per-snapshot validation that needs a second pass).
    """

    raw_dirname: str
    transform_module: str
    first_table: str
    tables_doc: dict[str, dict[str, ColumnSpec]] = {}

    def raw_path(self) -> Path:
        root = get_data_root() / "openalex"
        if not root.exists():
            raise FileNotFoundError(f"OpenAlex snapshot not found: {root}")
        versions = sorted(p for p in root.iterdir() if p.is_dir())
        if not versions:
            raise FileNotFoundError(f"No versions found under: {root}")
        entity_dir = versions[-1] / "data" / self.raw_dirname
        if not entity_dir.exists():
            raise FileNotFoundError(f"Entity directory not found: {entity_dir}")
        return entity_dir

    def _restart_table(self) -> str:
        """Table whose shards signal that a gz file has already been processed."""
        only = _only_tables()
        if only is None:
            return self.first_table
        if unknown := [t for t in only if t not in self.tables_doc]:
            raise ValueError(
                f"[{self.name}] OPENALEX_ONLY_TABLES names unknown table(s) "
                f"{', '.join(unknown)}; available: {', '.join(self.tables_doc)}"
            )
        return only[0]

    def _shard_done(self, gz_path: Path) -> bool:
        out_dir = self.output_path()
        if _done_marker(out_dir, gz_path).exists():
            return True
        # Fallback for outputs written before done-markers existed: those runs
        # completed, so a batch_000 shard does mean the file finished.
        marker_root = out_dir / "_done" / _selection_tag()
        if marker_root.exists():
            return False
        return (out_dir / self._restart_table() / _shard_name(gz_path, 0)).exists()

    def extract(self) -> None:
        from lacuna_etl.datasets.openalex._extract import iter_entity_files

        importlib.import_module(self.transform_module)

        entity_dir = self.raw_path()
        all_files = sorted(
            iter_entity_files(entity_dir),
            key=lambda p: p.stat().st_size,
            reverse=True,
        )
        pending = [f for f in all_files if not self._shard_done(f)]

        workers = int(os.environ.get("OPENALEX_WORKERS", _DEFAULT_WORKERS))
        batch_size = int(os.environ.get("OPENALEX_BATCH_SIZE", _DEFAULT_BATCH_SIZE))

        print(f"[{self.name}] {len(all_files)} gz files, {len(pending)} to process")
        print(f"[{self.name}] workers={workers}, batch_size={batch_size:,}")
        if (only := _only_tables()) is not None:
            print(f"[{self.name}] writing only: {', '.join(only)} "
                  f"(restart key: {self._restart_table()})")

        if not pending:
            return

        out_dir = self.output_path()
        worker_args = [(str(gz), str(out_dir), self.transform_module) for gz in pending]
        total_records = 0
        t0 = time.time()
        with ProcessPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(_process_file, a): a[0] for a in worker_args}
            with tqdm(total=len(pending), unit="file") as pbar:
                for future in as_completed(futures):
                    gz_str = futures[future]
                    try:
                        _, n_records, _ = future.result()
                        total_records += n_records
                    except Exception as e:
                        print(f"\nERROR [{gz_str}]: {e}", file=sys.stderr)
                        raise
                    finally:
                        pbar.update(1)

        elapsed = time.time() - t0
        rate = total_records / elapsed if elapsed > 0 else 0
        print(f"[{self.name}] {total_records:,} records in {elapsed:.1f}s ({rate:.0f} rec/s)")

    def transform(self) -> None:
        """Validate identifier columns across all written shards, processing one shard at a time.

        For each table in `tables_doc`, iterates parquet shards individually so peak memory
        is bounded by the largest single shard (~500 MB at our batch size), not by the whole
        table. Per shard, only the columns under validation are read, then null- and
        pattern-mismatch counts are accumulated. Raises on the first failing column with up
        to 5 sample bad values from the shard where they first appeared.
        """
        import polars as pl
        from tqdm import tqdm

        out_dir = self.output_path()
        if not self.tables_doc:
            return

        only = _only_tables()
        for table_name, doc in self.tables_doc.items():
            if only is not None and table_name not in only:
                continue
            table_dir = out_dir / table_name
            shards = sorted(table_dir.glob("*.parquet")) if table_dir.exists() else []
            if not shards:
                print(f"[{self.name}] no shards for {table_name}; skipping validation")
                continue

            checks = [(c, spec) for c, spec in doc.items()
                      if spec.identifier is not None or spec.allowed_values is not None or spec.required]
            if not checks:
                continue

            schema = pl.scan_parquet(shards[0]).collect_schema()
            checks = [(c, spec) for c, spec in checks if c in schema]
            if not checks:
                continue

            check_cols = [c for c, _ in checks]
            total_rows = 0
            null_counts: dict[str, int] = {c: 0 for c in check_cols}
            bad_counts:  dict[str, int] = {c: 0 for c, spec in checks
                                            if spec.identifier is not None and spec.identifier.pattern is not None}
            bad_samples: dict[str, list] = {}

            for shard in tqdm(shards, desc=f"[{self.name}] validate {table_name}", unit="shard"):
                df = pl.read_parquet(shard, columns=check_cols)
                total_rows += df.height
                for col, spec in checks:
                    s = df[col]
                    if s.dtype == pl.String:
                        s = s.str.strip_chars()
                        s = s.set(s == "", None)
                    null_counts[col] += s.null_count()
                    pattern = spec.identifier.pattern if spec.identifier is not None else None
                    if pattern is None:
                        continue
                    non_null = s.drop_nulls()
                    if non_null.len() == 0:
                        continue
                    bad_mask = ~non_null.str.contains(rf"^(?:{pattern})$")
                    n_bad = int(bad_mask.sum())
                    if n_bad:
                        bad_counts[col] += n_bad
                        if col not in bad_samples:
                            bad_samples[col] = non_null.filter(bad_mask).head(5).to_list()
                del df

            for col, spec in checks:
                if spec.required and null_counts[col] > 0:
                    raise ValueError(
                        f"[{self.name}/{table_name}] {col}: {null_counts[col]} nulls (required)"
                    )
            for col, n_bad in bad_counts.items():
                if n_bad > 0:
                    spec = dict(checks)[col]
                    raise ValueError(
                        f"[{self.name}/{table_name}] {col}: {n_bad} values not matching "
                        f"r'{spec.identifier.pattern}' ({spec.identifier.__name__}), "
                        f"e.g. {bad_samples.get(col)}"
                    )

            print(f"[{self.name}] validated {len(checks)} cols on {table_name} ({total_rows:,} rows)")

    def load(self) -> None:
        """Write per-table schema yaml files documenting columns and identifier types."""
        out_dir = self.output_path()
        for table_name, doc in self.tables_doc.items():
            data = {col: spec.yaml_entry() for col, spec in doc.items()}
            (out_dir / f"{table_name}.yml").write_text(
                yaml.dump(data, sort_keys=False, allow_unicode=True)
            )
