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

Output layout:
  <output_root>/<dataset_name>/<table>/<date>__part_NNNN__batch_NNN.parquet
"""

import importlib
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

from tqdm import tqdm

from lacuna_etl.config import get_data_root
from lacuna_etl.core.pipeline import DatasetPipeline

_DEFAULT_WORKERS = 2
_DEFAULT_BATCH_SIZE = 50_000


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

    records_written = 0
    batches_written = 0
    for batch_idx, batch in enumerate(iter_batches(gz_path, batch_size)):
        tables = transform_batch(batch)
        if verify_batch is not None:
            verify_batch(tables, gz_path_str, batch_idx)

        name = _shard_name(gz_path, batch_idx)
        for table_name, df in tables.items():
            if df.is_empty():
                continue
            out_dir = output_dir / table_name
            out_dir.mkdir(parents=True, exist_ok=True)
            df.write_parquet(out_dir / name, compression="snappy")

        records_written += len(batch)
        batches_written += 1

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

    def _shard_done(self, gz_path: Path) -> bool:
        return (self.output_path() / self.first_table / _shard_name(gz_path, 0)).exists()

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
        pass

    def load(self) -> None:
        pass
