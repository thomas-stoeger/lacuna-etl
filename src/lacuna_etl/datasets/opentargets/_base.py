"""
Shared base class and worker for Open Targets per-product pipelines.

Open Targets ships each data product as a directory of Parquet part files:
  <data_root>/opentargets/<version>/<product>/part-*.parquet

Each part file is processed independently into per-table Parquet shards, so a run
is restartable (a shard-exists check skips already-processed inputs) and peak
memory is bounded by one part file rather than the whole product. Unlike OpenAlex
(gzipped JSONL parsed row-by-row), the inputs are already Parquet, so the transform
is columnar: each product module exposes `transform_file(df)` that unnests structs
and explodes list columns with Polars and returns {table_name: polars.DataFrame}.

Concurrency:
  - extract() drives a ProcessPoolExecutor.
  - workers:  OPENTARGETS_WORKERS (default: 4)

Output layout (matches OpenAlex):
  <output_root>/<dataset_name>/<table>/<input_filename>
  <output_root>/<dataset_name>/<table>.yml
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

_DEFAULT_WORKERS = 4


def _process_file(args: tuple[str, str, str]) -> tuple[str, int]:
    """Worker: read one Parquet part file, transform it, write per-table shards.

    args = (input_path_str, output_dir_str, transform_module_name)
    """
    import polars as pl

    path_str, out_dir_str, module_name = args
    path = Path(path_str)
    out_dir = Path(out_dir_str)

    mod = importlib.import_module(module_name)
    df = pl.read_parquet(path)
    tables = mod.transform_file(df)

    for table_name, tdf in tables.items():
        if tdf.is_empty():
            continue
        table_dir = out_dir / table_name
        table_dir.mkdir(parents=True, exist_ok=True)
        tdf.write_parquet(table_dir / path.name, compression="snappy")

    return path_str, df.height


class OpenTargetsProductPipeline(DatasetPipeline):
    """Base class for Open Targets per-product pipelines.

    Subclasses set:
      name              - registered dataset name (e.g. "opentargets_target")
      raw_dirname       - product subdir under the snapshot (e.g. "target")
      transform_module  - module path exposing transform_file(df) -> dict[str, pl.DataFrame]
      first_table       - a table every non-empty input produces (for the already-done check)
      tables_doc        - {table_name: {column: ColumnSpec}} driving validation and sidecars

    The snapshot is already in final, immutable Parquet form, so extract() writes the
    shards, transform() validates identifier/required columns across them, and load()
    writes the sidecars.
    """

    raw_dirname: str
    transform_module: str
    first_table: str
    tables_doc: dict[str, dict[str, ColumnSpec]] = {}

    def raw_path(self) -> Path:
        root = get_data_root() / "opentargets"
        if not root.exists():
            raise FileNotFoundError(f"Open Targets snapshot not found: {root}")
        versions = sorted(p for p in root.iterdir() if p.is_dir())
        if not versions:
            raise FileNotFoundError(f"No versions found under: {root}")
        product_dir = versions[-1] / self.raw_dirname
        if not product_dir.exists():
            raise FileNotFoundError(f"Product directory not found: {product_dir}")
        return product_dir

    def _input_files(self) -> list[Path]:
        return sorted(self.raw_path().glob("*.parquet"))

    def _shard_done(self, path: Path) -> bool:
        return (self.output_path() / self.first_table / path.name).exists()

    def extract(self) -> None:
        importlib.import_module(self.transform_module)

        all_files = sorted(self._input_files(), key=lambda p: p.stat().st_size, reverse=True)
        pending = [f for f in all_files if not self._shard_done(f)]

        workers = int(os.environ.get("OPENTARGETS_WORKERS", _DEFAULT_WORKERS))
        print(f"[{self.name}] {len(all_files)} part files, {len(pending)} to process")
        print(f"[{self.name}] workers={workers}")

        if not pending:
            return

        out_dir = self.output_path()
        worker_args = [(str(f), str(out_dir), self.transform_module) for f in pending]
        total_records = 0
        t0 = time.time()
        if workers <= 1:
            # Serial in-process path: avoids ProcessPoolExecutor entirely, which
            # some sandboxes block (POSIX-semaphore syscalls denied at pool init).
            with tqdm(total=len(pending), unit="file") as pbar:
                for a in worker_args:
                    try:
                        _, n_records = _process_file(a)
                        total_records += n_records
                    except Exception as e:
                        print(f"\nERROR [{a[0]}]: {e}", file=sys.stderr)
                        raise
                    finally:
                        pbar.update(1)
        else:
            with ProcessPoolExecutor(max_workers=workers) as pool:
                futures = {pool.submit(_process_file, a): a[0] for a in worker_args}
                with tqdm(total=len(pending), unit="file") as pbar:
                    for future in as_completed(futures):
                        src = futures[future]
                        try:
                            _, n_records = future.result()
                            total_records += n_records
                        except Exception as e:
                            print(f"\nERROR [{src}]: {e}", file=sys.stderr)
                            raise
                        finally:
                            pbar.update(1)

        elapsed = time.time() - t0
        rate = total_records / elapsed if elapsed > 0 else 0
        print(f"[{self.name}] {total_records:,} source rows in {elapsed:.1f}s ({rate:.0f} row/s)")

    def transform(self) -> None:
        """Validate identifier / required columns across all written shards.

        Reads one shard at a time, accumulating null counts and pattern-mismatch counts
        so peak memory is bounded by a single shard. Raises on the first failing column
        with up to 5 sample bad values.
        """
        import polars as pl

        out_dir = self.output_path()
        if not self.tables_doc:
            return

        for table_name, doc in self.tables_doc.items():
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
            bad_counts: dict[str, int] = {c: 0 for c, spec in checks
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
        out_dir = self.output_path()
        for table_name, doc in self.tables_doc.items():
            data = {col: spec.yaml_entry() for col, spec in doc.items()}
            (out_dir / f"{table_name}.yml").write_text(
                yaml.dump(data, sort_keys=False, allow_unicode=True)
            )
