"""ETL pipeline for the PubTator3 BioC-XML annotation archives.

Layout under `data_root / ncbi_pubtator3`:
  YYYY-MM-DD/BioCXML.[0-9].tar.gz        10 archives, ~21 GB each

Each archive is an independent, immutable slice of the corpus (PubTator shards by
PMID, so a PMID lives in exactly one archive). We process the archives in parallel,
one per worker. Because a single archive holds millions of documents, a worker
cannot buffer a whole archive in memory, so it flushes per-table parquet shards
once it has buffered `_FLUSH_ROWS` annotation rows (with `_FLUSH_EVERY` documents
as a backstop) — bounding on rows keeps peak memory flat despite wide variation
in annotations-per-document.

Stages:
  extract()   streams each archive once (parallel workers) into per-table parquet
              shards under `intermediate_path()/<table>/arch<i>_partNNN.parquet`.
              A marker `_done/arch<i>.done` lets re-runs skip finished archives;
              an unfinished archive's stale shards are cleared before reprocessing.
  transform() concatenates the shards per table to `output_path()/<table>.parquet`
              (articles deduplicated on pmid) and validates identifier columns.
  load()      writes a side-car schema yaml per table.
"""

from __future__ import annotations

import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import polars as pl
import yaml
from tqdm import tqdm

from lacuna_etl.core.identifiers import doi_base_expr, doi_versioned_expr
from lacuna_etl.core.pipeline import DatasetPipeline
from lacuna_etl.datasets.pubtator3._parse import iter_documents, parse_document
from lacuna_etl.datasets.pubtator3._schemas import POLARS_SCHEMAS, TABLES_DOC
from lacuna_etl.datasets.registry import register

_DEFAULT_WORKERS = 5
_DONE_DIR = "_done"

# Flush accumulated rows to a shard once a worker has buffered this many annotation
# rows. Annotation density varies ~25x (a full-text PMC document can carry thousands
# of mentions vs tens for an abstract), so bounding on a document count leaves peak
# memory unbounded — a 50k-doc batch on a dense stretch reached 13M rows (~GBs) and
# could OOM-kill the run. Bounding on row count keeps each worker's buffer flat.
_FLUSH_ROWS = 1_500_000

# Backstop flush by document count, so sparse (abstract-only) stretches still write
# shards at a steady cadence even when the row threshold is slow to fill.
_FLUSH_EVERY = 50_000


def _archive_index(tar_path: Path) -> int:
    """Extract the integer N from a `BioCXML.N.tar.gz` filename."""
    return int(tar_path.name.split(".")[1])


def _write_shard(rows: list[dict], table: str, out_dir: Path, stem: str) -> None:
    if not rows:
        return
    out_dir.mkdir(parents=True, exist_ok=True)
    df = pl.DataFrame(rows, schema=POLARS_SCHEMAS[table])
    df.write_parquet(out_dir / f"{stem}.parquet", compression="snappy")


def _process_archive(args: tuple[str, str]) -> tuple[str, int, int, int]:
    """Parse one BioCXML tar.gz into per-table parquet shards.

    Returns (archive_path, n_articles, n_annotations, n_skipped) for progress
    reporting. `n_skipped` counts documents dropped for lacking a PMID.
    """
    tar_path_str, intermediate_dir_str = args
    tar_path = Path(tar_path_str)
    intermediate_dir = Path(intermediate_dir_str)
    arch = _archive_index(tar_path)

    # Clear any shards left from a previously interrupted run of this archive so
    # resume stays consistent (part numbering restarts from 0).
    for table in POLARS_SCHEMAS:
        for stale in (intermediate_dir / table).glob(f"arch{arch}_part*.parquet"):
            stale.unlink()

    buffers: dict[str, list[dict]] = {table: [] for table in POLARS_SCHEMAS}
    part = 0
    n_seen = 0
    n_articles = 0
    n_annotations = 0

    def flush() -> None:
        nonlocal part
        stem = f"arch{arch}_part{part:04d}"
        for table, rows in buffers.items():
            _write_shard(rows, table, intermediate_dir / table, stem)
            rows.clear()
        part += 1

    for doc in iter_documents(tar_path):
        parsed = parse_document(doc)
        for table, table_rows in parsed.items():
            buffers[table].extend(table_rows)
        n_seen += 1
        n_articles += len(parsed["articles"])
        n_annotations += len(parsed["annotations"])
        if len(buffers["annotations"]) >= _FLUSH_ROWS or n_seen % _FLUSH_EVERY == 0:
            flush()

    flush()  # remaining rows (and ensures an empty archive still completes)

    done_dir = intermediate_dir / _DONE_DIR
    done_dir.mkdir(parents=True, exist_ok=True)
    (done_dir / f"arch{arch}.done").touch()

    return tar_path_str, n_articles, n_annotations, n_seen - n_articles


@register
class Pubtator3(DatasetPipeline):
    """ETL for PubTator3 BioC-XML annotation archives."""

    name = "ncbi_pubtator3"

    # ---- extract ----------------------------------------------------------

    def _archives(self) -> list[Path]:
        version = self.raw_path()
        return sorted(version.glob("BioCXML.[0-9].tar.gz"), key=_archive_index)

    def _is_done(self, tar_path: Path) -> bool:
        return (self.intermediate_path() / _DONE_DIR / f"arch{_archive_index(tar_path)}.done").exists()

    def extract(self) -> None:
        all_archives = self._archives()
        if not all_archives:
            raise FileNotFoundError(f"[{self.name}] no BioCXML.[0-9].tar.gz under {self.raw_path()}")
        pending = [p for p in all_archives if not self._is_done(p)]

        workers = int(os.environ.get("PUBTATOR_WORKERS", _DEFAULT_WORKERS))
        print(f"[{self.name}] {len(all_archives)} archives, {len(pending)} to process")
        print(f"[{self.name}] workers={workers}")

        if not pending:
            return

        worker_args = [(str(tar), str(self.intermediate_path())) for tar in pending]
        total_articles = 0
        total_annotations = 0
        total_skipped = 0
        t0 = time.time()
        with ProcessPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(_process_archive, a): a[0] for a in worker_args}
            with tqdm(total=len(pending), unit="archive") as pbar:
                for future in as_completed(futures):
                    tar_str = futures[future]
                    try:
                        _, n_articles, n_annotations, n_skipped = future.result()
                        total_articles += n_articles
                        total_annotations += n_annotations
                        total_skipped += n_skipped
                    except Exception as e:
                        print(f"\nERROR [{tar_str}]: {e}", file=sys.stderr)
                        raise
                    finally:
                        pbar.update(1)

        elapsed = time.time() - t0
        rate = total_articles / elapsed if elapsed > 0 else 0
        print(
            f"[{self.name}] extracted {total_articles:,} documents, "
            f"{total_annotations:,} annotations in {elapsed:.1f}s ({rate:.0f} docs/s)"
        )
        if total_skipped:
            print(f"[{self.name}] skipped {total_skipped:,} documents with no PMID (PMC-only full text)")

    # ---- transform --------------------------------------------------------

    def _shards(self, table: str) -> list[Path]:
        d = self.intermediate_path() / table
        return sorted(d.glob("arch*_part*.parquet")) if d.exists() else []

    def transform(self) -> None:
        out_root = self.output_path()

        article_shards = self._shards("articles")
        if not article_shards:
            print(f"[{self.name}] no article shards; skipping transform")
            return

        # PubTator shards by PMID, so a PMID should appear exactly once. `unique`
        # is a safety net against a PMID landing in two archives.
        # Split publisher-versioned DOIs: doi keeps the article-level base,
        # doi_versioned keeps the original versioned form (null when unversioned).
        articles_lf = (
            pl.scan_parquet(article_shards)
            .unique(subset="pmid")
            .with_columns(
                doi_base_expr("doi").alias("doi"),
                doi_versioned_expr("doi").alias("doi_versioned"),
            )
        )
        articles_lf.sink_parquet(out_root / "articles.parquet", compression="snappy")

        n_shard_rows = pl.scan_parquet(article_shards).select(pl.len()).collect().item()
        n_unique = pl.scan_parquet(out_root / "articles.parquet").select(pl.len()).collect().item()
        if n_unique != n_shard_rows:
            print(f"[{self.name}] articles: dropped {n_shard_rows - n_unique:,} duplicate-PMID rows")

        for table in ("annotations", "relations"):
            shards = self._shards(table)
            if not shards:
                continue
            pl.scan_parquet(shards).sink_parquet(out_root / f"{table}.parquet", compression="snappy")

        self._validate_outputs()

    def _validate_outputs(self) -> None:
        """Run each table's ColumnSpec checks against the consolidated output."""
        out_root = self.output_path()
        for table, doc in TABLES_DOC.items():
            path = out_root / f"{table}.parquet"
            if not path.exists():
                continue
            check_cols = [
                c for c, spec in doc.items()
                if (spec.identifier is not None or spec.allowed_values is not None or spec.required)
            ]
            if not check_cols:
                continue
            lf = pl.scan_parquet(path)
            available = set(lf.collect_schema().names())
            check_cols = [c for c in check_cols if c in available]
            if not check_cols:
                continue
            df = lf.select(check_cols).collect()
            for col in check_cols:
                doc[col].validate_polars(df[col])
            print(f"[{self.name}] validated {len(check_cols)} cols on {table} ({df.height:,} rows)")

    # ---- load -------------------------------------------------------------

    def load(self) -> None:
        out_root = self.output_path()
        for table, doc in TABLES_DOC.items():
            path = out_root / f"{table}.parquet"
            if not path.exists():
                continue
            data = {col: spec.yaml_entry() for col, spec in doc.items()}
            (out_root / f"{table}.yml").write_text(
                yaml.dump(data, sort_keys=False, allow_unicode=True)
            )
