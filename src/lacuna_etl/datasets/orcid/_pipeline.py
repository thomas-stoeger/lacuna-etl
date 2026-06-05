"""ETL pipeline for the ORCID Public Data File (summaries).

Layout under ``data_root / orcid``:
  <release>/ORCID_<release>_summaries.tar.gz   one ~43 GB gzip, ~20M record XMLs

Unlike PubMed / PubTator (many independent archives processed by parallel workers),
ORCID ships as a *single* non-seekable gzip, so extraction is one streaming pass.
Restartability and bounded memory come from sharding on the archive's own 000–999
directory bucket: records stream in directory order, and at each directory boundary
the buffered rows are flushed to per-table shards
``intermediate/<table>/<NNN>.parquet`` and a marker ``_done/<NNN>.done`` is written.
A re-run skips buckets whose marker exists (reading but not parsing them, since gzip
must still be scanned from the start) and clears any half-written shards of an
interrupted bucket before redoing it. Peak memory is bounded by one bucket
(~20k records).

Set ``ORCID_MAX_BUCKETS`` to process only the first N pending buckets (used to smoke-
test on a subset without the full multi-hour pass).

Stages:
  extract()   single streaming pass -> per-table parquet shards per directory bucket
  transform() concatenate shards per table to ``output/<table>.parquet`` and validate
              identifier / required / categorical columns
  load()      write a side-car schema yaml per table
"""
from __future__ import annotations

import os
import time
from pathlib import Path

import polars as pl
import yaml
from tqdm import tqdm

from lacuna_etl.core.pipeline import DatasetPipeline
from lacuna_etl.datasets.orcid._parse import iter_members, parse_bytes, parse_record
from lacuna_etl.datasets.orcid._schemas import POLARS_SCHEMAS, TABLES_DOC
from lacuna_etl.datasets.registry import register

_DONE_DIR = "_done"


def _write_shard(rows: list[dict], table: str, out_dir: Path, prefix: str) -> None:
    if not rows:
        return
    out_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows, schema=POLARS_SCHEMAS[table]).write_parquet(
        out_dir / f"{prefix}.parquet", compression="snappy"
    )


@register
class Orcid(DatasetPipeline):
    name = "orcid"

    # ---- extract ----------------------------------------------------------

    def _summaries_tar(self) -> Path:
        matches = sorted(self.raw_path().glob("*_summaries.tar.gz"))
        if len(matches) != 1:
            raise FileNotFoundError(
                f"[{self.name}] expected exactly one *_summaries.tar.gz under {self.raw_path()}, found {len(matches)}"
            )
        return matches[0]

    def extract(self) -> None:
        tar_path = self._summaries_tar()
        inter = self.intermediate_path()
        done_dir = inter / _DONE_DIR
        done_dir.mkdir(parents=True, exist_ok=True)
        done = {p.stem for p in done_dir.glob("*.done")}
        max_buckets = int(os.environ.get("ORCID_MAX_BUCKETS", "0")) or None

        print(f"[{self.name}] streaming {tar_path.name}; {len(done)} buckets already done")

        buffers: dict[str, list[dict]] = {t: [] for t in POLARS_SCHEMAS}
        cur_prefix: str | None = None
        new_buckets = 0
        n_records = 0
        n_skipped = 0
        t0 = time.time()

        def flush(prefix: str) -> None:
            for table, rows in buffers.items():
                _write_shard(rows, table, inter / table, prefix)
                rows.clear()
            (done_dir / f"{prefix}.done").touch()

        pbar = tqdm(unit="rec")
        for prefix, data in iter_members(tar_path):
            if prefix in done:
                continue  # already processed; gzip is still scanned but not parsed
            if prefix != cur_prefix:
                if cur_prefix is not None:
                    flush(cur_prefix)
                    new_buckets += 1
                    if max_buckets and new_buckets >= max_buckets:
                        cur_prefix = None
                        break
                cur_prefix = prefix
                # Clear any shards left from an interrupted previous run of this bucket.
                for table in POLARS_SCHEMAS:
                    stale = inter / table / f"{prefix}.parquet"
                    if stale.exists():
                        stale.unlink()

            root = parse_bytes(data)
            parsed = parse_record(root) if root is not None else None
            if parsed is None:
                n_skipped += 1
                continue
            for table, rows in parsed.items():
                buffers[table].extend(rows)
            n_records += 1
            pbar.update(1)

        if cur_prefix is not None:
            flush(cur_prefix)
            new_buckets += 1
        pbar.close()

        elapsed = time.time() - t0
        rate = n_records / elapsed if elapsed > 0 else 0
        print(
            f"[{self.name}] extracted {n_records:,} records across {new_buckets} new buckets "
            f"in {elapsed:.1f}s ({rate:.0f} rec/s)"
        )
        if n_skipped:
            print(f"[{self.name}] skipped {n_skipped:,} documents with no parseable ORCID record")

    # ---- transform --------------------------------------------------------

    def _shards(self, table: str) -> list[Path]:
        # One shard per directory bucket. Buckets are 000–999 *and* 00X–99X (ORCID
        # iDs whose checksum digit is 'X'), so match every shard, not just numeric.
        d = self.intermediate_path() / table
        return sorted(d.glob("*.parquet")) if d.exists() else []

    def transform(self) -> None:
        out_root = self.output_path()
        for table in POLARS_SCHEMAS:
            shards = self._shards(table)
            if not shards:
                print(f"[{self.name}] no shards for {table}; skipping")
                continue
            pl.scan_parquet(shards).sink_parquet(out_root / f"{table}.parquet", compression="snappy")
        self._validate_outputs()

    def _validate_outputs(self) -> None:
        out_root = self.output_path()
        for table, doc in TABLES_DOC.items():
            path = out_root / f"{table}.parquet"
            if not path.exists():
                continue
            check_cols = [
                c for c, spec in doc.items()
                if spec.identifier is not None or spec.allowed_values is not None or spec.required
            ]
            if not check_cols:
                continue
            df = pl.scan_parquet(path).select(check_cols).collect()
            for col in check_cols:
                doc[col].validate_polars(df[col])
            print(f"[{self.name}] validated {len(check_cols)} cols on {table} ({df.height:,} rows)")

    # ---- load -------------------------------------------------------------

    def load(self) -> None:
        out_root = self.output_path()
        for table, doc in TABLES_DOC.items():
            if not (out_root / f"{table}.parquet").exists():
                continue
            data = {col: spec.yaml_entry() for col, spec in doc.items()}
            (out_root / f"{table}.yml").write_text(yaml.dump(data, sort_keys=False, allow_unicode=True))
