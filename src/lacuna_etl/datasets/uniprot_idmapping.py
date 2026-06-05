"""UniProt ID mapping — the UniProtKB cross-reference flat file (`idmapping.dat`).

The snapshot is a single ~20 GB gzip (`idmapping.dat.gz`) of the all-species
`idmapping.dat`: a plain three-column TSV, one row per cross-reference,
``<UniProtKB accession>  <id_type>  <value>`` (grouped by accession). Uncompressed
it is billions of rows, far too large for memory, and gzip is not seekable, so this
is a streaming pipeline: ``extract`` decompresses the file once (piped through the
system ``gzip``) and writes Parquet shards directly to the output, OpenAlex-style
(``uniprot_idmapping/id_mappings/part_NNNNN.parquet`` + an ``id_mappings.yml``
sidecar). ``transform`` validates the identifier column one shard at a time so peak
memory is bounded by a single shard; ``load`` writes the sidecar.

Restartability: shards are fixed-size (``_FLUSH_ROWS`` rows each) and written
atomically (temp file + rename). A re-run drops the last (possibly partial) shard,
fast-forwards past the rows already covered by the surviving shards, and resumes —
the gzip is re-scanned from the start (unavoidable) but already-written rows are not
re-emitted. A ``_SUCCESS`` marker lets a finished extract be skipped entirely.

Schema. The grain is one row per source line. UniProt's first column carries isoform
accessions (e.g. ``P48347-2``) alongside canonical ones, which are not valid base
accessions, so — following the repo's ``doi`` / ``doi_versioned`` split — the base
accession is typed ``UniprotAccession`` in ``uniprot_accession`` and the full isoform
form is preserved in a sibling ``isoform`` column (null for canonical rows). ``id_type``
(GI, RefSeq, GeneID, Ensembl, KEGG, PDB, HGNC, NCBI_TaxID, …; ~100 values) and
``value`` are documented plain strings: the mapped-database set grows between releases
and the values are heterogeneous by type (the PubTator / Open Targets precedent), so a
new database in a future release does not abort a run.
"""
from __future__ import annotations

import io
import os
import subprocess
import time
from pathlib import Path

import polars as pl

from lacuna_etl.core.identifiers import UniprotAccession
from lacuna_etl.core.pipeline import DatasetPipeline
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.registry import register

_TABLE = "id_mappings"
_SUCCESS = "_SUCCESS"

# Rows per output shard. ~5M rows of three short strings is a ~100-200 MB parquet
# shard and a bounded read for per-shard validation.
_FLUSH_ROWS = 5_000_000

POLARS_SCHEMA: dict[str, pl.DataType] = {
    "uniprot_accession": pl.String,
    "isoform": pl.String,
    "id_type": pl.String,
    "value": pl.String,
}

SCHEMA: dict[str, ColumnSpec] = {
    "uniprot_accession": ColumnSpec(identifier=UniprotAccession, required=True, description="UniProtKB accession; the base (canonical) accession — any isoform suffix is moved to the isoform column"),
    "isoform": ColumnSpec(description="Full isoform accession (e.g. 'P48347-2') when the source row maps a specific isoform; null for a canonical-accession row"),
    "id_type": ColumnSpec(required=True, description="Cross-referenced database / identifier type (idmapping.dat column 2: GI, EMBL, RefSeq, GeneID, Ensembl, KEGG, PDB, STRING, HGNC, MGI, NCBI_TaxID, …; a documented free string — UniProt's mapped-database set grows between releases)"),
    "value": ColumnSpec(required=True, description="The cross-reference value in that database (heterogeneous by id_type; plain string)"),
}


def _process_batch(blob: bytes) -> pl.DataFrame:
    """Parse one batch of raw TSV bytes into the output schema (isoform split out)."""
    df = pl.read_csv(
        io.BytesIO(blob),
        separator="\t",
        has_header=False,
        quote_char=None,  # idmapping.dat is unquoted; a bare " is literal data
        new_columns=["acc", "id_type", "value"],
        infer_schema_length=0,  # force all columns to String
    )
    acc = pl.col("acc")
    return df.with_columns(
        # isoform suffix is '-N'; UniProt accessions never contain '-' otherwise.
        pl.when(acc.str.contains("-", literal=True)).then(acc).otherwise(None).alias("isoform"),
        acc.str.splitn("-", 2).struct.field("field_0").alias("uniprot_accession"),
    ).select(list(POLARS_SCHEMA))


@register
class UniprotIdmapping(DatasetPipeline):
    name = "uniprot_idmapping"

    def _dat_gz(self) -> Path:
        matches = sorted(self.raw_path().glob("idmapping.dat.gz"))
        if len(matches) != 1:
            raise FileNotFoundError(
                f"[{self.name}] expected exactly one idmapping.dat.gz under {self.raw_path()}, found {len(matches)}"
            )
        return matches[0]

    def _table_dir(self) -> Path:
        d = self.output_path() / _TABLE
        d.mkdir(parents=True, exist_ok=True)
        return d

    # ---- extract ----------------------------------------------------------

    def extract(self) -> None:
        out_dir = self._table_dir()
        if (out_dir / _SUCCESS).exists():
            print(f"[{self.name}] extract already complete ({_SUCCESS} present); skipping")
            return

        # Resume: keep all but the last shard (which may be a partial final flush),
        # fast-forward past the rows they already cover, and continue numbering.
        existing = sorted(out_dir.glob("part_*.parquet"))
        n_keep = max(0, len(existing) - 1)
        for stale in existing[n_keep:]:
            stale.unlink()
        skip_rows = n_keep * _FLUSH_ROWS
        shard_idx = n_keep
        max_shards = int(os.environ.get("UNIPROT_IDMAPPING_MAX_SHARDS", "0")) or None

        dat_gz = self._dat_gz()
        print(f"[{self.name}] streaming {dat_gz.name}; resuming at shard {shard_idx} (skip {skip_rows:,} rows)")

        n_rows = 0
        t0 = time.time()
        proc = subprocess.Popen(["gzip", "-dc", str(dat_gz)], stdout=subprocess.PIPE, bufsize=1 << 20)
        assert proc.stdout is not None

        def write_shard(lines: list[bytes]) -> int:
            nonlocal shard_idx
            df = _process_batch(b"".join(lines))
            tmp = out_dir / f".part_{shard_idx:05d}.tmp"
            df.write_parquet(tmp, compression="snappy")
            os.replace(tmp, out_dir / f"part_{shard_idx:05d}.parquet")
            shard_idx += 1
            return df.height

        try:
            stdout = proc.stdout
            # Fast-forward already-covered rows without parsing them.
            for _ in range(skip_rows):
                if stdout.readline() == b"":
                    break

            buf: list[bytes] = []
            for line in stdout:
                buf.append(line)
                if len(buf) >= _FLUSH_ROWS:
                    n_rows += write_shard(buf)
                    buf = []
                    if max_shards and (shard_idx - n_keep) >= max_shards:
                        buf = []
                        break
            else:
                if buf:
                    n_rows += write_shard(buf)
                    buf = []
                (out_dir / _SUCCESS).touch()
        finally:
            if proc.poll() is None:
                proc.terminate()
            proc.wait()

        elapsed = time.time() - t0
        rate = n_rows / elapsed if elapsed > 0 else 0
        capped = " (capped)" if max_shards else ""
        print(f"[{self.name}] wrote {n_rows:,} rows to shards {n_keep}..{shard_idx - 1} in {elapsed:.1f}s ({rate:.0f} rows/s){capped}")

    # ---- transform --------------------------------------------------------

    def _shards(self) -> list[Path]:
        d = self.output_path() / _TABLE
        return sorted(d.glob("part_*.parquet")) if d.exists() else []

    def transform(self) -> None:
        # Faithful one-row-per-source-line projection; extract already wrote the final
        # shards. Validate the identifier / required columns one shard at a time so
        # peak memory stays bounded by a single shard (the OpenAlex idiom).
        shards = self._shards()
        if not shards:
            print(f"[{self.name}] no shards to validate")
            return
        check_cols = [c for c, spec in SCHEMA.items() if spec.identifier is not None or spec.required]
        total = 0
        for shard in shards:
            df = pl.read_parquet(shard, columns=check_cols)
            for col in check_cols:
                SCHEMA[col].validate_polars(df[col])
            total += df.height
        print(f"[{self.name}] validated {len(check_cols)} cols across {len(shards)} shards ({total:,} rows)")

    # ---- load -------------------------------------------------------------

    def load(self) -> None:
        self.save_schema_yaml(SCHEMA, _TABLE)
