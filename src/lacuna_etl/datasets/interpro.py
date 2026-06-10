"""InterPro — protein families, domains, and sites, and their protein matches.

InterPro ships small reference files (the entry list, the parent/child tree, the
InterPro→GO mapping) plus the very large ``protein2ipr.dat.gz`` (~17 GB compressed,
~1.18 billion rows) that records every protein's matched InterPro entries with their
positions. The reference files are in-memory pandas; the protein-match file is far too
large for memory and gzip is not seekable, so it is streamed: ``extract`` builds the
three small tables to the intermediate directory and streams ``protein2ipr`` block by
block (Python gzip → polars chunk parse) into sharded parquet written directly to the
output (the OpenAlex large-table convention), restartable via a ``_SUCCESS`` marker;
``transform`` is a no-op; ``load`` types/validates/writes the small tables and the
protein-match sidecar.

The grain key is the InterPro entry accession (``InterProId``, ``IPR\\d{6}``), which
keys ``entries``, both sides of ``entry_parents``, and ``entry2go`` (whose ``go_id`` is
``GoId``). In ``protein2entry`` (one row per protein-region match) the InterPro id is
typed; ``uniprot_accession`` is kept a documented plain string (the file spans
reviewed and unreviewed UniProtKB and validating ~1.18B values mid-stream is neither
cheap nor safe), and ``member_db_id`` (the signature's member-database accession,
PF/TIGR/G3DSA/SSF/…) stays a plain string. See docs/DESIGN.md for the table inventory.
"""
from __future__ import annotations

import gzip
import io
import re
import shutil
from pathlib import Path

import pandas as pd
import polars as pl
import yaml

from lacuna_etl.core.identifiers import GoId, InterProId
from lacuna_etl.core.pipeline import DatasetPipeline
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.registry import register

# ---- small reference tables (pandas) --------------------------------------
ENTRIES_SCHEMA = {
    "interpro_id": ColumnSpec(identifier=InterProId, required=True, description="InterPro entry accession (IPR…); the grain key"),
    "entry_type": ColumnSpec(required=True, description="Entry type (e.g. Family, Domain, Active_site, Binding_site, Conserved_site, Repeat, Homologous_superfamily, PTM); a documented free string"),
    "name": ColumnSpec(required=True, description="Entry full name (the entry.list ENTRY_NAME)"),
    "short_name": ColumnSpec(description="Entry short name (from short_names.dat)"),
}

ENTRY_PARENTS_SCHEMA = {
    "interpro_id": ColumnSpec(identifier=InterProId, required=True, description="InterPro entry accession (the child)"),
    "parent_id": ColumnSpec(identifier=InterProId, required=True, description="InterPro accession of the direct parent entry (from the ParentChildTree indentation)"),
}

ENTRY2GO_SCHEMA = {
    "interpro_id": ColumnSpec(identifier=InterProId, required=True, description="InterPro entry accession"),
    "go_id": ColumnSpec(identifier=GoId, required=True, description="GO term the entry is mapped to (the interpro2go mapping)"),
}

# ---- large protein-match table (polars streaming) -------------------------
PROTEIN2ENTRY_SCHEMA = {
    "uniprot_accession": ColumnSpec(required=True, description="UniProtKB accession of the matched protein (reviewed or unreviewed); a documented plain string"),
    "interpro_id": ColumnSpec(identifier=InterProId, required=True, description="Matched InterPro entry accession"),
    "description": ColumnSpec(description="InterPro entry name (repeated from the entry; kept verbatim)"),
    "member_db_id": ColumnSpec(description="Member-database signature accession that produced the match (PF/TIGR/G3DSA/SSF/…); a plain string"),
    "start_pos": ColumnSpec(description="Match start position on the protein (Int64)"),
    "end_pos": ColumnSpec(description="Match end position on the protein (Int64)"),
}
_P2E_COLUMNS = list(PROTEIN2ENTRY_SCHEMA)

_PROTEIN2IPR = "protein2ipr.dat.gz"
_BLOCK_BYTES = 512 << 20  # ~512 MiB of decompressed text per polars chunk
_ENTRY2GO_RE = re.compile(r"^InterPro:(IPR\d+)\b.*;\s*(GO:\d{7})\s*$")


@register
class Interpro(DatasetPipeline):
    name = "interpro"

    _SMALL_TABLES = [
        ("entries", ENTRIES_SCHEMA),
        ("entry_parents", ENTRY_PARENTS_SCHEMA),
        ("entry2go", ENTRY2GO_SCHEMA),
    ]

    def expected_schemas(self) -> dict:
        # protein2entry is streamed to sharded parquet (not in _SMALL_TABLES); add it here.
        return {**dict(self._SMALL_TABLES), "protein2entry": dict(PROTEIN2ENTRY_SCHEMA)}

    # ---- small reference tables ------------------------------------------
    def _build_entries(self) -> pd.DataFrame:
        entry = pd.read_csv(self.raw_path() / "entry.list", sep="\t", dtype=str, keep_default_na=False)
        entry = entry.rename(columns={"ENTRY_AC": "interpro_id", "ENTRY_TYPE": "entry_type", "ENTRY_NAME": "name"})
        short = pd.read_csv(self.raw_path() / "short_names.dat", sep="\t", header=None, names=["interpro_id", "short_name"], dtype=str, keep_default_na=False)
        df = entry.merge(short, on="interpro_id", how="left")
        return df[list(ENTRIES_SCHEMA)]

    def _build_entry_parents(self) -> pd.DataFrame:
        rows = []
        stack: list[str] = []
        with open(self.raw_path() / "ParentChildTreeFile.txt", encoding="utf-8") as fh:
            for line in fh:
                line = line.rstrip("\n")
                if not line:
                    continue
                stripped = line.lstrip("-")
                depth = (len(line) - len(stripped)) // 2  # two dashes per level
                ipr = stripped.split("::", 1)[0]
                del stack[depth:]
                if depth > 0:
                    rows.append({"interpro_id": ipr, "parent_id": stack[depth - 1]})
                stack.append(ipr)
        return pd.DataFrame(rows, columns=list(ENTRY_PARENTS_SCHEMA))

    def _build_entry2go(self) -> pd.DataFrame:
        rows = []
        with open(self.raw_path() / "interpro2go", encoding="utf-8") as fh:
            for line in fh:
                if line.startswith("!"):
                    continue
                m = _ENTRY2GO_RE.match(line.rstrip("\n"))
                if m:
                    rows.append({"interpro_id": m.group(1), "go_id": m.group(2)})
        return pd.DataFrame(rows, columns=list(ENTRY2GO_SCHEMA))

    # ---- large protein-match table ---------------------------------------
    def _stream_protein2entry(self) -> None:
        out_dir = self.output_path() / "protein2entry"
        if (out_dir / "_SUCCESS").exists():
            print(f"[{self.name}] protein2entry: already complete, skipping stream")
            return
        if out_dir.exists():  # clear any partial shards from an interrupted run
            shutil.rmtree(out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        path = self.raw_path() / _PROTEIN2IPR
        if not path.exists():
            raise FileNotFoundError(f"interpro: expected {_PROTEIN2IPR} under {self.raw_path()}")

        part = 0
        total = 0
        buf = b""
        with gzip.open(path, "rb") as fh:
            while True:
                block = fh.read(_BLOCK_BYTES)
                if not block:
                    break
                buf += block
                nl = buf.rfind(b"\n")
                if nl == -1:
                    continue
                chunk, buf = buf[: nl + 1], buf[nl + 1:]
                total += self._write_p2e_shard(chunk, out_dir, part)
                part += 1
                print(f"[{self.name}] protein2entry: shard {part}, {total:,} rows", flush=True)
            if buf.strip():
                total += self._write_p2e_shard(buf, out_dir, part)
                part += 1

        (out_dir / "_SUCCESS").touch()
        print(f"[{self.name}] protein2entry: {total:,} rows in {part} shards")

    def _write_p2e_shard(self, chunk: bytes, out_dir: Path, part: int) -> int:
        df = pl.read_csv(
            io.BytesIO(chunk), separator="\t", has_header=False, new_columns=_P2E_COLUMNS,
            infer_schema_length=0, quote_char=None,
        ).with_columns(
            pl.col("start_pos").cast(pl.Int64),
            pl.col("end_pos").cast(pl.Int64),
        )
        # Validate the InterPro id per shard (cheap, in-memory) — fail fast on bad data.
        PROTEIN2ENTRY_SCHEMA["interpro_id"].validate_polars(df["interpro_id"])
        df.write_parquet(out_dir / f"part_{part:05d}.parquet", compression="snappy")
        return df.height

    # ---- pipeline stages -------------------------------------------------
    def extract(self) -> None:
        self._build_entries().pipe(self.save_parquet, self.intermediate_path() / "entries.parquet")
        self._build_entry_parents().pipe(self.save_parquet, self.intermediate_path() / "entry_parents.parquet")
        self._build_entry2go().pipe(self.save_parquet, self.intermediate_path() / "entry2go.parquet")
        print(f"[{self.name}] small reference tables built")
        self._stream_protein2entry()

    def transform(self) -> None:
        # Faithful projection; parsing in extract, typing/validation in load.
        pass

    def load(self) -> None:
        for stem, schema in self._SMALL_TABLES:
            df = self.load_parquet(self.intermediate_path() / f"{stem}.parquet")
            df = df[list(schema)]
            for col in df.columns:
                df[col] = df[col].astype("string").str.strip().where(lambda x: x != "", None)
            df = self.apply_schema(df, schema)
            self.save_parquet(df, self.output_path() / f"{stem}.parquet")
            self.save_schema_yaml(schema, stem)
            print(f"[{self.name}] {stem}: {len(df):,} rows")
        # protein2entry was written as sharded parquet directly to the output; emit its sidecar.
        data = {col: spec.yaml_entry() for col, spec in PROTEIN2ENTRY_SCHEMA.items()}
        (self.output_path() / "protein2entry.yml").write_text(yaml.dump(data, sort_keys=False, allow_unicode=True))
