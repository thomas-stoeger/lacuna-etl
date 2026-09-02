"""UniProt Swiss-Prot — the manually-reviewed protein sequences and their headers.

The download is a single gzipped FASTA (``uniprot_sprot.fasta.gz``, ~575k reviewed
entries). Each record is a structured header line followed by the (multi-line)
amino-acid sequence. This pipeline parses the header into typed columns and keeps the
sequence: ``extract`` streams the gzip once, accumulating one row per record,
``transform`` is a no-op, and ``load`` types, validates, and writes a single
``proteins`` table.

The grain key is the UniProtKB ``accession`` (``UniprotAccession``), one row per
entry. The header's structured fields (UniProt's own ``OS=``/``OX=``/``GN=``/``PE=``/
``SV=`` convention) are split out: ``organism_name`` (OS), ``tax_id`` (OX,
``NcbiTaxId``), ``gene_name`` (GN, absent for ~14% of entries → null),
``protein_existence`` (PE, 1–5), ``sequence_version`` (SV). The free-text protein
description and the ``entry_name`` mnemonic stay plain strings. ``length`` is the
residue count of the stored ``sequence``. See docs/DESIGN.md for the table inventory.
"""
from __future__ import annotations

import gzip
import re
from pathlib import Path

import pandas as pd

from lacuna_etl.core.identifiers import NcbiTaxId, UniprotAccession
from lacuna_etl.core.pipeline import DatasetPipeline
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.registry import register

# `>sp|ACC|ENTRY <description> OS=<organism> OX=<taxid> [GN=<gene>] PE=<n> SV=<n>`.
# Name and organism are captured non-greedily up to the next ` OS=` / ` OX=` marker;
# GN is optional; PE and SV always close the header.
_HEADER_RE = re.compile(
    r"^>sp\|(?P<accession>[^|]+)\|(?P<entry_name>\S+)\s+"
    r"(?P<protein_name>.*?)\s+OS=(?P<organism_name>.+?)\s+OX=(?P<tax_id>\d+)"
    # GN is optional and, rarely, contains spaces ('GN=MGF 110-1L'), so capture
    # non-greedily up to the closing ' PE= ... SV=' that always ends the header.
    r"(?:\s+GN=(?P<gene_name>.+?))?\s+PE=(?P<protein_existence>\d+)\s+SV=(?P<sequence_version>\d+)$"
)

PROTEINS_SCHEMA = {
    "accession": ColumnSpec(identifier=UniprotAccession, required=True, description="UniProtKB accession; the grain key (the first '|'-delimited header field)"),
    "entry_name": ColumnSpec(required=True, description="UniProtKB entry name / mnemonic (e.g. '001R_FRG3G')"),
    "protein_name": ColumnSpec(required=True, description="Protein description (the header text between the entry name and 'OS=')"),
    "organism_name": ColumnSpec(required=True, description="Source organism scientific name (the header 'OS=' field)"),
    "tax_id": ColumnSpec(identifier=NcbiTaxId, required=True, description="NCBI taxonomy id of the source organism (the header 'OX=' field)"),
    "gene_name": ColumnSpec(description="Primary gene name (the header 'GN=' field; null when absent)"),
    "protein_existence": ColumnSpec(description="Protein existence evidence level 1–5 (the header 'PE=' field; Int64)"),
    "sequence_version": ColumnSpec(description="Sequence version (the header 'SV=' field; Int64)"),
    "sequence": ColumnSpec(required=True, description="Amino-acid sequence (single-letter codes)"),
    "length": ColumnSpec(required=True, description="Residue count of the sequence (Int64)"),
}


@register
class UniprotFasta(DatasetPipeline):
    name = "uniprot_fasta"
    _TABLES = [("proteins", PROTEINS_SCHEMA)]

    def _fasta_file(self) -> Path:
        path = self.raw_path() / "uniprot_sprot.fasta.gz"
        if not path.exists():
            raise FileNotFoundError(f"uniprot_fasta: expected uniprot_sprot.fasta.gz under {self.raw_path()}")
        return path

    def extract(self) -> None:
        rows: list[dict] = []
        header: str | None = None
        seq_parts: list[str] = []

        def flush() -> None:
            if header is None:
                return
            m = _HEADER_RE.match(header)
            if not m:
                raise ValueError(f"uniprot_fasta: unparseable header: {header!r}")
            seq = "".join(seq_parts)
            d = m.groupdict()
            rows.append({
                "accession": d["accession"],
                "entry_name": d["entry_name"],
                "protein_name": d["protein_name"],
                "organism_name": d["organism_name"],
                "tax_id": NcbiTaxId.parse(d["tax_id"]),
                "gene_name": d["gene_name"],
                "protein_existence": int(d["protein_existence"]),
                "sequence_version": int(d["sequence_version"]),
                "sequence": seq,
                "length": len(seq),
            })

        with gzip.open(self._fasta_file(), "rt", encoding="utf-8") as fh:
            for line in fh:
                if line.startswith(">"):
                    flush()
                    header = line.rstrip("\n")
                    seq_parts = []
                else:
                    seq_parts.append(line.strip())
            flush()

        self.save_parquet(
            pd.DataFrame(rows, columns=list(PROTEINS_SCHEMA)), self.intermediate_path() / "proteins.parquet"
        )
        print(f"[{self.name}] proteins: {len(rows):,} ({sum(r['gene_name'] is not None for r in rows):,} with gene name)")

    def transform(self) -> None:
        # Faithful projection; parsing happens in extract, typing/validation in load.
        pass

    def load(self) -> None:
        df = self.load_parquet(self.intermediate_path() / "proteins.parquet")
        df = df[list(PROTEINS_SCHEMA)]
        for col in ("protein_existence", "sequence_version", "length"):
            df[col] = df[col].astype("Int64")
        df = self.apply_schema(df, PROTEINS_SCHEMA)
        self.save_parquet(df, self.output_path() / "proteins.parquet")
        self.save_schema_yaml(PROTEINS_SCHEMA, "proteins")
