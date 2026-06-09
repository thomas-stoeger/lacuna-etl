"""OMIM — Online Mendelian Inheritance in Man, the gene↔identifier crosswalk.

OMIM as a whole is redistribution-restricted; the openly redistributable slice is
``mim2gene.txt``, a single small TSV (~29.6k rows) that links each MIM number to the
other gene identifiers (Entrez, the HGNC-approved symbol, Ensembl). It carries no
phenotype text, only the cross-reference. It fits trivially in memory, so this is an
in-memory pandas pipeline: ``extract`` reads the TSV into one table, ``transform`` is
a no-op (faithful projection), and ``load`` types, validates, and writes.

The grain key is the MIM number (``MimNumber``, a 6-digit catalog id), one row per
MIM entry. The cross-reference columns are sparse — only ``gene`` and some
``phenotype`` entries carry an Entrez/Ensembl id — so ``entrez_id``,
``approved_gene_symbol``, and ``ensembl_gene_id`` are nullable; the typed ones use
the repo's canonical ``NcbiGeneId`` / ``EnsemblGeneId``. The HGNC-approved symbol is a
plain string (it is the symbol, not the ``HGNC:`` curie). See docs/DESIGN.md for the
table inventory.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from lacuna_etl.core.identifiers import EnsemblGeneId, MimNumber, NcbiGeneId
from lacuna_etl.core.pipeline import DatasetPipeline
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.registry import register

# The five MIM entry types defined in the OMIM FAQ (1.3); a small, stable controlled
# vocabulary, so allowed_values-constrained.
_ENTRY_TYPES = {"gene", "gene/phenotype", "phenotype", "predominantly phenotypes", "moved/removed"}

_COLUMNS = ["mim_number", "mim_entry_type", "entrez_id", "approved_gene_symbol", "ensembl_gene_id"]

MIM2GENE_SCHEMA = {
    "mim_number": ColumnSpec(identifier=MimNumber, required=True, description="OMIM MIM number; the grain key"),
    "mim_entry_type": ColumnSpec(allowed_values=_ENTRY_TYPES, required=True, description="MIM entry type (OMIM FAQ 1.3): gene, gene/phenotype, phenotype, predominantly phenotypes, or moved/removed"),
    "entrez_id": ColumnSpec(identifier=NcbiGeneId, description="NCBI Entrez Gene id linked to the MIM entry (nullable)"),
    "approved_gene_symbol": ColumnSpec(description="HGNC-approved gene symbol (the symbol itself, not the HGNC curie; nullable)"),
    "ensembl_gene_id": ColumnSpec(identifier=EnsemblGeneId, description="Ensembl gene id (human, ENSG…; nullable)"),
}


def _clean(v: object) -> str | None:
    """Empty / whitespace-only string (OMIM's null marker) -> None."""
    if v is None or not isinstance(v, str):
        return None
    v = v.strip()
    return v or None


@register
class Omim(DatasetPipeline):
    name = "omim"

    _TABLES = [("mim2gene", MIM2GENE_SCHEMA)]

    def _read_tsv(self) -> pd.DataFrame:
        path = self.raw_path() / "mim2gene.txt"
        if not path.exists():
            raise FileNotFoundError(f"omim: expected mim2gene.txt under {self.raw_path()}")
        # The file opens with comment lines (the last of which is the column header);
        # comment='#' skips all of them regardless of count. Empty string is OMIM's
        # only null marker, normalised per column via _clean.
        return pd.read_csv(
            path, sep="\t", header=None, names=_COLUMNS, comment="#",
            dtype=str, keep_default_na=False, na_filter=False,
        )

    def extract(self) -> None:
        df = self._read_tsv()
        rows = [
            {
                "mim_number": _clean(rec["mim_number"]),
                "mim_entry_type": _clean(rec["mim_entry_type"]),
                "entrez_id": NcbiGeneId.parse(rec["entrez_id"]),
                "approved_gene_symbol": _clean(rec["approved_gene_symbol"]),
                "ensembl_gene_id": _clean(rec["ensembl_gene_id"]),
            }
            for rec in df.to_dict("records")
        ]
        self.save_parquet(
            pd.DataFrame(rows, columns=_COLUMNS), self.intermediate_path() / "mim2gene.parquet"
        )
        print(f"[{self.name}] mim2gene: {len(rows):,} ({sum(r['entrez_id'] is not None for r in rows):,} with entrez)")

    def transform(self) -> None:
        # Faithful projection; parsing happens in extract, typing/validation in load.
        pass

    def load(self) -> None:
        for stem, schema in self._TABLES:
            df = self.load_parquet(self.intermediate_path() / f"{stem}.parquet")
            df = df[list(schema)]
            # entrez_id is a legitimately-sparse Entrez Gene id; NumericIdentifier
            # casts it but its validate rejects any null, so check positivity on the
            # non-null values only (the hgnc nullable-entrez precedent).
            df["entrez_id"] = NcbiGeneId.cast(df["entrez_id"])
            if (df["entrez_id"].dropna() <= 0).any():
                raise ValueError("omim: non-positive entrez_id")
            schema_no_entrez = {k: v for k, v in schema.items() if k != "entrez_id"}
            df = self.apply_schema(df, schema_no_entrez)
            self.save_parquet(df, self.output_path() / f"{stem}.parquet")
            self.save_schema_yaml(schema, stem)
