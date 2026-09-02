"""GTEx — Genotype-Tissue Expression: gene expression across human tissues.

GTEx v10 ships three gene-level expression matrices in GCT format (a 2-line header
then a ``Name``/``Description`` + data-columns matrix): the per-tissue median TPM
(gene × 68 tissues), and the per-sample raw read counts and TPM (gene × ~19.7k
samples). The two per-sample matrices melt to ~1.16 billion rows each, so by design
this pipeline materialises only the compact, commonly-used ``gene_median_tpm`` summary
in long form, plus a small ``samples`` table cataloguing the per-sample matrices'
columns. ``extract`` reads the median matrix (pandas; it is small) and the per-sample
headers, ``transform`` is a no-op, and ``load`` types, validates, and writes.

The gene identifier is the GCT ``Name``, a versioned GENCODE id (``ENSG…\\.<v>``, with a
``_PAR_Y`` marker on the 45 Y-chromosome pseudoautosomal copies). It is split into the
unversioned ``ensembl_gene_id`` (``EnsemblGeneId``; version and ``_PAR_Y`` stripped — so
the X/Y PAR copies share it) and ``ensembl_gene_id_versioned`` (the full Name, the
real key, a plain string), following the Doi/DoiVersioned version-split precedent.
``tissue`` is a documented free string. See docs/DESIGN.md for the table inventory.
"""
from __future__ import annotations

import gzip
import re
from pathlib import Path

import pandas as pd

from lacuna_etl.core.identifiers import EnsemblGeneId
from lacuna_etl.core.pipeline import DatasetPipeline
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.registry import register

_MEDIAN_FILE = "GTEx_Analysis_v10_RNASeQCv2.4.2_gene_median_tpm.gct.gz"
_PERSAMPLE_FILES = {
    "gene_reads": "GTEx_Analysis_v10_RNASeQCv2.4.2_gene_reads.gct.gz",
    "gene_tpm": "GTEx_Analysis_v10_RNASeQCv2.4.2_gene_tpm.gct.gz",
}
# Strip the trailing '.<version>' and optional GENCODE '_PAR_Y' marker.
_VERSION_RE = re.compile(r"\.\d+(?:_PAR_Y)?$")

GENE_MEDIAN_TPM_SCHEMA = {
    "ensembl_gene_id": ColumnSpec(identifier=EnsemblGeneId, required=True, description="Unversioned Ensembl gene id (GCT Name with version and _PAR_Y stripped; non-unique for the X/Y PAR copies — join via ensembl_gene_id_versioned for the exact gene)"),
    "ensembl_gene_id_versioned": ColumnSpec(required=True, description="The full GCT Name (versioned GENCODE id, with _PAR_Y where applicable); the gene key (with tissue)"),
    "gene_symbol": ColumnSpec(description="HGNC gene symbol (the GCT Description column)"),
    "tissue": ColumnSpec(required=True, description="GTEx tissue (the matrix column header, e.g. 'Adipose_Subcutaneous')"),
    "median_tpm": ColumnSpec(required=True, description="Median TPM of the gene in the tissue (Float64)"),
}

SAMPLES_SCHEMA = {
    "matrix": ColumnSpec(allowed_values=set(_PERSAMPLE_FILES), required=True, description="Which per-sample matrix the sample appears in: gene_reads or gene_tpm"),
    "sample_id": ColumnSpec(required=True, description="GTEx sample id (a per-sample matrix column header, e.g. 'GTEX-1117F-0005-SM-HL9SH')"),
}


@register
class Gtex(DatasetPipeline):
    name = "gtex"

    _TABLES = [("gene_median_tpm", GENE_MEDIAN_TPM_SCHEMA), ("samples", SAMPLES_SCHEMA)]

    def _gct_header_columns(self, path: Path) -> list[str]:
        """Return the data-column names (GCT line 3, from the 3rd field on)."""
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            fh.readline()  # '#1.2'
            fh.readline()  # '<nrows>\t<ncols>'
            header = fh.readline().rstrip("\n").split("\t")
        return header[2:]

    def extract(self) -> None:
        # Median matrix: small enough for pandas. Skip the two GCT header lines.
        median_path = self.raw_path() / _MEDIAN_FILE
        if not median_path.exists():
            raise FileNotFoundError(f"gtex: expected {_MEDIAN_FILE} under {self.raw_path()}")
        wide = pd.read_csv(median_path, sep="\t", skiprows=2, dtype={"Name": str, "Description": str})
        tissues = [c for c in wide.columns if c not in ("Name", "Description")]
        long = wide.melt(
            id_vars=["Name", "Description"], value_vars=tissues,
            var_name="tissue", value_name="median_tpm",
        )
        long["ensembl_gene_id"] = long["Name"].str.replace(_VERSION_RE, "", regex=True)
        long = long.rename(columns={"Name": "ensembl_gene_id_versioned", "Description": "gene_symbol"})
        long = long[list(GENE_MEDIAN_TPM_SCHEMA)]
        self.save_parquet(long, self.intermediate_path() / "gene_median_tpm.parquet")

        # Per-sample matrices: catalogue their sample-id columns only (the data is the
        # ~1.16B-row-each melt we intentionally do not materialise).
        sample_rows = []
        for matrix, fname in _PERSAMPLE_FILES.items():
            path = self.raw_path() / fname
            if not path.exists():
                raise FileNotFoundError(f"gtex: expected {fname} under {self.raw_path()}")
            for sid in self._gct_header_columns(path):
                sample_rows.append({"matrix": matrix, "sample_id": sid})
        self.save_parquet(
            pd.DataFrame(sample_rows, columns=list(SAMPLES_SCHEMA)), self.intermediate_path() / "samples.parquet"
        )
        print(f"[{self.name}] gene_median_tpm: {len(long):,} ({len(tissues)} tissues), samples: {len(sample_rows):,}")

    def transform(self) -> None:
        # Faithful projection; reshaping in extract, typing/validation in load.
        pass

    def load(self) -> None:
        for stem, schema in self._TABLES:
            df = self.load_parquet(self.intermediate_path() / f"{stem}.parquet")
            df = df[list(schema)]
            if stem == "gene_median_tpm":
                df["median_tpm"] = df["median_tpm"].astype("Float64")
            df = self.apply_schema(df, schema)
            self.save_parquet(df, self.output_path() / f"{stem}.parquet")
            self.save_schema_yaml(schema, stem)
