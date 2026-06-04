"""The Unknome database — proteins ranked by how little is known about them.

Unknome (Rocha et al., PLoS Biol 2023) clusters eukaryotic proteins into
ortholog groups built on PANTHER families and assigns each protein and cluster a
"knownness" score (0 = completely uncharacterised), to surface conserved proteins
that remain unstudied. The download is two gzipped TSVs — one row per UniProt
protein entry and one row per cluster — which this in-memory pandas pipeline reads,
types, and validates into a `proteins` table and a `clusters` table joined on
`cluster_id`. Source headers are already snake_case, so they map straight through.
"""
from __future__ import annotations

import csv
from pathlib import Path

import pandas as pd

from lacuna_etl.core.identifiers import NcbiTaxId, UniprotAccession
from lacuna_etl.core.pipeline import DatasetPipeline
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.registry import register

# Source headers of the protein TSV; ``uniprot_accessions`` is a ``;``-delimited
# list that we explode into one ``uniprot_accession`` per row.
_PROTEIN_SOURCE_COLS = [
    "uniprot_name", "uniprot_accessions", "knownness", "gene_name",
    "protein_name", "taxon_id", "species", "cluster_id", "panther_group",
]

PROTEINS_SCHEMA = {
    "uniprot_name": ColumnSpec(description="UniProt entry name (mnemonic, e.g. RBL_AMBTC)", required=True),
    "uniprot_accession": ColumnSpec(identifier=UniprotAccession, required=True, description="UniProtKB accession; entries listing several accessions are exploded to one row per accession (so the grain is one row per uniprot_name × accession)"),
    "knownness": ColumnSpec(description="Unknome 'knownness' score (0-100): how much is known about the protein; lower = less studied, 0 = completely uncharacterised"),
    "gene_name": ColumnSpec(description="Gene name/symbol"),
    "protein_name": ColumnSpec(description="UniProt protein name/description"),
    "taxon_id": ColumnSpec(identifier=NcbiTaxId, description="NCBI taxonomy ID of the species"),
    "species": ColumnSpec(description="Species name"),
    "cluster_id": ColumnSpec(description="Unknome cluster the protein belongs to (UKP…); foreign key to the clusters table", required=True),
    "panther_group": ColumnSpec(description="PANTHER family ID the cluster is built on (PTHR…)"),
}

CLUSTERS_SCHEMA = {
    "cluster_id": ColumnSpec(description="Unknome cluster identifier (UKP…)", required=True),
    "panther_id": ColumnSpec(description="PANTHER family ID the cluster is built on (PTHR…)"),
    "cluster_name": ColumnSpec(description="Cluster name (typically the representative protein/family name)"),
    "knownness": ColumnSpec(description="Unknome 'knownness' score (0-100) for the cluster; lower = less studied"),
    "num_proteins": ColumnSpec(description="Number of proteins in the cluster"),
    "num_species": ColumnSpec(description="Number of distinct species represented in the cluster"),
    "best_known_protein_id": ColumnSpec(description="UniProt entry name of the best-known protein in the cluster"),
    "best_known_protein_gene": ColumnSpec(description="Gene name of the best-known protein"),
    "best_known_protein_name": ColumnSpec(description="Protein name of the best-known protein"),
    "key_protein_ids": ColumnSpec(description="UniProt entry name(s) of the cluster's key protein(s) (space-delimited if more than one)"),
    "key_protein_xrefs": ColumnSpec(description="External cross-reference(s) for the key protein(s), e.g. TAIR:AT… (space-delimited if more than one)"),
}


def _read(path: Path) -> pd.DataFrame:
    """Read an Unknome gzipped TSV as all-string columns (fields can carry bare
    ``"`` inside protein names, so quoting is disabled)."""
    return pd.read_csv(
        path,
        sep="\t",
        dtype=str,
        compression="gzip",
        quoting=csv.QUOTE_NONE,
        keep_default_na=False,
        na_filter=False,
    )


def _null_strings(df: pd.DataFrame, skip: set[str]) -> None:
    """In place: strip whitespace and map ``""`` / ``-`` (the source null markers) to null."""
    for col in df.columns:
        if col in skip:
            continue
        s = df[col].str.strip()
        df[col] = s.mask(s.isin(["", "-"]), pd.NA)


def _to_float(s: pd.Series) -> pd.Series:
    cleaned = s.str.strip().mask(s.str.strip().isin(["", "-"]), pd.NA)
    return pd.to_numeric(cleaned, errors="raise").astype("Float64")


def _to_int(s: pd.Series) -> pd.Series:
    cleaned = s.str.strip().mask(s.str.strip().isin(["", "-"]), pd.NA)
    return pd.to_numeric(cleaned, errors="raise").astype("Int64")


@register
class Unknome(DatasetPipeline):
    name = "unknome"

    _TABLES = [
        ("proteins", PROTEINS_SCHEMA),
        ("clusters", CLUSTERS_SCHEMA),
    ]

    def _file(self, kind: str) -> Path:
        matches = sorted(self.raw_path().glob(f"unknome_{kind}_table_*.tsv.gz"))
        if len(matches) != 1:
            raise FileNotFoundError(f"expected exactly one unknome_{kind}_table file under {self.raw_path()}, found {len(matches)}")
        return matches[0]

    def extract(self) -> None:
        self._extract_proteins()
        self._extract_clusters()

    def _extract_proteins(self) -> None:
        df = _read(self._file("protein"))
        if list(df.columns) != _PROTEIN_SOURCE_COLS:
            raise ValueError(f"unexpected protein columns: {list(df.columns)}")
        _null_strings(df, {"taxon_id", "knownness"})
        df["knownness"] = _to_float(df["knownness"])
        # Explode the ``;``-delimited accession list to one row per accession, so a
        # multi-accession entry no longer hides extra accessions in a packed string.
        df["uniprot_accessions"] = df["uniprot_accessions"].str.split(";")
        df = df.explode("uniprot_accessions", ignore_index=True)
        df["uniprot_accessions"] = df["uniprot_accessions"].str.strip()
        df = df.rename(columns={"uniprot_accessions": "uniprot_accession"})
        df = self.apply_schema(df[list(PROTEINS_SCHEMA)], PROTEINS_SCHEMA)
        self.save_parquet(df, self.intermediate_path() / "proteins.parquet")

    def _extract_clusters(self) -> None:
        df = _read(self._file("cluster"))
        if list(df.columns) != list(CLUSTERS_SCHEMA):
            raise ValueError(f"unexpected cluster columns: {list(df.columns)}")
        _null_strings(df, {"knownness", "num_proteins", "num_species"})
        df["knownness"] = _to_float(df["knownness"])
        df["num_proteins"] = _to_int(df["num_proteins"])
        df["num_species"] = _to_int(df["num_species"])
        df = self.apply_schema(df, CLUSTERS_SCHEMA)
        self.save_parquet(df, self.intermediate_path() / "clusters.parquet")

    def transform(self) -> None:
        # Faithful projection; casting/validation happen in extract.
        pass

    def load(self) -> None:
        for stem, schema in self._TABLES:
            df = self.load_parquet(self.intermediate_path() / f"{stem}.parquet")
            self.save_parquet(df, self.output_path() / f"{stem}.parquet")
            self.save_schema_yaml(schema, stem)
