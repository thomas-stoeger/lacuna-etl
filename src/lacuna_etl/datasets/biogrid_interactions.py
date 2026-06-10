"""BioGRID — the Biological General Repository for Interaction Datasets.

BioGRID's complete interaction set ships as one large tab-delimited file in the
"tab3" format (~2.9M rows, 37 columns), distributed inside a zip. Each row is one
curated physical or genetic interaction between two interactors, with the gene/
protein identifiers of both sides, the experimental system, the source publication,
and curated annotations. It reads with pandas (the repo runs much larger pandas
tables), so this is an in-memory pipeline: ``extract`` reads the TSV from the zip,
renames the headers to snake_case, and normalises BioGRID's ``-`` null marker;
``transform`` is a no-op; ``load`` types, validates, and writes one ``interactions``
table.

The grain is one row per interaction record, keyed by ``biogrid_interaction_id``
(``BiogridId``). The interactor gene ids (``entrez_gene_a/b`` → ``NcbiGeneId``, the
BioGRID interactor ids → ``BiogridId``, the organism ids → ``NcbiTaxId``) and the
derived ``pubmed_id`` (parsed from the ``PUBMED:`` publication source) are typed; the
SWISS-PROT/TREMBL/REFSEQ accession columns are pipe-delimited multi-value strings kept
verbatim, and the symbol/synonym/ontology/annotation columns stay documented plain
strings (the heterogeneous-id / alliancegenome MITAB precedent). ``-`` is BioGRID's
null marker and is normalised to null on read. See docs/DESIGN.md for the table.
"""
from __future__ import annotations

import csv
from pathlib import Path

import pandas as pd

from lacuna_etl.core.identifiers import BiogridId, NcbiGeneId, NcbiTaxId, PubmedId
from lacuna_etl.core.pipeline import DatasetPipeline
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.registry import register

_SYSTEM_TYPES = {"physical", "genetic"}

# The 37 source columns in order; the header is overridden positionally because the
# first column carries a leading '#' and the names contain spaces/brackets.
_SOURCE_COLUMNS = [
    "biogrid_interaction_id", "entrez_gene_a", "entrez_gene_b", "biogrid_id_a", "biogrid_id_b",
    "systematic_name_a", "systematic_name_b", "official_symbol_a", "official_symbol_b",
    "synonyms_a", "synonyms_b", "experimental_system", "experimental_system_type", "author",
    "publication_source", "organism_id_a", "organism_id_b", "throughput", "score",
    "modification", "qualifications", "tags", "source_database",
    "swissprot_a", "trembl_a", "refseq_a", "swissprot_b", "trembl_b", "refseq_b",
    "ontology_term_ids", "ontology_term_names", "ontology_term_categories",
    "ontology_term_qualifier_ids", "ontology_term_qualifier_names", "ontology_term_types",
    "organism_name_a", "organism_name_b",
]

INTERACTIONS_SCHEMA = {
    "biogrid_interaction_id": ColumnSpec(identifier=BiogridId, required=True, description="BioGRID interaction id; the grain key"),
    "entrez_gene_a": ColumnSpec(identifier=NcbiGeneId, description="Entrez Gene id of interactor A (nullable; '-' for non-gene interactors)"),
    "entrez_gene_b": ColumnSpec(identifier=NcbiGeneId, description="Entrez Gene id of interactor B (nullable)"),
    "biogrid_id_a": ColumnSpec(identifier=BiogridId, required=True, description="BioGRID interactor id of A"),
    "biogrid_id_b": ColumnSpec(identifier=BiogridId, required=True, description="BioGRID interactor id of B"),
    "systematic_name_a": ColumnSpec(description="Systematic name of interactor A"),
    "systematic_name_b": ColumnSpec(description="Systematic name of interactor B"),
    "official_symbol_a": ColumnSpec(description="Official symbol of interactor A"),
    "official_symbol_b": ColumnSpec(description="Official symbol of interactor B"),
    "synonyms_a": ColumnSpec(description="Synonyms for interactor A (pipe-delimited, kept verbatim)"),
    "synonyms_b": ColumnSpec(description="Synonyms for interactor B (pipe-delimited, kept verbatim)"),
    "experimental_system": ColumnSpec(description="Experimental system / detection method (e.g. 'Affinity Capture-MS')"),
    "experimental_system_type": ColumnSpec(allowed_values=_SYSTEM_TYPES, required=True, description="Interaction class: physical or genetic"),
    "author": ColumnSpec(description="Publication first author and year"),
    "publication_source": ColumnSpec(description="Source publication reference (PUBMED:<id> or DOI:<doi>)"),
    "pubmed_id": ColumnSpec(identifier=PubmedId, description="PubMed id parsed from publication_source (null for DOI-only sources)"),
    "organism_id_a": ColumnSpec(identifier=NcbiTaxId, description="NCBI taxonomy id of interactor A's organism (nullable)"),
    "organism_id_b": ColumnSpec(identifier=NcbiTaxId, description="NCBI taxonomy id of interactor B's organism (nullable)"),
    "throughput": ColumnSpec(description="Throughput class (e.g. 'Low Throughput', 'High Throughput')"),
    "score": ColumnSpec(description="Quantitative interaction score where reported (Float64; nullable)"),
    "modification": ColumnSpec(description="Post-translational modification (for some physical interactions)"),
    "qualifications": ColumnSpec(description="Free-text experimental qualifications"),
    "tags": ColumnSpec(description="Curated tags"),
    "source_database": ColumnSpec(description="Source database that contributed the interaction (e.g. BIOGRID, IntAct)"),
    "swissprot_a": ColumnSpec(description="Swiss-Prot accession(s) of interactor A (pipe-delimited, kept verbatim)"),
    "trembl_a": ColumnSpec(description="TrEMBL accession(s) of interactor A (pipe-delimited)"),
    "refseq_a": ColumnSpec(description="RefSeq accession(s) of interactor A (pipe-delimited)"),
    "swissprot_b": ColumnSpec(description="Swiss-Prot accession(s) of interactor B (pipe-delimited)"),
    "trembl_b": ColumnSpec(description="TrEMBL accession(s) of interactor B (pipe-delimited)"),
    "refseq_b": ColumnSpec(description="RefSeq accession(s) of interactor B (pipe-delimited)"),
    "ontology_term_ids": ColumnSpec(description="Ontology term id(s) annotating the interaction (pipe-delimited)"),
    "ontology_term_names": ColumnSpec(description="Ontology term name(s) (pipe-delimited)"),
    "ontology_term_categories": ColumnSpec(description="Ontology term category/categories (pipe-delimited)"),
    "ontology_term_qualifier_ids": ColumnSpec(description="Ontology term qualifier id(s) (pipe-delimited)"),
    "ontology_term_qualifier_names": ColumnSpec(description="Ontology term qualifier name(s) (pipe-delimited)"),
    "ontology_term_types": ColumnSpec(description="Ontology term type(s) (pipe-delimited)"),
    "organism_name_a": ColumnSpec(description="Organism name of interactor A"),
    "organism_name_b": ColumnSpec(description="Organism name of interactor B"),
}

@register
class BiogridInteractions(DatasetPipeline):
    name = "biogrid_interactions"
    _TABLES = [("interactions", INTERACTIONS_SCHEMA)]

    def _tab3_zip(self) -> Path:
        zips = sorted(self.raw_path().glob("*.zip"))
        if len(zips) != 1:
            raise FileNotFoundError(f"biogrid_interactions: expected exactly one *.zip under {self.raw_path()}, found {len(zips)}")
        return zips[0]

    def extract(self) -> None:
        # keep_default_na=False with na_values=['-', '']: BioGRID's '-' and empty are
        # its only null markers, normalised here in one vectorised pass.
        df = pd.read_csv(
            self._tab3_zip(), compression="zip", sep="\t", header=0, dtype=str,
            keep_default_na=False, na_values=["-", ""], quoting=csv.QUOTE_NONE,
        )
        if df.shape[1] != len(_SOURCE_COLUMNS):
            raise ValueError(f"biogrid_interactions: source has {df.shape[1]} columns, expected {len(_SOURCE_COLUMNS)}")
        df.columns = _SOURCE_COLUMNS
        # Derive pubmed_id from the PUBMED:<id> publication source (DOI sources -> null).
        df["pubmed_id"] = df["publication_source"].str.extract(r"^PUBMED:(\d+)$", expand=False)
        df = df[list(INTERACTIONS_SCHEMA)]
        self.save_parquet(df, self.intermediate_path() / "interactions.parquet")
        print(
            f"[{self.name}] interactions: {len(df):,} "
            f"({df['experimental_system_type'].eq('physical').sum():,} physical, "
            f"{df['pubmed_id'].notna().sum():,} with pubmed)"
        )

    def transform(self) -> None:
        # Faithful projection; reading/normalisation in extract, validation in load.
        pass

    def load(self) -> None:
        df = self.load_parquet(self.intermediate_path() / "interactions.parquet")
        df = df[list(INTERACTIONS_SCHEMA)]
        df["score"] = df["score"].astype("Float64")
        df = self.apply_schema(df, INTERACTIONS_SCHEMA)
        self.save_parquet(df, self.output_path() / "interactions.parquet")
        self.save_schema_yaml(INTERACTIONS_SCHEMA, "interactions")
