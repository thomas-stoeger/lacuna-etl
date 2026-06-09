"""GWAS Catalog — the NHGRI-EBI catalog of published genome-wide association studies.

The download is a small set of tab-delimited files: the study list
(``gwas-catalog-studies.tsv``), the per-study ancestry breakdown
(``gwas-catalog-ancestry.tsv``), the trait→EFO mappings
(``gwas-efo-trait-mappings.tsv``), and the zipped, ontology-annotated association
table (~1.14M SNP-trait associations). All read comfortably with pandas, so this is
an in-memory pipeline: ``extract`` reads each file, renames source headers to
snake_case, and normalises empties to null; ``transform`` is a no-op; ``load`` types,
validates, and writes one table per file.

The cross-file key is the study accession (``GwasStudyAccession``, ``GCST…``), which
keys the ``ancestry`` and ``associations`` tables. NOTE: this `studies` export does
not carry the accession column, so the ``studies`` table is a faithful projection
without it (typed only on ``pubmed_id``). ``pubmed_id`` is ``PubmedId`` throughout. The
``associations`` table is kept as a faithful 38-column projection; the SNP/gene columns
are heterogeneous, often multi-valued strings (kept verbatim), and only the three
clearly-numeric statistics (``p_value``, ``pvalue_mlog``, ``or_or_beta``) are typed
``Float64``. ``mapped_trait_uri`` mixes EFO with Orphanet/HP/MONDO, so it is a plain
string; only the trait-mapping file's confirmed ``EFO_`` term gets ``EfoId``. See
docs/DESIGN.md for the table inventory.
"""
from __future__ import annotations

import csv
import re
from pathlib import Path

import pandas as pd

from lacuna_etl.core.identifiers import EfoId, GwasStudyAccession, PubmedId
from lacuna_etl.core.pipeline import DatasetPipeline
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.registry import register

_EFO_URI_RE = re.compile(r"EFO_(\d+)$")

# Each table: ordered list of snake_case column names matching the source header
# left-to-right, plus the SCHEMA. The source headers are messy (spaces, brackets,
# slashes), so we override them positionally rather than rename by string.
STUDIES_COLUMNS = [
    "date_added", "pubmed_id", "first_author", "publication_date", "journal", "link",
    "study", "disease_trait", "initial_sample_size", "replication_sample_size",
    "platform", "association_count",
]
STUDIES_SCHEMA = {
    "date_added": ColumnSpec(description="Date the study was added to the catalog"),
    "pubmed_id": ColumnSpec(identifier=PubmedId, required=True, description="PubMed id of the publication"),
    "first_author": ColumnSpec(description="First author of the publication"),
    "publication_date": ColumnSpec(description="Publication date"),
    "journal": ColumnSpec(description="Journal name"),
    "link": ColumnSpec(description="Link to the publication (Europe PMC)"),
    "study": ColumnSpec(description="Study title / description"),
    "disease_trait": ColumnSpec(description="Reported disease or trait"),
    "initial_sample_size": ColumnSpec(description="Free-text description of the initial (discovery) sample"),
    "replication_sample_size": ColumnSpec(description="Free-text description of the replication sample"),
    "platform": ColumnSpec(description="Genotyping platform and SNP count (the source 'PLATFORM [SNPS PASSING QC]')"),
    "association_count": ColumnSpec(description="Number of associations reported for the study (Int64)"),
}

ANCESTRY_COLUMNS = [
    "study_accession", "pubmed_id", "first_author", "date", "initial_sample_description",
    "replication_sample_description", "stage", "number_of_individuals",
    "broad_ancestral_category", "country_of_origin", "country_of_recruitment",
    "additional_ancestry_description",
]
ANCESTRY_SCHEMA = {
    "study_accession": ColumnSpec(identifier=GwasStudyAccession, required=True, description="GWAS Catalog study accession (GCST…); the grain key"),
    "pubmed_id": ColumnSpec(identifier=PubmedId, description="PubMed id of the publication"),
    "first_author": ColumnSpec(description="First author of the publication"),
    "date": ColumnSpec(description="Publication date"),
    "initial_sample_description": ColumnSpec(description="Free-text description of the initial (discovery) sample"),
    "replication_sample_description": ColumnSpec(description="Free-text description of the replication sample"),
    "stage": ColumnSpec(description="Study stage (initial / replication)"),
    "number_of_individuals": ColumnSpec(description="Number of individuals in this ancestry group (Int64)"),
    "broad_ancestral_category": ColumnSpec(description="Broad ancestral category (e.g. European, East Asian)"),
    "country_of_origin": ColumnSpec(description="Country/countries of origin of the sample"),
    "country_of_recruitment": ColumnSpec(description="Country/countries of recruitment of the sample"),
    "additional_ancestry_description": ColumnSpec(description="Additional free-text ancestry description"),
}

EFO_COLUMNS = ["disease_trait", "efo_term", "efo_uri", "parent_term", "parent_uri"]
EFO_SCHEMA = {
    "disease_trait": ColumnSpec(required=True, description="Reported disease/trait string"),
    "efo_term": ColumnSpec(description="Mapped EFO term label"),
    "efo_uri": ColumnSpec(description="Full URI of the mapped ontology term (EFO, or Orphanet/HP/MONDO)"),
    "efo_id": ColumnSpec(identifier=EfoId, description="EFO term CURIE (EFO:N) derived from efo_uri; null when the mapped term is not an EFO_ term"),
    "parent_term": ColumnSpec(description="Parent EFO term label"),
    "parent_uri": ColumnSpec(description="Full URI of the parent term (heterogeneous; kept verbatim)"),
}

ASSOCIATIONS_COLUMNS = [
    "date_added", "pubmed_id", "first_author", "publication_date", "journal", "link",
    "study", "disease_trait", "initial_sample_size", "replication_sample_size", "region",
    "chr_id", "chr_pos", "reported_genes", "mapped_gene", "upstream_gene_id",
    "downstream_gene_id", "snp_gene_ids", "upstream_gene_distance", "downstream_gene_distance",
    "strongest_snp_risk_allele", "snps", "merged", "snp_id_current", "context", "intergenic",
    "risk_allele_frequency", "p_value", "pvalue_mlog", "p_value_text", "or_or_beta",
    "ci_text", "platform", "cnv", "mapped_trait", "mapped_trait_uri", "study_accession",
    "genotyping_technology",
]
ASSOCIATIONS_SCHEMA = {
    "date_added": ColumnSpec(description="Date the association was added to the catalog"),
    "pubmed_id": ColumnSpec(identifier=PubmedId, required=True, description="PubMed id of the publication"),
    "first_author": ColumnSpec(description="First author of the publication"),
    "publication_date": ColumnSpec(description="Publication date"),
    "journal": ColumnSpec(description="Journal name"),
    "link": ColumnSpec(description="Link to the publication (Europe PMC)"),
    "study": ColumnSpec(description="Study title / description"),
    "disease_trait": ColumnSpec(description="Reported disease or trait"),
    "initial_sample_size": ColumnSpec(description="Free-text description of the initial sample"),
    "replication_sample_size": ColumnSpec(description="Free-text description of the replication sample"),
    "region": ColumnSpec(description="Cytogenetic region"),
    "chr_id": ColumnSpec(description="Chromosome name (may be multi-valued)"),
    "chr_pos": ColumnSpec(description="Chromosome base-pair position (may be multi-valued)"),
    "reported_genes": ColumnSpec(description="Gene(s) reported by the authors (free text, may be multi-valued)"),
    "mapped_gene": ColumnSpec(description="Gene(s) mapped to the strongest SNP (may be multi-valued)"),
    "upstream_gene_id": ColumnSpec(description="Ensembl id of the nearest upstream gene (plain string)"),
    "downstream_gene_id": ColumnSpec(description="Ensembl id of the nearest downstream gene (plain string)"),
    "snp_gene_ids": ColumnSpec(description="Ensembl id(s) of gene(s) the SNP falls in (plain string, may be multi-valued)"),
    "upstream_gene_distance": ColumnSpec(description="Distance to the nearest upstream gene (bp)"),
    "downstream_gene_distance": ColumnSpec(description="Distance to the nearest downstream gene (bp)"),
    "strongest_snp_risk_allele": ColumnSpec(description="Strongest SNP and its risk allele (rsID-allele)"),
    "snps": ColumnSpec(description="SNP rsID(s) (may be multi-valued)"),
    "merged": ColumnSpec(description="Whether the SNP has been merged (source flag, kept verbatim)"),
    "snp_id_current": ColumnSpec(description="Current rsID number"),
    "context": ColumnSpec(description="Functional class / variant context"),
    "intergenic": ColumnSpec(description="Whether the SNP is intergenic (source flag, kept verbatim)"),
    "risk_allele_frequency": ColumnSpec(description="Reported risk allele frequency (free text: number, 'NR', or range)"),
    "p_value": ColumnSpec(description="Association p-value (Float64)"),
    "pvalue_mlog": ColumnSpec(description="-log10(p-value) (Float64)"),
    "p_value_text": ColumnSpec(description="Additional p-value annotation (free text)"),
    "or_or_beta": ColumnSpec(description="Odds ratio or beta-coefficient (Float64; 'Infinity' preserved as inf)"),
    "ci_text": ColumnSpec(description="95% confidence interval text (the source '95% CI (TEXT)')"),
    "platform": ColumnSpec(description="Genotyping platform and SNP count"),
    "cnv": ColumnSpec(description="Whether the association is a copy-number variant (source flag)"),
    "mapped_trait": ColumnSpec(description="Mapped EFO trait label(s)"),
    "mapped_trait_uri": ColumnSpec(description="Mapped trait URI(s); mixes EFO with Orphanet/HP/MONDO, so a plain string"),
    "study_accession": ColumnSpec(identifier=GwasStudyAccession, required=True, description="GWAS Catalog study accession (GCST…)"),
    "genotyping_technology": ColumnSpec(description="Genotyping technology used"),
}

_INT_COLS = {"studies": ["association_count"], "ancestry": ["number_of_individuals"]}
_FLOAT_COLS = {"associations": ["p_value", "pvalue_mlog", "or_or_beta"]}


def _efo_id(uri: str | None) -> str | None:
    """Derive the EFO CURIE (EFO:N) from a term URI, else None (non-EFO ontology)."""
    if uri is None:
        return None
    m = _EFO_URI_RE.search(uri)
    return f"EFO:{m.group(1)}" if m else None


@register
class GwasCatalog(DatasetPipeline):
    name = "gwas_catalog"

    _TABLES = [
        ("studies", STUDIES_SCHEMA),
        ("ancestry", ANCESTRY_SCHEMA),
        ("efo_trait_mappings", EFO_SCHEMA),
        ("associations", ASSOCIATIONS_SCHEMA),
    ]
    _SOURCES = {
        "studies": ("gwas-catalog-studies.tsv", STUDIES_COLUMNS),
        "ancestry": ("gwas-catalog-ancestry.tsv", ANCESTRY_COLUMNS),
        "efo_trait_mappings": ("gwas-efo-trait-mappings.tsv", EFO_COLUMNS),
        "associations": ("gwas-catalog-associations_ontology-annotated-full.zip", ASSOCIATIONS_COLUMNS),
    }

    def _read(self, filename: str, columns: list[str]) -> pd.DataFrame:
        path = self.raw_path() / filename
        if not path.exists():
            raise FileNotFoundError(f"gwas_catalog: expected {filename} under {self.raw_path()}")
        kwargs = dict(
            sep="\t", header=0, dtype=str, keep_default_na=False, na_filter=False,
            quoting=csv.QUOTE_NONE,
        )
        if path.suffix == ".zip":
            kwargs["compression"] = "zip"
        df = pd.read_csv(path, **kwargs)
        if df.shape[1] != len(columns):
            raise ValueError(f"gwas_catalog: {filename} has {df.shape[1]} columns, expected {len(columns)}")
        df.columns = columns
        for c in columns:
            df[c] = df[c].str.strip()
            df[c] = df[c].where(df[c] != "", None)
        return df

    def extract(self) -> None:
        counts = {}
        for stem, schema in self._TABLES:
            filename, columns = self._SOURCES[stem]
            df = self._read(filename, columns)
            if stem == "efo_trait_mappings":
                df["efo_id"] = df["efo_uri"].map(_efo_id)
                df = df[list(schema)]
            self.save_parquet(df, self.intermediate_path() / f"{stem}.parquet")
            counts[stem] = len(df)
        print("[gwas_catalog] " + ", ".join(f"{k}: {v:,}" for k, v in counts.items()))

    def transform(self) -> None:
        # Faithful projection; reading/normalisation in extract, validation in load.
        pass

    def load(self) -> None:
        for stem, schema in self._TABLES:
            df = self.load_parquet(self.intermediate_path() / f"{stem}.parquet")
            df = df[list(schema)]
            for col in _INT_COLS.get(stem, []):
                df[col] = df[col].astype("Int64")
            for col in _FLOAT_COLS.get(stem, []):
                df[col] = df[col].astype("Float64")
            df = self.apply_schema(df, schema)
            self.save_parquet(df, self.output_path() / f"{stem}.parquet")
            self.save_schema_yaml(schema, stem)
