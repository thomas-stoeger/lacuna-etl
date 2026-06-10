import pandas as pd

from lacuna_etl.core.identifiers import NcbiGeneId, NcbiTaxId, PubmedId
from lacuna_etl.core.pipeline import DatasetPipeline
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.ncbi_gene_history import update_entrez_ids
from lacuna_etl.datasets.registry import register

_EXTRACT_SCHEMA = {
    "tax_id":    ColumnSpec(identifier=NcbiTaxId,  description="NCBI taxonomy ID"),
    "entrez_id": ColumnSpec(identifier=NcbiGeneId, description="NCBI/Entrez Gene ID"),
    "pubmed_ids": ColumnSpec(description="Pipe-separated PubMed IDs supporting this RIF"),
    "last_update": ColumnSpec(description="Date the RIF was last updated"),
    "rif_text":  ColumnSpec(description="GeneRIF free-text functional annotation"),
}

SCHEMA = {
    "tax_id":    ColumnSpec(identifier=NcbiTaxId,  description="NCBI taxonomy ID"),
    "entrez_id": ColumnSpec(identifier=NcbiGeneId, description="NCBI/Entrez Gene ID"),
    "pubmed_id": ColumnSpec(identifier=PubmedId,   description="PubMed article ID"),
    "last_update": ColumnSpec(description="Date the RIF was last updated"),
    "rif_text":  ColumnSpec(description="GeneRIF free-text functional annotation"),
}

_RENAME = {
    "#Tax ID": "tax_id",
    "Gene ID": "entrez_id",
    "PubMed ID (PMID) list": "pubmed_ids",
    "last update timestamp": "last_update",
    "GeneRIF text": "rif_text",
}


@register
class NcbiGeneRif(DatasetPipeline):
    name = "ncbi_generifs"
    _TABLES = [("gene_rif", SCHEMA)]
    depends_on = ["ncbi_gene_history"]

    def extract(self) -> None:
        src = self.raw_path() / "generifs_basic.gz"
        df = pd.read_csv(src, sep="\t", dtype=str)
        df.rename(columns=_RENAME, inplace=True)
        df = self.apply_schema(df, _EXTRACT_SCHEMA)
        df["last_update"] = pd.to_datetime(df["last_update"], format="%Y-%m-%d %H:%M")
        self.save_parquet(df, self.intermediate_path() / "gene_rif.parquet")

    def transform(self) -> None:
        df = self.load_parquet(self.intermediate_path() / "gene_rif.parquet")
        df["pubmed_ids"] = df["pubmed_ids"].str.split(r"[|,]")
        df = df.explode("pubmed_ids").rename(columns={"pubmed_ids": "pubmed_id"})
        df["entrez_id"] = update_entrez_ids(df["entrez_id"])
        NcbiGeneId.validate(df["entrez_id"])
        df["pubmed_id"] = PubmedId.cast(df["pubmed_id"])
        PubmedId.validate(df["pubmed_id"])
        self.save_parquet(df, self.intermediate_path() / "gene_rif_transformed.parquet")

    def load(self) -> None:
        df = self.load_parquet(self.intermediate_path() / "gene_rif_transformed.parquet")
        self.save_parquet(df, self.output_path() / "gene_rif.parquet")
        self.save_schema_yaml(SCHEMA, "gene_rif")
