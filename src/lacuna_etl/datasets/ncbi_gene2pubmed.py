import pandas as pd

from lacuna_etl.core.identifiers import NcbiGeneId, NcbiTaxId, PubmedId
from lacuna_etl.core.pipeline import DatasetPipeline
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.ncbi_gene_history import update_entrez_ids
from lacuna_etl.datasets.registry import register

SCHEMA = {
    "tax_id":    ColumnSpec(identifier=NcbiTaxId,  description="NCBI taxonomy ID"),
    "entrez_id": ColumnSpec(identifier=NcbiGeneId, description="NCBI/Entrez Gene ID"),
    "pubmed_id": ColumnSpec(identifier=PubmedId,   description="PubMed article ID"),
}


@register
class NcbiGene2Pubmed(DatasetPipeline):
    name = "ncbi_gene2pubmed"
    depends_on = ["ncbi_gene_history"]

    def extract(self) -> None:
        src = self.raw_path() / "gene2pubmed.gz"
        df = pd.read_csv(src, sep="\t")
        df.rename(columns={"#tax_id": "tax_id", "GeneID": "entrez_id", "PubMed_ID": "pubmed_id"}, inplace=True)
        df = self.apply_schema(df, SCHEMA)
        self.save_parquet(df, self.intermediate_path() / "gene2pubmed.parquet")

    def transform(self) -> None:
        df = self.load_parquet(self.intermediate_path() / "gene2pubmed.parquet")
        df["entrez_id"] = update_entrez_ids(df["entrez_id"])
        NcbiGeneId.validate(df["entrez_id"])
        self.save_parquet(df, self.intermediate_path() / "gene2pubmed_transformed.parquet")

    def load(self) -> None:
        df = self.load_parquet(self.intermediate_path() / "gene2pubmed_transformed.parquet")
        self.save_parquet(df, self.output_path() / "gene2pubmed.parquet")
        self.save_schema_yaml(SCHEMA, "gene2pubmed")
