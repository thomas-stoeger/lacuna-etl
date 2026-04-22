import pandas as pd

from lacuna_etl.core.identifiers import NcbiGeneId, NcbiTaxId
from lacuna_etl.core.pipeline import DatasetPipeline
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.registry import register

_RENAME = {
    "#tax_id": "tax_id",
    "GeneID": "gene_id",
    "Discontinued_GeneID": "discontinued_gene_id",
    "Discontinued_Symbol": "discontinued_symbol",
    "Discontinue_Date": "discontinue_date",
}

SCHEMA = {
    "tax_id": ColumnSpec(identifier=NcbiTaxId, description="NCBI taxonomy ID"),
    "gene_id": ColumnSpec(identifier=NcbiGeneId, description="Entrez Gene ID that replaced the discontinued gene; null if discontinued without replacement"),
    "discontinued_gene_id": ColumnSpec(identifier=NcbiGeneId, description="Discontinued Entrez Gene ID"),
    "discontinued_symbol": ColumnSpec(description="Gene symbol used by the discontinued gene"),
    "discontinue_date": ColumnSpec(description="Date the gene ID was discontinued"),
}


@register
class NcbiGeneHistory(DatasetPipeline):
    name = "ncbi_gene_history"

    def extract(self) -> None:
        src = self.raw_path() / "gene_history.gz"
        df = pd.read_csv(src, sep="\t", na_values=["-"], dtype=str)
        df.rename(columns=_RENAME, inplace=True)

        df["tax_id"] = NcbiTaxId.cast(df["tax_id"])
        NcbiTaxId.validate(df["tax_id"])

        df["discontinued_gene_id"] = NcbiGeneId.cast(df["discontinued_gene_id"])
        NcbiGeneId.validate(df["discontinued_gene_id"])

        # gene_id is null when a gene is discontinued without a replacement
        df["gene_id"] = NcbiGeneId.cast(df["gene_id"])
        non_null = df["gene_id"].dropna()
        if (non_null <= 0).any():
            raise ValueError("gene_id: non-null values must be positive")

        df["discontinue_date"] = pd.to_datetime(df["discontinue_date"], format="%Y%m%d")

        self.save_parquet(df, self.intermediate_path() / "gene_history.parquet")

    def transform(self) -> None:
        df = self.load_parquet(self.intermediate_path() / "gene_history.parquet")
        df = df[list(SCHEMA.keys())]
        self.save_parquet(df, self.intermediate_path() / "gene_history_transformed.parquet")

    def load(self) -> None:
        df = self.load_parquet(self.intermediate_path() / "gene_history_transformed.parquet")
        self.save_parquet(df, self.output_path() / "gene_history.parquet")
        self.save_schema_yaml(SCHEMA, "gene_history")
