import pandas as pd

from lacuna_etl.core.identifiers import GoId, NcbiGeneId, NcbiTaxId, PubmedId
from lacuna_etl.core.pipeline import DatasetPipeline
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.ncbi_gene_history import update_entrez_ids
from lacuna_etl.datasets.registry import register

_EXTRACT_SCHEMA = {
    "tax_id":     ColumnSpec(identifier=NcbiTaxId,  description="NCBI taxonomy ID"),
    "entrez_id":  ColumnSpec(identifier=NcbiGeneId, description="NCBI/Entrez Gene ID"),
    "go_id":      ColumnSpec(identifier=GoId,       description="Gene Ontology term ID"),
    "evidence":   ColumnSpec(description="GO evidence code (e.g. IEA, IDA, IMP, TAS)"),
    "qualifier":  ColumnSpec(description="Qualifier(s) for the GO assignment, e.g. 'enables', 'NOT|involved_in'; pipe-separated when multi-valued"),
    "go_term":    ColumnSpec(description="Human-readable GO term name"),
    "pubmed_ids": ColumnSpec(description="Pipe-separated PubMed IDs supporting this annotation; null for electronic/IEA annotations"),
    "category":   ColumnSpec(description="GO aspect: Function, Process, or Component", allowed_values={"Function", "Process", "Component"}),
}

SCHEMA = {
    "tax_id":    ColumnSpec(identifier=NcbiTaxId,  description="NCBI taxonomy ID"),
    "entrez_id": ColumnSpec(identifier=NcbiGeneId, description="NCBI/Entrez Gene ID"),
    "go_id":     ColumnSpec(identifier=GoId,       description="Gene Ontology term ID"),
    "evidence":  ColumnSpec(description="GO evidence code (e.g. IEA, IDA, IMP, TAS)"),
    "qualifier": ColumnSpec(description="Qualifier(s) for the GO assignment, e.g. 'enables', 'NOT|involved_in'; pipe-separated when multi-valued"),
    "go_term":   ColumnSpec(description="Human-readable GO term name"),
    "pubmed_id": ColumnSpec(identifier=PubmedId,   description="PubMed article ID supporting this annotation; null for electronic/IEA annotations"),
    "category":  ColumnSpec(description="GO aspect: Function, Process, or Component", allowed_values={"Function", "Process", "Component"}),
}

_RENAME = {
    "#tax_id": "tax_id",
    "GeneID": "entrez_id",
    "GO_ID": "go_id",
    "Evidence": "evidence",
    "Qualifier": "qualifier",
    "GO_term": "go_term",
    "PubMed": "pubmed_ids",
    "Category": "category",
}


@register
class NcbiGene2Go(DatasetPipeline):
    name = "ncbi_gene2go"
    depends_on = ["ncbi_gene_history"]

    def extract(self) -> None:
        src = self.raw_path() / "gene2go.gz"
        df = pd.read_csv(src, sep="\t", dtype=str, na_values=["-"])
        df.rename(columns=_RENAME, inplace=True)
        df = self.apply_schema(df, _EXTRACT_SCHEMA)
        self.save_parquet(df, self.intermediate_path() / "gene2go.parquet")

    def transform(self) -> None:
        df = self.load_parquet(self.intermediate_path() / "gene2go.parquet")
        df["pubmed_ids"] = df["pubmed_ids"].str.split(r"[|,]")
        df = df.explode("pubmed_ids").rename(columns={"pubmed_ids": "pubmed_id"})
        df["entrez_id"] = update_entrez_ids(df["entrez_id"])
        NcbiGeneId.validate(df["entrez_id"])
        df["pubmed_id"] = PubmedId.cast(df["pubmed_id"])
        # pubmed_id is nullable here: IEA/electronic annotations carry no PubMed support
        PubmedId.validate(df["pubmed_id"].dropna())
        self.save_parquet(df, self.intermediate_path() / "gene2go_transformed.parquet")

    def load(self) -> None:
        df = self.load_parquet(self.intermediate_path() / "gene2go_transformed.parquet")
        self.save_parquet(df, self.output_path() / "gene2go.parquet")
        self.save_schema_yaml(SCHEMA, "gene2go")
