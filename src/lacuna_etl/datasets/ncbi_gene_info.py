import pandas as pd

from lacuna_etl.core.identifiers import NcbiGeneId, NcbiTaxId
from lacuna_etl.core.pipeline import DatasetPipeline
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.ncbi_gene_history import update_entrez_ids
from lacuna_etl.datasets.registry import register

SCHEMA = {
    "tax_id":                              ColumnSpec(identifier=NcbiTaxId,  description="NCBI taxonomy ID"),
    "entrez_id":                           ColumnSpec(identifier=NcbiGeneId, description="NCBI/Entrez Gene ID"),
    "symbol":                              ColumnSpec(description="NCBI gene symbol"),
    "locus_tag":                           ColumnSpec(description="Locus tag"),
    "synonyms":                            ColumnSpec(description="Pipe-separated gene symbol synonyms"),
    "db_xrefs":                            ColumnSpec(description="Pipe-separated cross-references to other databases"),
    "chromosome":                          ColumnSpec(description="Chromosome"),
    "map_location":                        ColumnSpec(description="Cytogenetic map location"),
    "description":                         ColumnSpec(description="Gene description"),
    "type_of_gene":                        ColumnSpec(description="Gene type (protein-coding, ncRNA, etc.)"),
    "symbol_from_nomenclature_authority":  ColumnSpec(description="Symbol from nomenclature authority (e.g. HGNC)"),
    "full_name_from_nomenclature_authority": ColumnSpec(description="Full name from nomenclature authority"),
    "nomenclature_status":                 ColumnSpec(description="O=official, I=interim", allowed_values={"O", "I"}),
    "other_designations":                  ColumnSpec(description="Pipe-separated alternative descriptions"),
    "modification_date":                   ColumnSpec(description="Last modification date"),
    "feature_type":                        ColumnSpec(description="Genomic feature type"),
}

_RENAME = {
    "#tax_id": "tax_id",
    "GeneID": "entrez_id",
    "Symbol": "symbol",
    "LocusTag": "locus_tag",
    "Synonyms": "synonyms",
    "dbXrefs": "db_xrefs",
    "chromosome": "chromosome",
    "map_location": "map_location",
    "description": "description",
    "type_of_gene": "type_of_gene",
    "Symbol_from_nomenclature_authority": "symbol_from_nomenclature_authority",
    "Full_name_from_nomenclature_authority": "full_name_from_nomenclature_authority",
    "Nomenclature_status": "nomenclature_status",
    "Other_designations": "other_designations",
    "Modification_date": "modification_date",
    "Feature_type": "feature_type",
}


@register
class NcbiGeneInfo(DatasetPipeline):
    name = "ncbi_gene_info"
    _TABLES = [("gene_info", SCHEMA)]
    depends_on = ["ncbi_gene_history"]

    def extract(self) -> None:
        src = self.raw_path() / "gene_info.gz"
        df = pd.read_csv(src, sep="\t", na_values=["-"], dtype=str)
        df.rename(columns=_RENAME, inplace=True)
        df = self.apply_schema(df, SCHEMA)
        df["modification_date"] = pd.to_datetime(df["modification_date"], format="%Y%m%d")
        self.save_parquet(df, self.intermediate_path() / "gene_info.parquet")

    def transform(self) -> None:
        df = self.load_parquet(self.intermediate_path() / "gene_info.parquet")
        df["entrez_id"] = update_entrez_ids(df["entrez_id"])
        NcbiGeneId.validate(df["entrez_id"])
        self.save_parquet(df, self.intermediate_path() / "gene_info_transformed.parquet")

    def load(self) -> None:
        df = self.load_parquet(self.intermediate_path() / "gene_info_transformed.parquet")
        self.save_parquet(df, self.output_path() / "gene_info.parquet")
        self.save_schema_yaml(SCHEMA, "gene_info")
