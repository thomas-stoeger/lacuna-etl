import pandas as pd

from lacuna_etl.core.identifiers import NcbiGeneId, NcbiTaxId, RefSeqAccession
from lacuna_etl.core.pipeline import DatasetPipeline
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.ncbi_gene_history import update_entrez_ids
from lacuna_etl.datasets.registry import register

# NCBI's gene2ensembl is genome-wide; we keep only the reference model organisms
# the rest of this project is scoped to (the same eight as ensembl_tsv). Worm and
# yeast happen to be absent from the current snapshot, which is fine — the filter
# keeps whatever of the set is present.
MODEL_ORGANISM_TAX_IDS = {
    9606,    # Homo sapiens
    10090,   # Mus musculus
    10116,   # Rattus norvegicus
    7955,    # Danio rerio
    7227,    # Drosophila melanogaster
    6239,    # Caenorhabditis elegans
    559292,  # Saccharomyces cerevisiae S288C
    9031,    # Gallus gallus
}

SCHEMA = {
    "tax_id":                   ColumnSpec(identifier=NcbiTaxId,  description="NCBI taxonomy ID"),
    "entrez_id":                ColumnSpec(identifier=NcbiGeneId, description="NCBI/Entrez Gene ID"),
    "ensembl_gene_id":          ColumnSpec(description="Ensembl gene stable ID; species-native (ENS…G for vertebrates, FlyBase FBgn for fly), so not typed"),
    "refseq_rna_accession":     ColumnSpec(identifier=RefSeqAccession, description="RefSeq RNA accession (with version) matched to the Ensembl transcript; null where none"),
    "ensembl_transcript_id":    ColumnSpec(description="Ensembl transcript stable ID, version stripped; species-native (ENS…T / FlyBase FBtr), so not typed; null where none"),
    "refseq_protein_accession": ColumnSpec(identifier=RefSeqAccession, description="RefSeq protein accession (with version) matched to the Ensembl protein; null where none"),
    "ensembl_protein_id":       ColumnSpec(description="Ensembl protein stable ID, version stripped; species-native (ENS…P / FlyBase FBpp), so not typed; null where none"),
}

_RENAME = {
    "#tax_id": "tax_id",
    "GeneID": "entrez_id",
    "Ensembl_gene_identifier": "ensembl_gene_id",
    "RNA_nucleotide_accession.version": "refseq_rna_accession",
    "Ensembl_rna_identifier": "ensembl_transcript_id",
    "protein_accession.version": "refseq_protein_accession",
    "Ensembl_protein_identifier": "ensembl_protein_id",
}


@register
class NcbiGene2Ensembl(DatasetPipeline):
    name = "ncbi_gene2ensembl"
    depends_on = ["ncbi_gene_history"]

    def extract(self) -> None:
        src = self.raw_path() / "gene2ensembl.gz"
        df = pd.read_csv(src, sep="\t", dtype=str, na_values=["-"])
        df.rename(columns=_RENAME, inplace=True)
        df = df[df["tax_id"].isin({str(t) for t in MODEL_ORGANISM_TAX_IDS})]
        df = self.apply_schema(df[list(SCHEMA)], SCHEMA)
        self.save_parquet(df, self.intermediate_path() / "gene2ensembl.parquet")

    def transform(self) -> None:
        df = self.load_parquet(self.intermediate_path() / "gene2ensembl.parquet")
        df["entrez_id"] = update_entrez_ids(df["entrez_id"])
        NcbiGeneId.validate(df["entrez_id"])
        # Ensembl stable IDs are kept unversioned (matching EnsemblGeneId's canonical
        # form and the ensembl_tsv dump); RefSeq accessions keep their version.
        for col in ("ensembl_transcript_id", "ensembl_protein_id"):
            df[col] = df[col].str.replace(r"\.\d+$", "", regex=True)
        self.save_parquet(df, self.intermediate_path() / "gene2ensembl_transformed.parquet")

    def load(self) -> None:
        df = self.load_parquet(self.intermediate_path() / "gene2ensembl_transformed.parquet")
        self.save_parquet(df, self.output_path() / "gene2ensembl.parquet")
        self.save_schema_yaml(SCHEMA, "gene2ensembl")
