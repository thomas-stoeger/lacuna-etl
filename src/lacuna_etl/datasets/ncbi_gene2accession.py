import pandas as pd
import polars as pl
import pyarrow.parquet as pq

from lacuna_etl.config import get_output_root
from lacuna_etl.core.identifiers import NcbiGeneId, NcbiTaxId
from lacuna_etl.core.pipeline import DatasetPipeline
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.ncbi_gene_history import update_entrez_ids
from lacuna_etl.datasets.registry import register

# gene2accession is genome-wide and large (~4 GB gzipped); we keep only the
# reference model organisms this project is scoped to. Both yeast taxa appear in
# the source (4932 the species node and 559292 the S288C reference strain), as do
# all eight of the other organisms.
MODEL_ORGANISM_TAX_IDS = {
    9606,    # Homo sapiens
    10090,   # Mus musculus
    10116,   # Rattus norvegicus
    7955,    # Danio rerio
    7227,    # Drosophila melanogaster
    6239,    # Caenorhabditis elegans
    559292,  # Saccharomyces cerevisiae S288C
    4932,    # Saccharomyces cerevisiae (species node)
    9031,    # Gallus gallus
}

# Curation status (RefSeq lifecycle) and genomic strand: both use "-" as a real
# value ("-" status = a non-RefSeq/GenBank row; "-" orientation = the minus
# strand), so "-" is NOT nulled for these two columns. Value sets enumerated from
# the model-organism subset of the current snapshot.
_STATUS_VALUES = {"-", "REVIEWED", "VALIDATED", "PROVISIONAL", "MODEL",
                  "INFERRED", "PREDICTED", "NA", "SUPPRESSED"}
_ORIENTATION_VALUES = {"+", "-", "?"}

SCHEMA = {
    "tax_id":                       ColumnSpec(identifier=NcbiTaxId,  description="NCBI taxonomy ID"),
    "entrez_id":                    ColumnSpec(identifier=NcbiGeneId, description="NCBI/Entrez Gene ID"),
    "status":                       ColumnSpec(description="RefSeq curation status; '-' for non-RefSeq (GenBank) rows", allowed_values=_STATUS_VALUES),
    "rna_nucleotide_accession":     ColumnSpec(description="RNA nucleotide accession.version; mixes RefSeq (NM_/NR_/XM_/XR_) and GenBank (e.g. AK036727.1), so a plain string"),
    "rna_nucleotide_gi":            ColumnSpec(description="NCBI GI for the RNA nucleotide accession (integer)"),
    "protein_accession":            ColumnSpec(description="Protein accession.version; mixes RefSeq (NP_/XP_/YP_) and GenBank (e.g. BAN84074.1), so a plain string"),
    "protein_gi":                   ColumnSpec(description="NCBI GI for the protein accession (integer)"),
    "genomic_nucleotide_accession": ColumnSpec(description="Genomic nucleotide accession.version; mixes RefSeq (NC_/NT_/NW_), WGS (NZ_…), and GenBank, so a plain string"),
    "genomic_nucleotide_gi":        ColumnSpec(description="NCBI GI for the genomic nucleotide accession (integer)"),
    "genomic_start":                ColumnSpec(description="Start position on the genomic accession (1-based, integer)"),
    "genomic_end":                  ColumnSpec(description="End position on the genomic accession (1-based, integer)"),
    "orientation":                  ColumnSpec(description="Strand of the gene on the genomic accession", allowed_values=_ORIENTATION_VALUES),
    "assembly":                     ColumnSpec(description="Assembly the genomic placement is from (e.g. GRCh38.p14)"),
    "mature_peptide_accession":     ColumnSpec(description="Mature peptide accession.version where annotated; mixes RefSeq and GenBank, so a plain string"),
    "mature_peptide_gi":            ColumnSpec(description="NCBI GI for the mature peptide accession (integer)"),
    "symbol":                       ColumnSpec(description="Gene symbol as reported in gene2accession"),
}

_RENAME = {
    "#tax_id": "tax_id",
    "GeneID": "entrez_id",
    "status": "status",
    "RNA_nucleotide_accession.version": "rna_nucleotide_accession",
    "RNA_nucleotide_gi": "rna_nucleotide_gi",
    "protein_accession.version": "protein_accession",
    "protein_gi": "protein_gi",
    "genomic_nucleotide_accession.version": "genomic_nucleotide_accession",
    "genomic_nucleotide_gi": "genomic_nucleotide_gi",
    "start_position_on_the_genomic_accession": "genomic_start",
    "end_position_on_the_genomic_accession": "genomic_end",
    "orientation": "orientation",
    "assembly": "assembly",
    "mature_peptide_accession.version": "mature_peptide_accession",
    "mature_peptide_gi": "mature_peptide_gi",
    "Symbol": "symbol",
}

# "-" is the source's missing marker everywhere EXCEPT status/orientation.
_NULL_DASH_COLS = [c for c in SCHEMA if c not in ("tax_id", "entrez_id", "status", "orientation")]
_INT_COLS = ["rna_nucleotide_gi", "protein_gi", "genomic_nucleotide_gi",
             "genomic_start", "genomic_end", "mature_peptide_gi"]


@register
class NcbiGene2Accession(DatasetPipeline):
    name = "ncbi_gene2accession"
    depends_on = ["ncbi_gene_history"]

    def extract(self) -> None:
        # Restartable: the filter over the ~4 GB source is the expensive step, so
        # skip it once the filtered intermediate exists.
        out = self.intermediate_path() / "gene2accession_filtered.parquet"
        if out.exists():
            print(f"[{self.name}] filtered intermediate exists, skipping read")
            return
        src = self.raw_path() / "gene2accession.gz"
        # Read every column as Utf8 (the file has no quoting and GI values would
        # otherwise force noisy dtype inference), then keep only the model taxa.
        df = pl.read_csv(src, separator="\t", has_header=True,
                         infer_schema_length=0, quote_char=None)
        df = df.rename(_RENAME)
        df = df.filter(pl.col("tax_id").cast(pl.Int64).is_in(list(MODEL_ORGANISM_TAX_IDS)))
        df.select(list(SCHEMA)).write_parquet(out)
        print(f"[{self.name}] filtered to {df.height:,} model-organism rows")

    def transform(self) -> None:
        df = pl.read_parquet(self.intermediate_path() / "gene2accession_filtered.parquet").to_pandas()

        # "-"/blank → null, except for the status/orientation categoricals.
        df[_NULL_DASH_COLS] = df[_NULL_DASH_COLS].replace({"-": pd.NA, "": pd.NA})

        df["tax_id"] = NcbiTaxId.cast(df["tax_id"])
        NcbiTaxId.validate(df["tax_id"])

        df["entrez_id"] = NcbiGeneId.cast(df["entrez_id"])
        # Unlike the current gene list, an accession dump can still reference a gene
        # that has since been discontinued *without* a replacement (a true deletion,
        # not a merge). Those rows point at a gene that no longer exists, so drop
        # them — reported, not silent — before remapping the (merge-)replaceable IDs.
        hist = pq.read_table(
            str(get_output_root() / "ncbi_gene_history" / "gene_history.parquet"),
            columns=["gene_id", "discontinued_gene_id"],
        ).to_pandas()
        deleted = set(hist.loc[hist["gene_id"].isna(), "discontinued_gene_id"].dropna())
        is_deleted = df["entrez_id"].isin(deleted)
        if is_deleted.any():
            print(f"[{self.name}] dropping {int(is_deleted.sum())} rows "
                  f"({df.loc[is_deleted, 'entrez_id'].nunique()} genes) discontinued without replacement")
            df = df[~is_deleted].copy()
        df["entrez_id"] = update_entrez_ids(df["entrez_id"])
        NcbiGeneId.validate(df["entrez_id"])

        for col in _INT_COLS:
            df[col] = pd.to_numeric(df[col]).astype("Int64")

        # status/orientation keep "-"; validate their value sets.
        SCHEMA["status"].validate(df["status"])
        SCHEMA["orientation"].validate(df["orientation"])

        self.save_parquet(df[list(SCHEMA)], self.intermediate_path() / "gene2accession_transformed.parquet")

    def load(self) -> None:
        df = self.load_parquet(self.intermediate_path() / "gene2accession_transformed.parquet")
        self.save_parquet(df, self.output_path() / "gene2accession.parquet")
        self.save_schema_yaml(SCHEMA, "gene2accession")
