import pandas as pd

from lacuna_etl.core.identifiers import NcbiTaxId, RefSeqAccession
from lacuna_etl.core.pipeline import DatasetPipeline
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.registry import register

# Ensembl ships per-species "Stable ID to <db>" TSV dumps. The download was
# restricted upstream to these eight reference organisms; the directory name maps
# to the NCBI taxon (the yeast reference is S288C / 559292, matching the taxid
# Ensembl reports in its own ENA dump).
SPECIES_TAX_IDS = {
    "homo_sapiens":             9606,
    "mus_musculus":             10090,
    "rattus_norvegicus":        10116,
    "danio_rerio":              7955,
    "drosophila_melanogaster":  7227,
    "caenorhabditis_elegans":   6239,
    "saccharomyces_cerevisiae": 559292,
    "gallus_gallus":            9031,
}

# The gene/transcript/protein stable IDs are NOT uniformly Ensembl-form across
# these species — vertebrates use ENS…G/T/P, but yeast uses SGD systematic names
# (YDL246C), worm uses WormBase (WBGene…/F07C3.7.1), fly uses FlyBase (FBgn/FBtr/
# FBpp). So those columns stay plain strings rather than EnsemblGeneId etc.; only
# the cross-referenced identifier spaces that are uniform across species (Entrez
# Gene IDs, RefSeq accessions) are typed.
_ENSEMBL_ID_COLS = {
    "ensembl_gene_id":       ColumnSpec(description="Ensembl gene stable ID; species-native (ENS…G for vertebrates, SGD/WormBase/FlyBase IDs otherwise), so not typed"),
    "ensembl_transcript_id": ColumnSpec(description="Ensembl transcript stable ID; species-native, so not typed"),
    "ensembl_protein_id":    ColumnSpec(description="Ensembl protein/translation stable ID; species-native, so not typed; null where the row has no translation"),
}
_XREF_TAIL = {
    "db_name":         ColumnSpec(description="Ensembl external-database name for this cross-reference"),
    "info_type":       ColumnSpec(description="How the xref was assigned (e.g. DIRECT, DEPENDENT, SEQUENCE_MATCH)"),
    "source_identity": ColumnSpec(description="% identity of the Ensembl feature to the xref sequence, where applicable; null otherwise"),
    "xref_identity":   ColumnSpec(description="% identity of the xref sequence to the Ensembl feature, where applicable; null otherwise"),
    "linkage_type":    ColumnSpec(description="Ontology linkage type, where applicable; usually null"),
}

ENTREZ_SCHEMA = {
    "tax_id": ColumnSpec(identifier=NcbiTaxId, description="NCBI taxonomy ID of the species"),
    **_ENSEMBL_ID_COLS,
    "xref": ColumnSpec(description="NCBI Entrez Gene ID when db_name='EntrezGene'; an Entrez transcript name when db_name='EntrezGene_trans_name' (kept a plain string because the column mixes both)"),
    **_XREF_TAIL,
}

REFSEQ_SCHEMA = {
    "tax_id": ColumnSpec(identifier=NcbiTaxId, description="NCBI taxonomy ID of the species"),
    **_ENSEMBL_ID_COLS,
    "refseq_accession": ColumnSpec(identifier=RefSeqAccession, description="RefSeq transcript/protein accession (unversioned in this Ensembl dump); db_name gives the molecule type"),
    **_XREF_TAIL,
}

UNIPROT_SCHEMA = {
    "tax_id": ColumnSpec(identifier=NcbiTaxId, description="NCBI taxonomy ID of the species"),
    **_ENSEMBL_ID_COLS,
    "uniprot_accession": ColumnSpec(description="UniProt accession; db_name distinguishes SWISSPROT, SPTREMBL, and isoform xrefs (e.g. 'Q9HAZ2-2'), so it is a plain string"),
    **_XREF_TAIL,
}

ENA_SCHEMA = {
    "tax_id": ColumnSpec(identifier=NcbiTaxId, description="NCBI taxonomy ID reported by Ensembl in the ENA dump"),
    **_ENSEMBL_ID_COLS,
    "primary_accession":   ColumnSpec(description="ENA/INSDC primary accession the feature maps to (heterogeneous: clone/contig/chromosome labels appear), so a plain string"),
    "secondary_accession": ColumnSpec(description="ENA/INSDC secondary accession, where present"),
}

# Source-header → snake_case for the three xref dumps that share a schema. The
# xref column is renamed per table by `_xref_rename`.
_XREF_RENAME = {
    "gene_stable_id":       "ensembl_gene_id",
    "transcript_stable_id": "ensembl_transcript_id",
    "protein_stable_id":    "ensembl_protein_id",
    "db_name":              "db_name",
    "info_type":            "info_type",
    "source_identity":      "source_identity",
    "xref_identity":        "xref_identity",
    "linkage_type":         "linkage_type",
}

_ENA_RENAME = {
    "taxid":                "tax_id",
    "gene_stable_id":       "ensembl_gene_id",
    "transcript_stable_id": "ensembl_transcript_id",
    "protein_stable_id":    "ensembl_protein_id",
    "primary_accession":    "primary_accession",
    "secondary_accession":  "secondary_accession",
}


@register
class EnsemblTsv(DatasetPipeline):
    name = "ensembl_tsv"
    _TABLES = [
        ("entrez", ENTREZ_SCHEMA),
        ("refseq", REFSEQ_SCHEMA),
        ("uniprot", UNIPROT_SCHEMA),
        ("ena", ENA_SCHEMA),
    ]

    def _read_xref(self, db: str, xref_name: str) -> pd.DataFrame:
        """Concatenate the per-species '<db>' dumps into one xref table."""
        frames = []
        for species, tax_id in SPECIES_TAX_IDS.items():
            for src in sorted((self.raw_path() / species).glob(f"*.{db}.tsv.gz")):
                df = pd.read_csv(src, sep="\t", dtype=str, na_values=["-"])
                df.rename(columns={**_XREF_RENAME, "xref": xref_name}, inplace=True)
                df.insert(0, "tax_id", tax_id)
                frames.append(df)
        return pd.concat(frames, ignore_index=True)

    def extract(self) -> None:
        entrez = self._read_xref("entrez", "xref")
        refseq = self._read_xref("refseq", "refseq_accession")
        uniprot = self._read_xref("uniprot", "uniprot_accession")

        ena_frames = []
        for species in SPECIES_TAX_IDS:
            for src in sorted((self.raw_path() / species).glob("*.ena.tsv.gz")):
                df = pd.read_csv(src, sep="\t", dtype=str, na_values=["-"])
                df.rename(columns=_ENA_RENAME, inplace=True)
                ena_frames.append(df[list(_ENA_RENAME.values())])
        ena = pd.concat(ena_frames, ignore_index=True)

        entrez = self.apply_schema(entrez[list(ENTREZ_SCHEMA)], ENTREZ_SCHEMA)
        refseq = self.apply_schema(refseq[list(REFSEQ_SCHEMA)], REFSEQ_SCHEMA)
        uniprot = self.apply_schema(uniprot[list(UNIPROT_SCHEMA)], UNIPROT_SCHEMA)
        ena = self.apply_schema(ena[list(ENA_SCHEMA)], ENA_SCHEMA)

        self.save_parquet(entrez, self.intermediate_path() / "entrez.parquet")
        self.save_parquet(refseq, self.intermediate_path() / "refseq.parquet")
        self.save_parquet(uniprot, self.intermediate_path() / "uniprot.parquet")
        self.save_parquet(ena, self.intermediate_path() / "ena.parquet")

    def transform(self) -> None:
        # Each table is a faithful one-row-per-source-mapping projection; no
        # cross-table reconciliation is needed.
        pass

    def load(self) -> None:
        for stem, schema in self._TABLES:
            df = self.load_parquet(self.intermediate_path() / f"{stem}.parquet")
            self.save_parquet(df, self.output_path() / f"{stem}.parquet")
            self.save_schema_yaml(schema, stem)
