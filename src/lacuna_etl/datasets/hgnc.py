"""HGNC — the HUGO Gene Nomenclature Committee's complete human gene set.

HGNC assigns the approved symbol and name for every human gene and curates a dense
set of cross-references to external databases. The whole knowledge base ships as one
wide tab-delimited file (``hgnc_complete_set_<date>.txt``): ~45k rows, one per
approved gene, across ~54 columns. It fits comfortably in memory, so this is an
in-memory pandas pipeline: ``extract`` reads the TSV and reshapes it into per-table
row lists, ``transform`` is a no-op (faithful projection), and ``load`` types,
validates against each ``SCHEMA``, and writes final Parquet + sidecar.

The grain key is the HGNC id (``HgncId``, ``HGNC:5``), unique and never null. The
parent ``genes`` table keeps the one-per-gene scalar columns — the approved symbol
and name, the locus classification, cytogenetic location, the curation dates, and
the single-valued external cross-references (Entrez, Ensembl, Vega, UCSC, OMIM-free
resources, the AGR/Alliance curie, the MANE Select transcript pair, …). The
``|``-delimited multi-valued fields are exploded into one-value-per-row child tables
keyed by ``hgnc_id``:

- nomenclature history — ``gene_alias_symbols``, ``gene_alias_names``,
  ``gene_prev_symbols``, ``gene_prev_names``;
- gene-family membership — ``gene_groups`` (paired group id + name);
- joinable cross-references — ``gene_uniprot`` (``UniprotAccession``),
  ``gene_refseq`` (``RefSeqAccession``), ``gene_pubmed`` (``PubmedId``),
  ``gene_ena``, ``gene_ccds``, ``gene_mgd`` (mouse MGI curie), ``gene_rgd`` (rat RGD
  curie), ``gene_omim``, ``gene_enzyme`` (EC number);
- ``gene_lsdb`` — the locus-specific-database ``name|url`` pairs.

Three source columns that are entirely empty in the snapshot (``location_sortable``,
``kznf_gene_catalog``, ``intermediate_filament_db``) are not ingested. The many
heterogeneous external-resource ids (Vega, UCSC, OMIM, Orphanet, COSMIC, MGI/RGD
curies, EC numbers, …) have no single canonical form across resources and stay
documented plain strings (the Open Targets / PubTator precedent); only the HGNC key
and the cross-references that already have a canonical type in this repo (Entrez,
Ensembl gene, UniProt, RefSeq, PubMed, the Alliance curie) are typed. See
docs/DESIGN.md for the full table inventory.
"""
from __future__ import annotations

import re

import pandas as pd

from lacuna_etl.core.identifiers import (
    AllianceGeneId,
    EnsemblGeneId,
    HgncId,
    NcbiGeneId,
    PubmedId,
    RefSeqAccession,
    UniprotAccession,
)
from lacuna_etl.core.pipeline import DatasetPipeline
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.registry import register

# --------------------------------------------------------------------------
# Parent: one row per approved gene, keyed by hgnc_id.
#
# Scalar source columns -> snake_case parent columns (verbatim values). Listed in
# source order so the projection reads against the header. The mane_select pair and
# the dropped all-empty columns are handled separately.
# --------------------------------------------------------------------------
_RENAME = {
    "hgnc_id": "hgnc_id",
    "symbol": "symbol",
    "name": "name",
    "locus_group": "locus_group",
    "locus_type": "locus_type",
    "status": "status",
    "location": "location",
    "date_approved_reserved": "date_approved_reserved",
    "date_symbol_changed": "date_symbol_changed",
    "date_name_changed": "date_name_changed",
    "date_modified": "date_modified",
    "entrez_id": "entrez_id",
    "ensembl_gene_id": "ensembl_gene_id",
    "vega_id": "vega_id",
    "ucsc_id": "ucsc_id",
    "cosmic": "cosmic",
    "mirbase": "mirbase",
    "homeodb": "homeodb",
    "snornabase": "snornabase",
    "bioparadigms_slc": "bioparadigms_slc",
    "orphanet": "orphanet",
    "pseudogene.org": "pseudogene_org",
    "horde_id": "horde_id",
    "merops": "merops",
    "imgt": "imgt",
    "iuphar": "iuphar",
    "mamit-trnadb": "mamit_trnadb",
    "cd": "cd",
    "lncrnadb": "lncrnadb",
    "rna_central_id": "rna_central_id",
    "lncipedia": "lncipedia",
    "gtrnadb": "gtrnadb",
    "agr": "agr",
    "gencc": "gencc",
}

GENES_SCHEMA = {
    "hgnc_id": ColumnSpec(identifier=HgncId, required=True, description="HGNC gene id (HGNC:N); the grain key"),
    "symbol": ColumnSpec(required=True, description="HGNC-approved gene symbol (unique)"),
    "name": ColumnSpec(required=True, description="HGNC-approved gene name"),
    "locus_group": ColumnSpec(description="Broad locus classification (protein-coding gene, non-coding RNA, pseudogene, other)"),
    "locus_type": ColumnSpec(description="Specific locus type (e.g. 'gene with protein product', 'RNA, long non-coding')"),
    "status": ColumnSpec(description="HGNC entry status (the complete set carries only 'Approved')"),
    "location": ColumnSpec(description="Cytogenetic location (e.g. '19q13.43')"),
    "date_approved_reserved": ColumnSpec(description="Date the symbol was approved or reserved (ISO date string)"),
    "date_symbol_changed": ColumnSpec(description="Date the symbol last changed (ISO date string)"),
    "date_name_changed": ColumnSpec(description="Date the name last changed (ISO date string)"),
    "date_modified": ColumnSpec(description="Date the record was last modified (ISO date string)"),
    "entrez_id": ColumnSpec(identifier=NcbiGeneId, description="NCBI Entrez Gene id (nullable)"),
    "ensembl_gene_id": ColumnSpec(identifier=EnsemblGeneId, description="Ensembl gene id (human, ENSG…; nullable)"),
    "vega_id": ColumnSpec(description="VEGA gene id"),
    "ucsc_id": ColumnSpec(description="UCSC Genome Browser gene id"),
    "cosmic": ColumnSpec(description="COSMIC gene symbol"),
    "mirbase": ColumnSpec(description="miRBase accession"),
    "homeodb": ColumnSpec(description="HomeoDB id"),
    "snornabase": ColumnSpec(description="snoRNABase id"),
    "bioparadigms_slc": ColumnSpec(description="BioParadigms SLC-tables symbol"),
    "orphanet": ColumnSpec(description="Orphanet gene id"),
    "pseudogene_org": ColumnSpec(description="pseudogene.org id"),
    "horde_id": ColumnSpec(description="HORDE (olfactory receptor) id"),
    "merops": ColumnSpec(description="MEROPS peptidase id"),
    "imgt": ColumnSpec(description="IMGT/GENE-DB symbol"),
    "iuphar": ColumnSpec(description="IUPHAR/Guide to Pharmacology object id (an 'HGNC:' or 'objectId:' reference)"),
    "mamit_trnadb": ColumnSpec(description="Mamit-tRNAdb id"),
    "cd": ColumnSpec(description="Human Cell Differentiation Molecule (CD) symbol"),
    "lncrnadb": ColumnSpec(description="lncRNAdb id"),
    "rna_central_id": ColumnSpec(description="RNAcentral id"),
    "lncipedia": ColumnSpec(description="LNCipedia id"),
    "gtrnadb": ColumnSpec(description="GtRNAdb id"),
    "agr": ColumnSpec(identifier=AllianceGeneId, description="Alliance of Genome Resources gene curie (the HGNC id, for human genes; nullable)"),
    "gencc": ColumnSpec(description="GenCC gene id (an 'HGNC:' reference)"),
    "mane_select_ensembl_transcript_id": ColumnSpec(description="MANE Select Ensembl transcript id (versioned ENST…), from the 'mane_select' pair"),
    "mane_select_refseq_accession": ColumnSpec(identifier=RefSeqAccession, description="MANE Select RefSeq transcript accession, from the 'mane_select' pair"),
}

# --------------------------------------------------------------------------
# Child tables keyed by hgnc_id (the source's |-delimited multi-valued fields).
# --------------------------------------------------------------------------
GENE_ALIAS_SYMBOLS_SCHEMA = {
    "hgnc_id": ColumnSpec(identifier=HgncId, required=True, description="HGNC gene id"),
    "alias_symbol": ColumnSpec(required=True, description="An alternative (alias) symbol for the gene"),
}

GENE_ALIAS_NAMES_SCHEMA = {
    "hgnc_id": ColumnSpec(identifier=HgncId, required=True, description="HGNC gene id"),
    "alias_name": ColumnSpec(required=True, description="An alternative (alias) name for the gene"),
}

GENE_PREV_SYMBOLS_SCHEMA = {
    "hgnc_id": ColumnSpec(identifier=HgncId, required=True, description="HGNC gene id"),
    "prev_symbol": ColumnSpec(required=True, description="A previously approved symbol for the gene"),
}

GENE_PREV_NAMES_SCHEMA = {
    "hgnc_id": ColumnSpec(identifier=HgncId, required=True, description="HGNC gene id"),
    "prev_name": ColumnSpec(required=True, description="A previously approved name for the gene"),
}

GENE_GROUPS_SCHEMA = {
    "hgnc_id": ColumnSpec(identifier=HgncId, required=True, description="HGNC gene id"),
    "gene_group_id": ColumnSpec(required=True, description="HGNC gene group (family) numeric id (Int64)"),
    "gene_group_name": ColumnSpec(required=True, description="HGNC gene group (family) name"),
}

GENE_ENA_SCHEMA = {
    "hgnc_id": ColumnSpec(identifier=HgncId, required=True, description="HGNC gene id"),
    "ena_accession": ColumnSpec(required=True, description="An ENA / INSDC nucleotide accession"),
}

GENE_REFSEQ_SCHEMA = {
    "hgnc_id": ColumnSpec(identifier=HgncId, required=True, description="HGNC gene id"),
    "refseq_accession": ColumnSpec(identifier=RefSeqAccession, required=True, description="A RefSeq nucleotide accession"),
}

GENE_CCDS_SCHEMA = {
    "hgnc_id": ColumnSpec(identifier=HgncId, required=True, description="HGNC gene id"),
    "ccds_id": ColumnSpec(required=True, description="A Consensus CDS (CCDS) id"),
}

GENE_UNIPROT_SCHEMA = {
    "hgnc_id": ColumnSpec(identifier=HgncId, required=True, description="HGNC gene id"),
    "uniprot_accession": ColumnSpec(identifier=UniprotAccession, required=True, description="A mapped UniProtKB accession"),
}

GENE_PUBMED_SCHEMA = {
    "hgnc_id": ColumnSpec(identifier=HgncId, required=True, description="HGNC gene id"),
    "pubmed_id": ColumnSpec(identifier=PubmedId, required=True, description="A PubMed id linked to the gene"),
}

GENE_MGD_SCHEMA = {
    "hgnc_id": ColumnSpec(identifier=HgncId, required=True, description="HGNC gene id"),
    "mgd_id": ColumnSpec(required=True, description="A mouse ortholog MGI curie (MGI:…)"),
}

GENE_RGD_SCHEMA = {
    "hgnc_id": ColumnSpec(identifier=HgncId, required=True, description="HGNC gene id"),
    "rgd_id": ColumnSpec(required=True, description="A rat ortholog RGD curie (RGD:…)"),
}

GENE_OMIM_SCHEMA = {
    "hgnc_id": ColumnSpec(identifier=HgncId, required=True, description="HGNC gene id"),
    "omim_id": ColumnSpec(required=True, description="A linked OMIM id"),
}

GENE_ENZYME_SCHEMA = {
    "hgnc_id": ColumnSpec(identifier=HgncId, required=True, description="HGNC gene id"),
    "enzyme_id": ColumnSpec(required=True, description="An Enzyme Commission (EC) number"),
}

GENE_LSDB_SCHEMA = {
    "hgnc_id": ColumnSpec(identifier=HgncId, required=True, description="HGNC gene id"),
    "lsdb_name": ColumnSpec(required=True, description="A locus-specific mutation database name"),
    "lsdb_url": ColumnSpec(description="The locus-specific database URL"),
}

# Source columns that are entirely empty in the snapshot and are not ingested.
_DROPPED = {"location_sortable", "kznf_gene_catalog", "intermediate_filament_db"}

# Simple |-delimited list field -> (output stem, value column). One row per value.
_LIST_FIELDS: list[tuple[str, str, str]] = [
    ("alias_symbol", "gene_alias_symbols", "alias_symbol"),
    ("alias_name", "gene_alias_names", "alias_name"),
    ("prev_symbol", "gene_prev_symbols", "prev_symbol"),
    ("prev_name", "gene_prev_names", "prev_name"),
    ("ena", "gene_ena", "ena_accession"),
    ("ccds_id", "gene_ccds", "ccds_id"),
    ("mgd_id", "gene_mgd", "mgd_id"),
    ("rgd_id", "gene_rgd", "rgd_id"),
    ("omim_id", "gene_omim", "omim_id"),
    ("enzyme_id", "gene_enzyme", "enzyme_id"),
]


def _clean(v: object) -> str | None:
    """Empty / whitespace-only string (HGNC's null marker) -> None."""
    if v is None or not isinstance(v, str):
        return None
    v = v.strip()
    return v or None


def _split(v: object) -> list[str]:
    """Split a |-delimited list field, dropping empty fragments."""
    v = _clean(v)
    if v is None:
        return []
    return [p.strip() for p in v.split("|") if p.strip()]


@register
class Hgnc(DatasetPipeline):
    name = "hgnc"

    _TABLES = [
        ("genes", GENES_SCHEMA),
        ("gene_alias_symbols", GENE_ALIAS_SYMBOLS_SCHEMA),
        ("gene_alias_names", GENE_ALIAS_NAMES_SCHEMA),
        ("gene_prev_symbols", GENE_PREV_SYMBOLS_SCHEMA),
        ("gene_prev_names", GENE_PREV_NAMES_SCHEMA),
        ("gene_groups", GENE_GROUPS_SCHEMA),
        ("gene_ena", GENE_ENA_SCHEMA),
        ("gene_refseq", GENE_REFSEQ_SCHEMA),
        ("gene_ccds", GENE_CCDS_SCHEMA),
        ("gene_uniprot", GENE_UNIPROT_SCHEMA),
        ("gene_pubmed", GENE_PUBMED_SCHEMA),
        ("gene_mgd", GENE_MGD_SCHEMA),
        ("gene_rgd", GENE_RGD_SCHEMA),
        ("gene_omim", GENE_OMIM_SCHEMA),
        ("gene_enzyme", GENE_ENZYME_SCHEMA),
        ("gene_lsdb", GENE_LSDB_SCHEMA),
    ]

    def _read_tsv(self) -> pd.DataFrame:
        matches = sorted(self.raw_path().glob("hgnc_complete_set_*.txt"))
        if len(matches) != 1:
            raise FileNotFoundError(
                f"expected exactly one hgnc_complete_set_*.txt under {self.raw_path()}, found {len(matches)}"
            )
        # keep_default_na=False / na_filter=False: empty string is HGNC's only null
        # marker; we normalise it per column via _clean.
        return pd.read_csv(matches[0], sep="\t", dtype=str, keep_default_na=False, na_filter=False)

    def extract(self) -> None:
        df = self._read_tsv()
        missing = set(_RENAME) - set(df.columns)
        if missing:
            raise ValueError(f"hgnc: source is missing expected columns {sorted(missing)}")

        out: dict[str, list[dict]] = {stem: [] for stem, _ in self._TABLES}

        for rec in df.to_dict("records"):
            hid = _clean(rec["hgnc_id"])

            # MANE Select is a single 'ENST…|NM_…' transcript pair (never a list).
            mane = _split(rec.get("mane_select"))
            mane_ensembl = mane[0] if len(mane) >= 1 else None
            mane_refseq = mane[1] if len(mane) >= 2 else None

            out["genes"].append({
                **{col: _clean(rec.get(src)) for src, col in _RENAME.items()},
                "entrez_id": NcbiGeneId.parse(rec.get("entrez_id")),
                "mane_select_ensembl_transcript_id": mane_ensembl,
                "mane_select_refseq_accession": _strip_trailing_dot(mane_refseq),
            })

            for src, stem, col in _LIST_FIELDS:
                for val in _split(rec.get(src)):
                    out[stem].append({"hgnc_id": hid, col: val})

            # gene_group / gene_group_id are parallel |-lists of equal length.
            groups = _split(rec.get("gene_group"))
            group_ids = _split(rec.get("gene_group_id"))
            if len(groups) != len(group_ids):
                raise ValueError(f"{hid}: gene_group / gene_group_id length mismatch ({len(groups)} vs {len(group_ids)})")
            for gid, gname in zip(group_ids, groups):
                out["gene_groups"].append({"hgnc_id": hid, "gene_group_id": int(gid), "gene_group_name": gname})

            for acc in _split(rec.get("refseq_accession")):
                out["gene_refseq"].append({"hgnc_id": hid, "refseq_accession": _strip_trailing_dot(acc)})
            for acc in _split(rec.get("uniprot_ids")):
                out["gene_uniprot"].append({"hgnc_id": hid, "uniprot_accession": acc})
            for tok in _split(rec.get("pubmed_id")):
                out["gene_pubmed"].append({"hgnc_id": hid, "pubmed_id": PubmedId.parse(tok)})

            # lsdb is a flat list of 'name|url' pairs (name, url, name, url, …).
            lsdb = _split(rec.get("lsdb"))
            for i in range(0, len(lsdb), 2):
                name = lsdb[i]
                url = lsdb[i + 1] if i + 1 < len(lsdb) else None
                out["gene_lsdb"].append({"hgnc_id": hid, "lsdb_name": name, "lsdb_url": url})

        for stem, schema in self._TABLES:
            self.save_parquet(
                pd.DataFrame(out[stem], columns=list(schema)), self.intermediate_path() / f"{stem}.parquet"
            )
        print(
            f"[{self.name}] genes: {len(out['genes']):,}, uniprot: {len(out['gene_uniprot']):,}, "
            f"refseq: {len(out['gene_refseq']):,}, pubmed: {len(out['gene_pubmed']):,}, "
            f"groups: {len(out['gene_groups']):,}, lsdb: {len(out['gene_lsdb']):,}"
        )

    def transform(self) -> None:
        # Faithful projection; parsing happens in extract, typing/validation in load.
        pass

    def load(self) -> None:
        for stem, schema in self._TABLES:
            df = self.load_parquet(self.intermediate_path() / f"{stem}.parquet")
            df = df[list(schema)]
            if stem == "gene_groups":
                df["gene_group_id"] = df["gene_group_id"].astype("Int64")
            if stem == "genes":
                # entrez_id is a legitimately-sparse Entrez Gene id; NumericIdentifier
                # casts it but its validate rejects any null, so check positivity on
                # the non-null values only (the alliancegenome nullable-PMID precedent).
                df["entrez_id"] = NcbiGeneId.cast(df["entrez_id"])
                if (df["entrez_id"].dropna() <= 0).any():
                    raise ValueError("hgnc: non-positive entrez_id")
                schema = {k: v for k, v in schema.items() if k != "entrez_id"}
            df = self.apply_schema(df, schema)
            self.save_parquet(df, self.output_path() / f"{stem}.parquet")
            self.save_schema_yaml(stem=stem, schema=GENES_SCHEMA if stem == "genes" else schema)


def _strip_trailing_dot(v: str | None) -> str | None:
    """RefSeq accessions occasionally carry a dangling '.' with no version
    (``NM_021728.``); drop it so the value matches the canonical RefSeq form."""
    if v is None:
        return None
    return re.sub(r"\.$", "", v)
