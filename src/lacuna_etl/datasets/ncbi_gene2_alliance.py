"""
Crosswalk from NCBI Entrez Gene IDs to Alliance of Genome Resources gene curies.

Per docs/DESIGN.md ("Filling in missing values"), a cross-dataset linkage lives
in its own table so each upstream stays unchanged and consumers join when they
want the enriched view. Here the link is authoritative, not heuristic: NCBI's
``gene_info`` cross-references the Alliance gene directly in its ``db_xrefs``
column as ``AllianceGenome:<curie>`` (e.g. ``AllianceGenome:WB:WBGene00022277``).
Stripping the ``AllianceGenome:`` prefix yields the Alliance gene curie
(``AllianceGeneId``).

The crosswalk depends on both `ncbi_gene_info` (the source of the link, with
Entrez IDs already made current via gene_history) and `alliancegenome` (to confirm
each cross-referenced curie is a gene actually present in the Alliance release):
links whose curie is absent from the Alliance snapshot are dropped and counted, so
the table only ever points at genes both sides currently know. Both upstreams are
read from `get_output_root()`, so this pipeline has no `raw_path()`; run them first.
"""

from __future__ import annotations

import polars as pl

from lacuna_etl.config import get_output_root
from lacuna_etl.core.identifiers import AllianceGeneId, NcbiGeneId, NcbiTaxId
from lacuna_etl.core.pipeline import DatasetPipeline
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.registry import register

SCHEMA = {
    "entrez_id": ColumnSpec(identifier=NcbiGeneId, required=True, description="NCBI Entrez Gene ID (current; remapped through gene_history upstream)"),
    "alliance_gene_id": ColumnSpec(identifier=AllianceGeneId, required=True, description="Alliance gene curie cross-referenced by NCBI (gene_info 'AllianceGenome:<curie>' dbXref), confirmed present in the Alliance release"),
    "tax_id": ColumnSpec(identifier=NcbiTaxId, required=True, description="NCBI taxonomy ID of the gene"),
}

# Alliance output columns that hold canonical Alliance gene curies; their union is
# the set of genes actually present in the Alliance release. Plain-string columns
# (disease object, gene cross-references) are included but pattern-filtered, since
# they also carry non-gene identifiers (alleles, models, RefSeq, ...).
_ALLIANCE_GENE_COLUMNS = [
    ("orthology", "gene1_id"),
    ("orthology", "gene2_id"),
    ("expression", "gene_id"),
    ("gene_descriptions", "gene_id"),
    ("variant_alleles", "allele_associated_gene_id"),
    ("variant_alleles", "variant_affected_gene_id"),
    ("disease_associations", "db_object_id"),
    ("gene_cross_references", "gene_id"),
]

_ALLIANCE_CURIE_RE = rf"^(?:{AllianceGeneId.pattern})$"


@register
class NcbiGene2Alliance(DatasetPipeline):
    name = "ncbi_gene2_alliance"
    depends_on = ["ncbi_gene_info", "alliancegenome"]

    def _gene_info_path(self):
        return get_output_root() / "ncbi_gene_info" / "gene_info.parquet"

    def _alliance_dir(self):
        return get_output_root() / "alliancegenome"

    def extract(self) -> None:
        """Pull the AllianceGenome dbXref links out of gene_info.

        gene_info is ~68M rows; the scan is lazy and only the ~0.3M rows that
        carry an ``AllianceGenome:`` xref are ever materialised.
        """
        links = (
            pl.scan_parquet(self._gene_info_path())
            .select(["entrez_id", "tax_id", "db_xrefs"])
            .filter(pl.col("db_xrefs").str.contains("AllianceGenome:", literal=True))
            .with_columns(pl.col("db_xrefs").str.split("|"))
            .explode("db_xrefs")
            .filter(pl.col("db_xrefs").str.starts_with("AllianceGenome:"))
            .with_columns(
                pl.col("db_xrefs").str.replace("^AllianceGenome:", "").alias("alliance_gene_id")
            )
            .select(["entrez_id", "alliance_gene_id", "tax_id"])
            .unique()
            .collect()
        )
        print(f"[{self.name}] AllianceGenome dbXref links in gene_info: {links.height:,}")
        self.save_parquet(links.to_pandas(), self.intermediate_path() / "raw_links.parquet")

    def transform(self) -> None:
        """Confirm each curie against the Alliance gene universe; drop+count drift."""
        links = pl.read_parquet(self.intermediate_path() / "raw_links.parquet")

        universe: set[str] = set()
        for table, col in _ALLIANCE_GENE_COLUMNS:
            s = (
                pl.scan_parquet(self._alliance_dir() / f"{table}.parquet")
                .select(col)
                .drop_nulls()
                .filter(pl.col(col).str.contains(_ALLIANCE_CURIE_RE))
                .unique()
                .collect()[col]
            )
            universe.update(s.to_list())
        print(f"[{self.name}] Alliance gene universe: {len(universe):,} curies")

        before = links.height
        kept = links.filter(pl.col("alliance_gene_id").is_in(list(universe)))
        dropped = before - kept.height
        print(
            f"[{self.name}] kept {kept.height:,} links; dropped {dropped:,} "
            f"({dropped / before:.2%}) whose curie is absent from the Alliance release"
        )

        df = kept.to_pandas()[list(SCHEMA)]
        # Grain is (entrez_id, alliance_gene_id); a gene has a single tax_id, so the
        # unique() above already enforces it, but guard against any surprise.
        dups = df.duplicated(subset=["entrez_id", "alliance_gene_id"])
        if dups.any():
            raise ValueError(
                f"[{self.name}] duplicate (entrez_id, alliance_gene_id) pairs: "
                f"{df.loc[dups, ['entrez_id', 'alliance_gene_id']].head(10).to_dict('records')}"
            )
        df = self.apply_schema(df, SCHEMA)
        self.save_parquet(df, self.intermediate_path() / "ncbi_gene2_alliance.parquet")

    def load(self) -> None:
        df = self.load_parquet(self.intermediate_path() / "ncbi_gene2_alliance.parquet")
        self.save_parquet(df, self.output_path() / "ncbi_gene2_alliance.parquet")
        self.save_schema_yaml(SCHEMA, "ncbi_gene2_alliance")
