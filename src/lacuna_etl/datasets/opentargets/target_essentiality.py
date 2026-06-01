"""Open Targets target (gene) essentiality from DepMap.

  target_essentiality         - one row per gene per essentiality assessment (is_essential)
  target_essentiality_screens - one row per DepMap cell-line screen, flattened from
                                geneEssentiality -> depMapEssentiality -> screens
"""

import polars as pl

from lacuna_etl.core.identifiers import EnsemblGeneId
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.opentargets._base import OpenTargetsProductPipeline
from lacuna_etl.datasets.registry import register


def transform_file(df: pl.DataFrame) -> dict[str, pl.DataFrame]:
    ge = (
        df.select(pl.col("id").alias("target_id"), pl.col("geneEssentiality"))
        .explode("geneEssentiality")
        .filter(pl.col("geneEssentiality").is_not_null())
        .unnest("geneEssentiality")
    )
    essentiality = ge.select(
        pl.col("target_id"),
        pl.col("isEssential").alias("is_essential"),
    )

    screens = pl.DataFrame()
    if "depMapEssentiality" in ge.columns:
        dm = (
            ge.select(pl.col("target_id"), pl.col("depMapEssentiality"))
            .explode("depMapEssentiality")
            .filter(pl.col("depMapEssentiality").is_not_null())
            .unnest("depMapEssentiality")
        )
        if "screens" in dm.columns:
            screens = (
                dm.select(pl.col("target_id"), pl.col("tissueId"), pl.col("tissueName"), pl.col("screens"))
                .explode("screens")
                .filter(pl.col("screens").is_not_null())
                .unnest("screens")
                .select(
                    pl.col("target_id"),
                    pl.col("tissueId").alias("tissue_id"),
                    pl.col("tissueName").alias("tissue_name"),
                    pl.col("depmapId").alias("depmap_id"),
                    pl.col("cellLineName").alias("cell_line_name"),
                    pl.col("diseaseFromSource").alias("disease_from_source"),
                    pl.col("diseaseCellLineId").alias("disease_cell_line_id"),
                    pl.col("mutation").alias("mutation"),
                    pl.col("geneEffect").alias("gene_effect"),
                    pl.col("expression").alias("expression"),
                )
            )

    return {
        "target_essentiality": essentiality,
        "target_essentiality_screens": screens,
    }


TABLES_DOC = {
    "target_essentiality": {
        "target_id":    ColumnSpec(identifier=EnsemblGeneId, required=True, description="Ensembl gene ID"),
        "is_essential": ColumnSpec(description="Whether the gene is assessed as essential"),
    },
    "target_essentiality_screens": {
        "target_id":            ColumnSpec(identifier=EnsemblGeneId, required=True, description="Ensembl gene ID"),
        "tissue_id":            ColumnSpec(description="UBERON tissue ID of the screened tissue"),
        "tissue_name":          ColumnSpec(description="Tissue name"),
        "depmap_id":            ColumnSpec(description="DepMap cell-line ID"),
        "cell_line_name":       ColumnSpec(description="Cell-line name"),
        "disease_from_source":  ColumnSpec(description="Disease label reported by DepMap for the cell line"),
        "disease_cell_line_id": ColumnSpec(description="Disease/cell-line ID reported by the source"),
        "mutation":             ColumnSpec(description="Mutation annotation for the cell line"),
        "gene_effect":          ColumnSpec(description="DepMap gene-effect (Chronos) score; more negative = more essential"),
        "expression":           ColumnSpec(description="Gene expression in the cell line"),
    },
}


@register
class OpenTargetsTargetEssentiality(OpenTargetsProductPipeline):
    name = "opentargets_target_essentiality"
    raw_dirname = "target_essentiality"
    transform_module = "lacuna_etl.datasets.opentargets.target_essentiality"
    first_table = "target_essentiality"
    tables_doc = TABLES_DOC
