"""Open Targets baseline expression (per gene, per tissue; RNA + protein).

  expression_tissues          - one row per (gene, tissue) with RNA + protein summary levels
  expression_protein_cell_types - per-(gene, tissue) protein levels broken down by cell type
"""

import polars as pl

from lacuna_etl.core.identifiers import EnsemblGeneId
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.opentargets._base import OpenTargetsProductPipeline
from lacuna_etl.datasets.registry import register


def transform_file(df: pl.DataFrame) -> dict[str, pl.DataFrame]:
    t = (
        df.select(pl.col("id").alias("target_id"), pl.col("tissues"))
        .explode("tissues")
        .filter(pl.col("tissues").is_not_null())
        .unnest("tissues")
    )
    tissues = t.select(
        pl.col("target_id"),
        pl.col("efo_code"),
        pl.col("label"),
        pl.col("organs"),
        pl.col("anatomical_systems"),
        pl.col("rna").struct.field("value").alias("rna_value"),
        pl.col("rna").struct.field("zscore").cast(pl.Int64).alias("rna_zscore"),
        pl.col("rna").struct.field("level").cast(pl.Int64).alias("rna_level"),
        pl.col("rna").struct.field("unit").alias("rna_unit"),
        pl.col("protein").struct.field("reliability").alias("protein_reliability"),
        pl.col("protein").struct.field("level").cast(pl.Int64).alias("protein_level"),
    )

    cell_types = (
        t.select(
            pl.col("target_id"),
            pl.col("efo_code"),
            pl.col("protein").struct.field("cell_type").alias("cell_type"),
        )
        .explode("cell_type")
        .filter(pl.col("cell_type").is_not_null())
        .unnest("cell_type")
        .select(
            pl.col("target_id"),
            pl.col("efo_code"),
            pl.col("name").alias("cell_type_name"),
            pl.col("reliability").alias("reliability"),
            pl.col("level").cast(pl.Int64).alias("level"),
        )
    )

    return {"expression_tissues": tissues, "expression_protein_cell_types": cell_types}


TABLES_DOC = {
    "expression_tissues": {
        "target_id":           ColumnSpec(identifier=EnsemblGeneId, required=True, description="Ensembl gene ID"),
        "efo_code":            ColumnSpec(description="EFO tissue/cell-type code"),
        "label":               ColumnSpec(description="Tissue label"),
        "organs":              ColumnSpec(description="Organs the tissue belongs to (list)"),
        "anatomical_systems":  ColumnSpec(description="Anatomical systems the tissue belongs to (list)"),
        "rna_value":           ColumnSpec(description="RNA expression value"),
        "rna_zscore":          ColumnSpec(description="RNA expression z-score across tissues"),
        "rna_level":           ColumnSpec(description="Binned RNA expression level"),
        "rna_unit":            ColumnSpec(description="RNA expression unit"),
        "protein_reliability": ColumnSpec(description="Whether the protein-level call is reliable"),
        "protein_level":       ColumnSpec(description="Binned protein expression level"),
    },
    "expression_protein_cell_types": {
        "target_id":      ColumnSpec(identifier=EnsemblGeneId, required=True, description="Ensembl gene ID"),
        "efo_code":       ColumnSpec(description="EFO tissue/cell-type code of the parent tissue"),
        "cell_type_name": ColumnSpec(description="Cell-type name"),
        "reliability":    ColumnSpec(description="Whether the protein call in this cell type is reliable"),
        "level":          ColumnSpec(description="Binned protein expression level in this cell type"),
    },
}


@register
class OpenTargetsExpression(OpenTargetsProductPipeline):
    name = "opentargets_expression"
    raw_dirname = "expression"
    transform_module = "lacuna_etl.datasets.opentargets.expression"
    first_table = "expression_tissues"
    tables_doc = TABLES_DOC
