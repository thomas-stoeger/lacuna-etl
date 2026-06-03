"""Open Targets openFDA significant adverse drug reactions (FAERS disproportionality).

  drug_adverse_reactions - one row per (molecule, adverse event) with its likelihood ratio
"""

import polars as pl

from lacuna_etl.core.identifiers import ChemblId
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.opentargets._base import OpenTargetsProductPipeline
from lacuna_etl.datasets.registry import register


def transform_file(df: pl.DataFrame) -> dict[str, pl.DataFrame]:
    reactions = df.select(
        pl.col("chembl_id").alias("chembl_id"),
        pl.col("event").alias("event"),
        pl.col("count").cast(pl.Int64).alias("count"),
        pl.col("llr").alias("llr"),
        pl.col("critval").alias("critical_value"),
        pl.col("meddraCode").alias("meddra_code"),
    )
    return {"drug_adverse_reactions": reactions}


TABLES_DOC = {
    "drug_adverse_reactions": {
        "chembl_id":      ColumnSpec(identifier=ChemblId, required=True, description="ChEMBL molecule ID"),
        "event":          ColumnSpec(description="MedDRA adverse-event term"),
        "count":          ColumnSpec(description="Number of co-occurrence reports"),
        "llr":            ColumnSpec(description="Likelihood ratio of the disproportionality signal"),
        "critical_value": ColumnSpec(description="Critical value the LLR is tested against"),
        "meddra_code":    ColumnSpec(description="MedDRA code of the adverse event"),
    },
}


@register
class OpenTargetsOpenfdaAdverseReactions(OpenTargetsProductPipeline):
    name = "opentargets_openfda_significant_adverse_drug_reactions"
    raw_dirname = "openfda_significant_adverse_drug_reactions"
    transform_module = "lacuna_etl.datasets.opentargets.openfda_significant_adverse_drug_reactions"
    first_table = "drug_adverse_reactions"
    tables_doc = TABLES_DOC
