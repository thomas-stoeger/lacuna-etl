"""Open Targets drug mechanisms of action (ChEMBL).

Source records have no stable key, so the mechanism is exploded by the ChEMBL
molecules it applies to.

  drug_mechanisms            - one row per (molecule, mechanism of action)
  drug_mechanism_references  - one row per (molecule, mechanism, source) reference
"""

import polars as pl

from lacuna_etl.core.identifiers import ChemblId
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.opentargets._base import OpenTargetsProductPipeline
from lacuna_etl.datasets.registry import register


def transform_file(df: pl.DataFrame) -> dict[str, pl.DataFrame]:
    mechanisms = (
        df.select(
            pl.col("chemblIds"),
            pl.col("actionType").alias("action_type"),
            pl.col("mechanismOfAction").alias("mechanism_of_action"),
            pl.col("targetName").alias("target_name"),
            pl.col("targetType").alias("target_type"),
            pl.col("targets").alias("target_ids"),
        )
        .explode("chemblIds")
        .filter(pl.col("chemblIds").is_not_null())
        .rename({"chemblIds": "chembl_id"})
        .select("chembl_id", "action_type", "mechanism_of_action", "target_name", "target_type", "target_ids")
    )

    references = pl.DataFrame()
    if "references" in df.columns:
        references = (
            df.select(pl.col("chemblIds"), pl.col("mechanismOfAction").alias("mechanism_of_action"),
                      pl.col("references"))
            .explode("chemblIds")
            .filter(pl.col("chemblIds").is_not_null())
            .explode("references")
            .filter(pl.col("references").is_not_null())
            .unnest("references")
            .select(
                pl.col("chemblIds").alias("chembl_id"),
                pl.col("mechanism_of_action"),
                pl.col("source"),
                pl.col("ids").alias("reference_ids"),
                pl.col("urls").alias("reference_urls"),
            )
        )

    return {"drug_mechanisms": mechanisms, "drug_mechanism_references": references}


TABLES_DOC = {
    "drug_mechanisms": {
        "chembl_id":           ColumnSpec(identifier=ChemblId, required=True, description="ChEMBL molecule ID the mechanism applies to"),
        "action_type":         ColumnSpec(description="Action type (INHIBITOR, AGONIST, ...)"),
        "mechanism_of_action": ColumnSpec(description="Mechanism-of-action description"),
        "target_name":         ColumnSpec(description="Target name as reported by ChEMBL"),
        "target_type":         ColumnSpec(description="Target type (SINGLE PROTEIN, PROTEIN COMPLEX, ...)"),
        "target_ids":          ColumnSpec(description="Ensembl gene IDs of the mechanism's targets (list)"),
    },
    "drug_mechanism_references": {
        "chembl_id":           ColumnSpec(identifier=ChemblId, required=True, description="ChEMBL molecule ID"),
        "mechanism_of_action": ColumnSpec(description="Mechanism-of-action description"),
        "source":              ColumnSpec(description="Reference source"),
        "reference_ids":       ColumnSpec(description="Identifiers within the source (list)"),
        "reference_urls":      ColumnSpec(description="Reference URLs (list)"),
    },
}


@register
class OpenTargetsDrugMechanismOfAction(OpenTargetsProductPipeline):
    name = "opentargets_drug_mechanism_of_action"
    raw_dirname = "drug_mechanism_of_action"
    transform_module = "lacuna_etl.datasets.opentargets.drug_mechanism_of_action"
    first_table = "drug_mechanisms"
    tables_doc = TABLES_DOC
