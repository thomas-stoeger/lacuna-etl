"""Open Targets drug warnings (ChEMBL withdrawn / black-box warnings).

  drug_warnings            - one row per warning record (keyed by warning_id)
  drug_warnings_chembl_ids - ChEMBL molecules the warning applies to
  drug_warnings_references - supporting references
"""

import polars as pl

from lacuna_etl.core.identifiers import ChemblId
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.opentargets._base import OpenTargetsProductPipeline
from lacuna_etl.datasets.opentargets._helpers import explode_scalar_list, explode_struct_list
from lacuna_etl.datasets.registry import register

_KEY = {"id": "warning_id"}


def transform_file(df: pl.DataFrame) -> dict[str, pl.DataFrame]:
    warnings = df.select(
        pl.col("id").cast(pl.Int64).alias("warning_id"),
        pl.col("warningType").alias("warning_type"),
        pl.col("toxicityClass").alias("toxicity_class"),
        pl.col("country").alias("country"),
        pl.col("description").alias("description"),
        pl.col("year").cast(pl.Int64).alias("year"),
        pl.col("efoTerm").alias("efo_term"),
        pl.col("efoId").alias("efo_id"),
        pl.col("efoIdForWarningClass").alias("efo_id_for_warning_class"),
    )
    chembl_ids = explode_scalar_list(df, _KEY, "chemblIds", "chembl_id")
    references = explode_struct_list(
        df, _KEY, "references",
        {"id": "reference_id", "source": "source", "url": "url"},
    )
    return {
        "drug_warnings": warnings,
        "drug_warnings_chembl_ids": chembl_ids,
        "drug_warnings_references": references,
    }


TABLES_DOC = {
    "drug_warnings": {
        "warning_id":               ColumnSpec(required=True, description="Source warning record ID"),
        "warning_type":             ColumnSpec(description="Warning type (Withdrawn, Black Box Warning, ...)"),
        "toxicity_class":           ColumnSpec(description="Toxicity class"),
        "country":                  ColumnSpec(description="Country the warning was issued in"),
        "description":              ColumnSpec(description="Warning description"),
        "year":                     ColumnSpec(description="Year the warning was issued"),
        "efo_term":                 ColumnSpec(description="EFO term label for the adverse event"),
        "efo_id":                   ColumnSpec(description="EFO ID for the adverse event"),
        "efo_id_for_warning_class": ColumnSpec(description="EFO ID for the warning class"),
    },
    "drug_warnings_chembl_ids": {
        "warning_id": ColumnSpec(required=True, description="Source warning record ID"),
        "chembl_id":  ColumnSpec(identifier=ChemblId, description="ChEMBL molecule the warning applies to"),
    },
    "drug_warnings_references": {
        "warning_id":   ColumnSpec(required=True, description="Source warning record ID"),
        "reference_id": ColumnSpec(description="Identifier within the reference source"),
        "source":       ColumnSpec(description="Reference source"),
        "url":          ColumnSpec(description="Reference URL"),
    },
}


@register
class OpenTargetsDrugWarning(OpenTargetsProductPipeline):
    name = "opentargets_drug_warning"
    raw_dirname = "drug_warning"
    transform_module = "lacuna_etl.datasets.opentargets.drug_warning"
    first_table = "drug_warnings"
    tables_doc = TABLES_DOC
