"""Open Targets clinical indications (drug -> disease at a clinical stage).

  clinical_indications - one row per (drug, disease) indication
"""

import polars as pl

from lacuna_etl.core.identifiers import ChemblId
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.opentargets._base import OpenTargetsProductPipeline
from lacuna_etl.datasets.registry import register


def transform_file(df: pl.DataFrame) -> dict[str, pl.DataFrame]:
    indications = df.select(
        pl.col("id").alias("indication_id"),
        pl.col("drugId").alias("drug_id"),
        pl.col("diseaseId").alias("disease_id"),
        pl.col("maxClinicalStage").alias("max_clinical_stage"),
        pl.col("clinicalReportIds").alias("clinical_report_ids"),
    )
    return {"clinical_indications": indications}


TABLES_DOC = {
    "clinical_indications": {
        "indication_id":       ColumnSpec(required=True, description="Open Targets indication record ID"),
        "drug_id":             ColumnSpec(identifier=ChemblId, description="ChEMBL molecule ID"),
        "disease_id":          ColumnSpec(description="Open Targets / EFO disease ontology ID"),
        "max_clinical_stage":  ColumnSpec(description="Maximum clinical stage reached for this indication"),
        "clinical_report_ids": ColumnSpec(description="Clinical report / trial IDs backing the indication (list)"),
    },
}


@register
class OpenTargetsClinicalIndication(OpenTargetsProductPipeline):
    name = "opentargets_clinical_indication"
    raw_dirname = "clinical_indication"
    transform_module = "lacuna_etl.datasets.opentargets.clinical_indication"
    first_table = "clinical_indications"
    tables_doc = TABLES_DOC
