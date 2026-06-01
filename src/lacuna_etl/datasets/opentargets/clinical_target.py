"""Open Targets clinical targets (drug -> target with clinical context).

  clinical_targets          - one row per (drug, target) clinical record
  clinical_targets_diseases - diseases the record applies to
"""

import polars as pl

from lacuna_etl.core.identifiers import ChemblId, EnsemblGeneId
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.opentargets._base import OpenTargetsProductPipeline
from lacuna_etl.datasets.opentargets._helpers import explode_struct_list
from lacuna_etl.datasets.registry import register

_KEY = {"id": "clinical_target_id"}


def transform_file(df: pl.DataFrame) -> dict[str, pl.DataFrame]:
    targets = df.select(
        pl.col("id").alias("clinical_target_id"),
        pl.col("drugId").alias("drug_id"),
        pl.col("targetId").alias("target_id"),
        pl.col("maxClinicalStage").alias("max_clinical_stage"),
        pl.col("clinicalReportIds").alias("clinical_report_ids"),
    )
    diseases = explode_struct_list(
        df, _KEY, "diseases",
        {"diseaseId": "disease_id", "diseaseFromSource": "disease_from_source"},
    )
    return {"clinical_targets": targets, "clinical_targets_diseases": diseases}


TABLES_DOC = {
    "clinical_targets": {
        "clinical_target_id":  ColumnSpec(required=True, description="Open Targets clinical-target record ID"),
        "drug_id":             ColumnSpec(identifier=ChemblId, description="ChEMBL molecule ID"),
        "target_id":           ColumnSpec(identifier=EnsemblGeneId, description="Ensembl gene ID of the target"),
        "max_clinical_stage":  ColumnSpec(description="Maximum clinical stage reached"),
        "clinical_report_ids": ColumnSpec(description="Clinical report / trial IDs (list)"),
    },
    "clinical_targets_diseases": {
        "clinical_target_id": ColumnSpec(required=True, description="Open Targets clinical-target record ID"),
        "disease_id":         ColumnSpec(description="Open Targets / EFO disease ontology ID"),
        "disease_from_source": ColumnSpec(description="Disease label as reported by the source"),
    },
}


@register
class OpenTargetsClinicalTarget(OpenTargetsProductPipeline):
    name = "opentargets_clinical_target"
    raw_dirname = "clinical_target"
    transform_module = "lacuna_etl.datasets.opentargets.clinical_target"
    first_table = "clinical_targets"
    tables_doc = TABLES_DOC
