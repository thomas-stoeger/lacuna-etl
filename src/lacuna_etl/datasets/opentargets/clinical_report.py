"""Open Targets clinical reports (clinical trials and other clinical sources).

  clinical_reports              - one row per clinical report / trial
  clinical_reports_drugs        - drugs studied in the report
  clinical_reports_diseases     - diseases / conditions of the report
  clinical_reports_side_effects - side-effect diseases reported
"""

import polars as pl

from lacuna_etl.core.identifiers import ChemblId
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.opentargets._base import OpenTargetsProductPipeline
from lacuna_etl.datasets.opentargets._helpers import explode_struct_list
from lacuna_etl.datasets.registry import register

_KEY = {"id": "report_id"}


def transform_file(df: pl.DataFrame) -> dict[str, pl.DataFrame]:
    reports = df.select(
        pl.col("id").alias("report_id"),
        pl.col("type").alias("type"),
        pl.col("source").alias("source"),
        pl.col("clinicalStage").alias("clinical_stage"),
        pl.col("phaseFromSource").alias("phase_from_source"),
        pl.col("trialPhase").alias("trial_phase"),
        pl.col("trialStudyType").alias("trial_study_type"),
        pl.col("trialPrimaryPurpose").alias("trial_primary_purpose"),
        pl.col("trialNumberOfArms").cast(pl.Int64).alias("trial_number_of_arms"),
        pl.col("trialOverallStatus").alias("trial_overall_status"),
        pl.col("trialWhyStopped").alias("trial_why_stopped"),
        pl.col("trialDescription").alias("trial_description"),
        pl.col("trialOfficialTitle").alias("trial_official_title"),
        pl.col("title").alias("title"),
        pl.col("hasExpertReview").alias("has_expert_review"),
        pl.col("trialStartDate").alias("trial_start_date"),
        pl.col("year").cast(pl.Int64).alias("year"),
        pl.col("url").alias("url"),
        pl.col("countries").alias("countries"),
        pl.col("trialStopReasonCategories").alias("trial_stop_reason_categories"),
        pl.col("trialLiterature").alias("trial_literature"),
        pl.col("qualityControls").alias("quality_controls"),
    )
    drugs = explode_struct_list(
        df, _KEY, "drugs", {"drugId": "drug_id", "drugFromSource": "drug_from_source"}
    )
    diseases = explode_struct_list(
        df, _KEY, "diseases", {"diseaseId": "disease_id", "diseaseFromSource": "disease_from_source"}
    )
    side_effects = explode_struct_list(
        df, _KEY, "sideEffects", {"diseaseId": "disease_id", "diseaseFromSource": "disease_from_source"}
    )
    return {
        "clinical_reports": reports,
        "clinical_reports_drugs": drugs,
        "clinical_reports_diseases": diseases,
        "clinical_reports_side_effects": side_effects,
    }


_DISEASE_CHILD = {
    "report_id":           ColumnSpec(required=True, description="Open Targets clinical-report ID"),
    "disease_id":          ColumnSpec(description="Open Targets / EFO disease ontology ID"),
    "disease_from_source": ColumnSpec(description="Disease label as reported by the source"),
}

TABLES_DOC = {
    "clinical_reports": {
        "report_id":                    ColumnSpec(required=True, description="Open Targets clinical-report ID"),
        "type":                         ColumnSpec(description="Report type"),
        "source":                       ColumnSpec(description="Report source (e.g. ClinicalTrials.gov)"),
        "clinical_stage":               ColumnSpec(description="Clinical stage"),
        "phase_from_source":            ColumnSpec(description="Trial phase as reported by the source"),
        "trial_phase":                  ColumnSpec(description="Harmonised trial phase"),
        "trial_study_type":             ColumnSpec(description="Trial study type"),
        "trial_primary_purpose":        ColumnSpec(description="Trial primary purpose"),
        "trial_number_of_arms":         ColumnSpec(description="Number of trial arms"),
        "trial_overall_status":         ColumnSpec(description="Overall trial status"),
        "trial_why_stopped":            ColumnSpec(description="Reason the trial was stopped, if any"),
        "trial_description":            ColumnSpec(description="Trial description"),
        "trial_official_title":         ColumnSpec(description="Official trial title"),
        "title":                        ColumnSpec(description="Report title"),
        "has_expert_review":            ColumnSpec(description="Whether the report had expert review"),
        "trial_start_date":            ColumnSpec(description="Trial start date"),
        "year":                         ColumnSpec(description="Report year"),
        "url":                          ColumnSpec(description="Source URL"),
        "countries":                    ColumnSpec(description="Countries of the trial (list)"),
        "trial_stop_reason_categories": ColumnSpec(description="Categorised trial stop reasons (list)"),
        "trial_literature":             ColumnSpec(description="Supporting literature references (list)"),
        "quality_controls":             ColumnSpec(description="Quality-control flags (list)"),
    },
    "clinical_reports_drugs": {
        "report_id":       ColumnSpec(required=True, description="Open Targets clinical-report ID"),
        "drug_id":         ColumnSpec(identifier=ChemblId, description="ChEMBL molecule ID"),
        "drug_from_source": ColumnSpec(description="Drug identifier/name as reported by the source"),
    },
    "clinical_reports_diseases": _DISEASE_CHILD,
    "clinical_reports_side_effects": _DISEASE_CHILD,
}


@register
class OpenTargetsClinicalReport(OpenTargetsProductPipeline):
    name = "opentargets_clinical_report"
    raw_dirname = "clinical_report"
    transform_module = "lacuna_etl.datasets.opentargets.clinical_report"
    first_table = "clinical_reports"
    tables_doc = TABLES_DOC
