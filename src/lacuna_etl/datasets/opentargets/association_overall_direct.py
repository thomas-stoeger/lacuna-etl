"""Open Targets overall direct target-disease associations.

  associations            - one row per (target, disease) association with its overall score
  associations_timeseries - one row per (target, disease, year) novelty/score point
"""

import polars as pl

from lacuna_etl.core.identifiers import EnsemblGeneId
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.opentargets._base import OpenTargetsProductPipeline
from lacuna_etl.datasets.opentargets._helpers import explode_struct_list
from lacuna_etl.datasets.registry import register

_KEYS = {"targetId": "target_id", "diseaseId": "disease_id"}


def transform_file(df: pl.DataFrame) -> dict[str, pl.DataFrame]:
    associations = df.select(
        pl.col("targetId").alias("target_id"),
        pl.col("diseaseId").alias("disease_id"),
        pl.col("aggregationType").alias("aggregation_type"),
        pl.col("aggregationValue").alias("aggregation_value"),
        pl.col("associationScore").alias("association_score"),
        pl.col("evidenceCount").cast(pl.Int64).alias("evidence_count"),
        pl.col("currentNovelty").alias("current_novelty"),
    )
    timeseries = explode_struct_list(
        df, _KEYS, "timeseries",
        {
            "year": "year",
            "associationScore": "association_score",
            "novelty": "novelty",
            "yearlyEvidenceCount": "yearly_evidence_count",
        },
        casts={"year": pl.Int64, "yearly_evidence_count": pl.Int64},
    )
    return {"associations": associations, "associations_timeseries": timeseries}


_ASSOCIATIONS_DOC = {
    "target_id":         ColumnSpec(identifier=EnsemblGeneId, required=True, description="Ensembl gene ID of the target"),
    "disease_id":        ColumnSpec(required=True, description="Open Targets / EFO disease (or phenotype) ontology ID"),
    "aggregation_type":  ColumnSpec(description="How the association is aggregated (e.g. overall)"),
    "aggregation_value": ColumnSpec(description="Aggregation grouping value"),
    "association_score": ColumnSpec(description="Overall direct association score in [0, 1]"),
    "evidence_count":    ColumnSpec(description="Number of evidence records supporting the association"),
    "current_novelty":   ColumnSpec(description="Current novelty of the target-disease association"),
}

_TIMESERIES_DOC = {
    "target_id":            ColumnSpec(identifier=EnsemblGeneId, required=True, description="Ensembl gene ID of the target"),
    "disease_id":           ColumnSpec(required=True, description="Open Targets / EFO disease (or phenotype) ontology ID"),
    "year":                 ColumnSpec(description="Calendar year of the data point"),
    "association_score":    ColumnSpec(description="Association score at this year"),
    "novelty":              ColumnSpec(description="Novelty score at this year"),
    "yearly_evidence_count": ColumnSpec(description="Number of evidence records in this year"),
}

TABLES_DOC = {
    "associations": _ASSOCIATIONS_DOC,
    "associations_timeseries": _TIMESERIES_DOC,
}


@register
class OpenTargetsAssociationOverallDirect(OpenTargetsProductPipeline):
    name = "opentargets_association_overall_direct"
    raw_dirname = "association_overall_direct"
    transform_module = "lacuna_etl.datasets.opentargets.association_overall_direct"
    first_table = "associations"
    tables_doc = TABLES_DOC
