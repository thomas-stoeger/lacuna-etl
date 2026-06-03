"""Shared transform for the Open Targets association products.

All six association products — overall / by_datatype / by_datasource, each in a
`direct` and an `indirect` flavour — ship the identical schema: one row per
association carrying an `aggregationType` / `aggregationValue` discriminator, an
overall `associationScore`, and a `timeseries` list of per-year score / novelty /
evidence points. Only the grain differs by aggregation:

  overall        - one row per (target, disease)
  by_datatype    - one row per (target, disease, datatype)   [datatype in aggregation_value]
  by_datasource  - one row per (target, disease, datasource) [datasource in aggregation_value]

`direct` counts only evidence annotated directly to the disease; `indirect` also
propagates evidence from descendant terms up the disease ontology.
"""

import polars as pl

from lacuna_etl.core.identifiers import EnsemblGeneId
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.opentargets._helpers import explode_struct_list

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


def make_tables_doc(score_desc: str) -> dict[str, dict[str, ColumnSpec]]:
    """Build TABLES_DOC; `score_desc` describes what the overall score aggregates."""
    associations_doc = {
        "target_id":         ColumnSpec(identifier=EnsemblGeneId, required=True, description="Ensembl gene ID of the target"),
        "disease_id":        ColumnSpec(required=True, description="Open Targets / EFO disease (or phenotype) ontology ID"),
        "aggregation_type":  ColumnSpec(description="How the association is aggregated (overall, datatype, or datasource)"),
        "aggregation_value": ColumnSpec(description="Aggregation grouping value (the datatype or datasource ID; null for overall)"),
        "association_score": ColumnSpec(description=score_desc),
        "evidence_count":    ColumnSpec(description="Number of evidence records supporting the association"),
        "current_novelty":   ColumnSpec(description="Current novelty of the target-disease association"),
    }
    timeseries_doc = {
        "target_id":             ColumnSpec(identifier=EnsemblGeneId, required=True, description="Ensembl gene ID of the target"),
        "disease_id":            ColumnSpec(required=True, description="Open Targets / EFO disease (or phenotype) ontology ID"),
        "year":                  ColumnSpec(description="Calendar year of the data point"),
        "association_score":     ColumnSpec(description="Association score at this year"),
        "novelty":               ColumnSpec(description="Novelty score at this year"),
        "yearly_evidence_count": ColumnSpec(description="Number of evidence records in this year"),
    }
    return {"associations": associations_doc, "associations_timeseries": timeseries_doc}
