"""Open Targets locus-to-gene (L2G) predictions.

  l2g_predictions          - one row per (study-locus, gene) prediction with its L2G score
  l2g_prediction_features  - per-prediction feature contributions (SHAP values)
"""

import polars as pl

from lacuna_etl.core.identifiers import EnsemblGeneId
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.opentargets._base import OpenTargetsProductPipeline
from lacuna_etl.datasets.opentargets._helpers import explode_struct_list
from lacuna_etl.datasets.registry import register

_KEYS = {"studyLocusId": "study_locus_id", "geneId": "gene_id"}


def transform_file(df: pl.DataFrame) -> dict[str, pl.DataFrame]:
    predictions = df.select(
        pl.col("studyLocusId").alias("study_locus_id"),
        pl.col("geneId").alias("gene_id"),
        pl.col("score").alias("score"),
        pl.col("shapBaseValue").alias("shap_base_value"),
    )
    features = explode_struct_list(
        df, _KEYS, "features",
        {"name": "feature_name", "value": "value", "shapValue": "shap_value"},
    )
    return {"l2g_predictions": predictions, "l2g_prediction_features": features}


TABLES_DOC = {
    "l2g_predictions": {
        "study_locus_id":  ColumnSpec(required=True, description="GWAS study-locus ID"),
        "gene_id":         ColumnSpec(identifier=EnsemblGeneId, required=True, description="Ensembl gene ID of the predicted causal gene"),
        "score":           ColumnSpec(description="Locus-to-gene score in [0, 1]"),
        "shap_base_value": ColumnSpec(description="SHAP base (expected) value of the L2G model"),
    },
    "l2g_prediction_features": {
        "study_locus_id": ColumnSpec(required=True, description="GWAS study-locus ID"),
        "gene_id":        ColumnSpec(identifier=EnsemblGeneId, required=True, description="Ensembl gene ID"),
        "feature_name":   ColumnSpec(description="L2G feature name"),
        "value":          ColumnSpec(description="Feature value"),
        "shap_value":     ColumnSpec(description="SHAP contribution of the feature to the score"),
    },
}


@register
class OpenTargetsL2gPrediction(OpenTargetsProductPipeline):
    name = "opentargets_l2g_prediction"
    raw_dirname = "l2g_prediction"
    transform_module = "lacuna_etl.datasets.opentargets.l2g_prediction"
    first_table = "l2g_predictions"
    tables_doc = TABLES_DOC
