"""Open Targets enhancer-to-gene predictions.

  enhancer_gene_predictions        - one row per predicted enhancer -> gene link
  enhancer_gene_prediction_scores  - per-prediction named resource scores
"""

import polars as pl

from lacuna_etl.core.identifiers import EnsemblGeneId, PubmedId
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.opentargets._base import OpenTargetsProductPipeline
from lacuna_etl.datasets.opentargets._helpers import explode_struct_list
from lacuna_etl.datasets.registry import register

_KEY = {"intervalId": "interval_id"}


def transform_file(df: pl.DataFrame) -> dict[str, pl.DataFrame]:
    predictions = df.select(
        pl.col("intervalId").alias("interval_id"),
        pl.col("geneId").alias("gene_id"),
        pl.col("biosampleId").alias("biosample_id"),
        pl.col("biosampleName").alias("biosample_name"),
        pl.col("biosampleFromSourceId").alias("biosample_from_source_id"),
        pl.col("chromosome").alias("chromosome"),
        pl.col("start").cast(pl.Int64).alias("start"),
        pl.col("end").cast(pl.Int64).alias("end"),
        pl.col("score").alias("score"),
        pl.col("distanceToTss").cast(pl.Int64).alias("distance_to_tss"),
        pl.col("datasourceId").alias("datasource_id"),
        pl.col("intervalType").alias("interval_type"),
        pl.col("studyId").alias("study_id"),
        pl.col("pmid").cast(pl.Int64).alias("pmid"),
        pl.col("qualityControls").alias("quality_controls"),
    )
    scores = explode_struct_list(
        df, _KEY, "resourceScore", {"name": "score_name", "value": "value"},
    )
    return {
        "enhancer_gene_predictions": predictions,
        "enhancer_gene_prediction_scores": scores,
    }


TABLES_DOC = {
    "enhancer_gene_predictions": {
        "interval_id":              ColumnSpec(required=True, description="Source interval (enhancer) ID"),
        "gene_id":                  ColumnSpec(identifier=EnsemblGeneId, description="Ensembl gene ID the enhancer is linked to"),
        "biosample_id":             ColumnSpec(description="Biosample ontology ID the prediction was made in"),
        "biosample_name":           ColumnSpec(description="Biosample name"),
        "biosample_from_source_id": ColumnSpec(description="Biosample identifier as reported by the source"),
        "chromosome":               ColumnSpec(description="Chromosome of the enhancer interval"),
        "start":                    ColumnSpec(description="Start coordinate of the enhancer interval"),
        "end":                      ColumnSpec(description="End coordinate of the enhancer interval"),
        "score":                    ColumnSpec(description="Aggregated enhancer-gene link score"),
        "distance_to_tss":          ColumnSpec(description="Distance from the enhancer to the gene's TSS (bp)"),
        "datasource_id":            ColumnSpec(description="Prediction datasource ID"),
        "interval_type":            ColumnSpec(description="Interval type"),
        "study_id":                 ColumnSpec(description="Source study ID"),
        "pmid":                     ColumnSpec(identifier=PubmedId, description="Supporting PubMed ID"),
        "quality_controls":         ColumnSpec(description="Quality-control flags (list)"),
    },
    "enhancer_gene_prediction_scores": {
        "interval_id": ColumnSpec(required=True, description="Source interval (enhancer) ID"),
        "score_name":  ColumnSpec(description="Name of the resource score"),
        "value":       ColumnSpec(description="Score value"),
    },
}


@register
class OpenTargetsEnhancerToGene(OpenTargetsProductPipeline):
    name = "opentargets_enhancer_to_gene"
    raw_dirname = "enhancer_to_gene"
    transform_module = "lacuna_etl.datasets.opentargets.enhancer_to_gene"
    first_table = "enhancer_gene_predictions"
    tables_doc = TABLES_DOC
