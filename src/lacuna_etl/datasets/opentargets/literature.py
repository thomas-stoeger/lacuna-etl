"""Open Targets literature (per-publication mined keyword occurrences).

  literature - one row per (publication, keyword) occurrence with its relevance
"""

import polars as pl

from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.opentargets._base import OpenTargetsProductPipeline
from lacuna_etl.datasets.registry import register


def transform_file(df: pl.DataFrame) -> dict[str, pl.DataFrame]:
    literature = df.select(
        pl.col("pmid").alias("pmid"),
        pl.col("pmcid").alias("pmcid"),
        pl.col("keywordId").alias("keyword_id"),
        pl.col("keywordType").alias("keyword_type"),
        pl.col("relevance").alias("relevance"),
        pl.col("date").alias("date"),
        pl.col("year").cast(pl.Int64).alias("year"),
        pl.col("month").cast(pl.Int64).alias("month"),
        pl.col("day").cast(pl.Int64).alias("day"),
    )
    return {"literature": literature}


TABLES_DOC = {
    "literature": {
        "pmid":         ColumnSpec(description="Publication ID: a PubMed ID, or a Europe PMC 'IND...' ID for non-PubMed sources"),
        "pmcid":        ColumnSpec(description="PubMed Central ID of the publication"),
        "keyword_id":   ColumnSpec(description="Mined keyword/entity ID (target, disease, or drug ID)"),
        "keyword_type": ColumnSpec(description="Keyword type (TARGET, DISEASE, DRUG)"),
        "relevance":    ColumnSpec(description="Relevance score of the keyword to the publication"),
        "date":         ColumnSpec(description="Publication date"),
        "year":         ColumnSpec(description="Publication year"),
        "month":        ColumnSpec(description="Publication month"),
        "day":          ColumnSpec(description="Publication day"),
    },
}


@register
class OpenTargetsLiterature(OpenTargetsProductPipeline):
    name = "opentargets_literature"
    raw_dirname = "literature"
    transform_module = "lacuna_etl.datasets.opentargets.literature"
    first_table = "literature"
    tables_doc = TABLES_DOC
