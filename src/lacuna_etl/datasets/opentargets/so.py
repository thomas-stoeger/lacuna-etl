"""Open Targets Sequence Ontology terms.

  so_terms - one row per Sequence Ontology term (ID + label)
"""

import polars as pl

from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.opentargets._base import OpenTargetsProductPipeline
from lacuna_etl.datasets.registry import register


def transform_file(df: pl.DataFrame) -> dict[str, pl.DataFrame]:
    so_terms = df.select(
        pl.col("id").alias("so_id"),
        pl.col("label").alias("label"),
    )
    return {"so_terms": so_terms}


TABLES_DOC = {
    "so_terms": {
        "so_id": ColumnSpec(required=True, description="Sequence Ontology term ID (e.g. SO:0001583)"),
        "label": ColumnSpec(description="Term label"),
    },
}


@register
class OpenTargetsSo(OpenTargetsProductPipeline):
    name = "opentargets_so"
    raw_dirname = "so"
    transform_module = "lacuna_etl.datasets.opentargets.so"
    first_table = "so_terms"
    tables_doc = TABLES_DOC
