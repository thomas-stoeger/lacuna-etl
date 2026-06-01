"""Open Targets literature word vectors (word2vec embeddings over mined entities).

  literature_vectors - one row per (category, word) with its embedding vector
"""

import polars as pl

from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.opentargets._base import OpenTargetsProductPipeline
from lacuna_etl.datasets.registry import register


def transform_file(df: pl.DataFrame) -> dict[str, pl.DataFrame]:
    vectors = df.select(
        pl.col("category").alias("category"),
        pl.col("word").alias("word"),
        pl.col("norm").alias("norm"),
        pl.col("vector").alias("vector"),
    )
    return {"literature_vectors": vectors}


TABLES_DOC = {
    "literature_vectors": {
        "category": ColumnSpec(description="Entity category of the word (target, disease, drug, ...)"),
        "word":     ColumnSpec(description="Word / entity ID the vector embeds"),
        "norm":     ColumnSpec(description="L2 norm of the embedding vector"),
        "vector":   ColumnSpec(description="Word2vec embedding vector (list of floats)"),
    },
}


@register
class OpenTargetsLiteratureVector(OpenTargetsProductPipeline):
    name = "opentargets_literature_vector"
    raw_dirname = "literature_vector"
    transform_module = "lacuna_etl.datasets.opentargets.literature_vector"
    first_table = "literature_vectors"
    tables_doc = TABLES_DOC
