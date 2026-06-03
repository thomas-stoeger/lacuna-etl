"""Open Targets Gene Ontology terms.

  go_terms                       - one row per GO term (scalar fields + obsolete flag)
  go_terms_alt_ids               - alternative/secondary GO IDs
  go_terms_is_a                  - is_a parent edges
  go_terms_part_of               - part_of edges
  go_terms_regulates             - regulates edges
  go_terms_negatively_regulates  - negatively_regulates edges
  go_terms_positively_regulates  - positively_regulates edges
"""

import polars as pl

from lacuna_etl.core.identifiers import GoId
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.opentargets._base import OpenTargetsProductPipeline
from lacuna_etl.datasets.opentargets._helpers import explode_scalar_list
from lacuna_etl.datasets.registry import register

_KEY = {"id": "go_id"}

# source list column -> (output table, output value column, value is a GO id?)
_EDGES = {
    "altIds":              ("go_terms_alt_ids", "alt_go_id", True),
    "isA":                 ("go_terms_is_a", "parent_go_id", True),
    "partOf":              ("go_terms_part_of", "part_of_go_id", True),
    "regulates":           ("go_terms_regulates", "regulates_go_id", True),
    "negativelyRegulates": ("go_terms_negatively_regulates", "regulates_go_id", True),
    "positivelyRegulates": ("go_terms_positively_regulates", "regulates_go_id", True),
}


def transform_file(df: pl.DataFrame) -> dict[str, pl.DataFrame]:
    go_terms = df.select(
        pl.col("id").alias("go_id"),
        pl.col("label").alias("label"),
        pl.col("namespace").alias("namespace"),
        pl.col("isObsolete").alias("is_obsolete"),
    )
    tables = {"go_terms": go_terms}
    for src, (table, out, _is_go) in _EDGES.items():
        tables[table] = explode_scalar_list(df, _KEY, src, out)
    return tables


def _edge_doc(value_col: str, value_desc: str, is_go: bool) -> dict[str, ColumnSpec]:
    return {
        "go_id":   ColumnSpec(identifier=GoId, required=True, description="Gene Ontology term ID"),
        value_col: ColumnSpec(identifier=GoId if is_go else None, description=value_desc),
    }


TABLES_DOC = {
    "go_terms": {
        "go_id":       ColumnSpec(identifier=GoId, required=True, description="Gene Ontology term ID"),
        "label":       ColumnSpec(description="Term label"),
        "namespace":   ColumnSpec(description="GO namespace (biological_process, molecular_function, cellular_component)"),
        "is_obsolete": ColumnSpec(description="Whether the term is obsolete"),
    },
    "go_terms_alt_ids":              _edge_doc("alt_go_id", "Alternative/secondary GO ID for this term", True),
    "go_terms_is_a":                 _edge_doc("parent_go_id", "is_a parent GO term ID", True),
    "go_terms_part_of":              _edge_doc("part_of_go_id", "part_of target GO term ID", True),
    "go_terms_regulates":            _edge_doc("regulates_go_id", "GO term ID this term regulates", True),
    "go_terms_negatively_regulates": _edge_doc("regulates_go_id", "GO term ID this term negatively regulates", True),
    "go_terms_positively_regulates": _edge_doc("regulates_go_id", "GO term ID this term positively regulates", True),
}


@register
class OpenTargetsGo(OpenTargetsProductPipeline):
    name = "opentargets_go"
    raw_dirname = "go"
    transform_module = "lacuna_etl.datasets.opentargets.go"
    first_table = "go_terms"
    tables_doc = TABLES_DOC
