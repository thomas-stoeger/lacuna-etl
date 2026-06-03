"""Open Targets biosamples (cell types / tissues ontology).

  biosamples             - one row per biosample term (scalar fields)
  biosamples_synonyms    - synonym strings
  biosamples_xrefs       - cross-references
  biosamples_parents     - direct parent edges
  biosamples_children    - direct child edges
  biosamples_ancestors   - transitive ancestor edges
  biosamples_descendants - transitive descendant edges
"""

from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.opentargets._base import OpenTargetsProductPipeline
from lacuna_etl.datasets.opentargets._helpers import explode_scalar_list
from lacuna_etl.datasets.registry import register

import polars as pl

_KEY = {"biosampleId": "biosample_id"}

# source list column -> (output table, output value column)
_EDGES = {
    "synonyms":    ("biosamples_synonyms", "synonym"),
    "xrefs":       ("biosamples_xrefs", "xref"),
    "parents":     ("biosamples_parents", "parent_id"),
    "children":    ("biosamples_children", "child_id"),
    "ancestors":   ("biosamples_ancestors", "ancestor_id"),
    "descendants": ("biosamples_descendants", "descendant_id"),
}


def transform_file(df: pl.DataFrame) -> dict[str, pl.DataFrame]:
    biosamples = df.select(
        pl.col("biosampleId").alias("biosample_id"),
        pl.col("biosampleName").alias("biosample_name"),
        pl.col("description").alias("description"),
    )
    tables = {"biosamples": biosamples}
    for src, (table, out) in _EDGES.items():
        tables[table] = explode_scalar_list(df, _KEY, src, out)
    return tables


def _edge_doc(value_col: str, value_desc: str) -> dict[str, ColumnSpec]:
    return {
        "biosample_id": ColumnSpec(required=True, description="Biosample ontology ID (Cell Ontology / UBERON / EFO)"),
        value_col:      ColumnSpec(description=value_desc),
    }


TABLES_DOC = {
    "biosamples": {
        "biosample_id":   ColumnSpec(required=True, description="Biosample ontology ID (Cell Ontology / UBERON / EFO)"),
        "biosample_name": ColumnSpec(description="Biosample term label"),
        "description":    ColumnSpec(description="Term description"),
    },
    "biosamples_synonyms":    {"biosample_id": ColumnSpec(required=True, description="Biosample ontology ID"),
                               "synonym": ColumnSpec(description="Synonym string")},
    "biosamples_xrefs":       _edge_doc("xref", "Cross-reference to another database"),
    "biosamples_parents":     _edge_doc("parent_id", "Direct parent term ID"),
    "biosamples_children":    _edge_doc("child_id", "Direct child term ID"),
    "biosamples_ancestors":   _edge_doc("ancestor_id", "Transitive ancestor term ID"),
    "biosamples_descendants": _edge_doc("descendant_id", "Transitive descendant term ID"),
}


@register
class OpenTargetsBiosample(OpenTargetsProductPipeline):
    name = "opentargets_biosample"
    raw_dirname = "biosample"
    transform_module = "lacuna_etl.datasets.opentargets.biosample"
    first_table = "biosamples"
    tables_doc = TABLES_DOC
