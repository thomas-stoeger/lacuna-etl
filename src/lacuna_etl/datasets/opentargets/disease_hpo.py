"""Open Targets Human Phenotype Ontology (HPO) terms.

  hpo_terms                 - one row per HPO term (scalar fields)
  hpo_terms_xrefs           - cross-references
  hpo_terms_parents         - direct parent edges
  hpo_terms_obsolete_terms  - obsolete term IDs merged into this one
"""

import polars as pl

from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.opentargets._base import OpenTargetsProductPipeline
from lacuna_etl.datasets.opentargets._helpers import explode_scalar_list
from lacuna_etl.datasets.registry import register

_KEY = {"id": "hpo_id"}

_EDGES = {
    "dbXRefs":       ("hpo_terms_xrefs", "xref"),
    "parents":       ("hpo_terms_parents", "parent_id"),
    "obsoleteTerms": ("hpo_terms_obsolete_terms", "obsolete_term"),
}


def transform_file(df: pl.DataFrame) -> dict[str, pl.DataFrame]:
    hpo_terms = df.select(
        pl.col("id").alias("hpo_id"),
        pl.col("name").alias("name"),
        pl.col("description").alias("description"),
    )
    tables = {"hpo_terms": hpo_terms}
    for src, (table, out) in _EDGES.items():
        tables[table] = explode_scalar_list(df, _KEY, src, out)
    return tables


def _edge_doc(value_col: str, value_desc: str) -> dict[str, ColumnSpec]:
    return {
        "hpo_id":  ColumnSpec(required=True, description="Human Phenotype Ontology term ID"),
        value_col: ColumnSpec(description=value_desc),
    }


TABLES_DOC = {
    "hpo_terms": {
        "hpo_id":      ColumnSpec(required=True, description="Human Phenotype Ontology term ID (e.g. HP_0000118)"),
        "name":        ColumnSpec(description="Term label"),
        "description": ColumnSpec(description="Term description"),
    },
    "hpo_terms_xrefs":          _edge_doc("xref", "Cross-reference to another database"),
    "hpo_terms_parents":        _edge_doc("parent_id", "Direct parent term ID"),
    "hpo_terms_obsolete_terms": _edge_doc("obsolete_term", "Obsolete term ID merged into this one"),
}


@register
class OpenTargetsDiseaseHpo(OpenTargetsProductPipeline):
    name = "opentargets_disease_hpo"
    raw_dirname = "disease_hpo"
    transform_module = "lacuna_etl.datasets.opentargets.disease_hpo"
    first_table = "hpo_terms"
    tables_doc = TABLES_DOC
