"""Open Targets diseases / phenotypes (EFO-based ontology).

  diseases                  - one row per ontology term (scalar fields + ontology flags)
  diseases_synonyms         - one row per (disease, synonym) with its scope
  diseases_parents          - direct parent edges
  diseases_children         - direct child edges
  diseases_ancestors        - transitive ancestor edges
  diseases_descendants      - transitive descendant edges
  diseases_therapeutic_areas- disease -> therapeutic-area edges
  diseases_xrefs            - cross-references to other databases
  diseases_obsolete_terms   - obsolete term IDs merged into this one
  diseases_obsolete_xrefs   - obsolete cross-references
  diseases_ontology_sources - source ontologies the term was assembled from

disease IDs are heterogeneous CURIEs (EFO_, MONDO_, HP_, Orphanet_, ...), so like
PubTator's concept identifier they are stored as documented plain strings rather
than under a single canonical identifier type.
"""

import polars as pl

from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.opentargets._base import OpenTargetsProductPipeline
from lacuna_etl.datasets.opentargets._helpers import concat_tagged, explode_scalar_list
from lacuna_etl.datasets.registry import register

_KEY = {"id": "disease_id"}

_SYNONYM_SCOPES = {
    "exactSynonyms": "exact",
    "relatedSynonyms": "related",
    "narrowSynonyms": "narrow",
    "broadSynonyms": "broad",
}

# scalar-list edge tables: source column -> (output table, output value column)
_EDGES = {
    "parents":         ("diseases_parents", "parent_id"),
    "children":        ("diseases_children", "child_id"),
    "ancestors":       ("diseases_ancestors", "ancestor_id"),
    "descendants":     ("diseases_descendants", "descendant_id"),
    "therapeuticAreas": ("diseases_therapeutic_areas", "therapeutic_area_id"),
    "dbXRefs":         ("diseases_xrefs", "xref"),
    "obsoleteTerms":   ("diseases_obsolete_terms", "obsolete_term"),
    "obsoleteXRefs":   ("diseases_obsolete_xrefs", "obsolete_xref"),
}


def _ontology_flag(df: pl.DataFrame, field: str) -> pl.Expr:
    """Pull a boolean flag out of the `ontology` struct, or a null literal if absent."""
    if "ontology" in df.columns and isinstance(df.schema["ontology"], pl.Struct):
        names = {f.name for f in df.schema["ontology"].fields}
        if field in names:
            return pl.col("ontology").struct.field(field)
    return pl.lit(None, dtype=pl.Boolean)


def _ontology_sources(df: pl.DataFrame) -> pl.DataFrame:
    if "ontology" not in df.columns or not isinstance(df.schema["ontology"], pl.Struct):
        return pl.DataFrame()
    if "sources" not in {f.name for f in df.schema["ontology"].fields}:
        return pl.DataFrame()
    sc = df.select(
        pl.col("id").alias("disease_id"),
        pl.col("ontology").struct.field("sources").alias("sources"),
    )
    if isinstance(sc.schema["sources"], pl.List):
        sc = sc.explode("sources")
    sc = sc.filter(pl.col("sources").is_not_null())
    if not isinstance(sc.schema["sources"], pl.Struct):
        return pl.DataFrame()
    sc = sc.unnest("sources")
    return sc.select("disease_id", pl.col("url"), pl.col("name"))


def transform_file(df: pl.DataFrame) -> dict[str, pl.DataFrame]:
    diseases = df.select(
        pl.col("id").alias("disease_id"),
        pl.col("code").alias("code"),
        pl.col("name").alias("name"),
        pl.col("description").alias("description"),
        _ontology_flag(df, "isTherapeuticArea").alias("is_therapeutic_area"),
        _ontology_flag(df, "leaf").alias("is_leaf"),
    )

    synonym_parts = []
    for src, scope in _SYNONYM_SCOPES.items():
        part = explode_scalar_list(df, _KEY, src, "synonym")
        if not part.is_empty():
            synonym_parts.append(part.with_columns(pl.lit(scope).alias("synonym_scope")))
    synonyms = concat_tagged(synonym_parts)

    tables = {"diseases": diseases, "diseases_synonyms": synonyms,
              "diseases_ontology_sources": _ontology_sources(df)}
    for src, (table, out) in _EDGES.items():
        tables[table] = explode_scalar_list(df, _KEY, src, out)
    return tables


_DISEASES_DOC = {
    "disease_id":          ColumnSpec(required=True, description="Open Targets / EFO ontology ID (CURIE, e.g. EFO_0000274)"),
    "code":                ColumnSpec(description="Full ontology URI for the term"),
    "name":                ColumnSpec(description="Term label"),
    "description":         ColumnSpec(description="Term description"),
    "is_therapeutic_area": ColumnSpec(description="Whether the term is a therapeutic area"),
    "is_leaf":             ColumnSpec(description="Whether the term is a leaf in the ontology"),
}


def _edge_doc(value_col: str, value_desc: str) -> dict[str, ColumnSpec]:
    return {
        "disease_id": ColumnSpec(required=True, description="Open Targets / EFO ontology ID"),
        value_col:    ColumnSpec(description=value_desc),
    }


TABLES_DOC = {
    "diseases": _DISEASES_DOC,
    "diseases_synonyms": {
        "disease_id":     ColumnSpec(required=True, description="Open Targets / EFO ontology ID"),
        "synonym":        ColumnSpec(description="Synonym string"),
        "synonym_scope":  ColumnSpec(description="Synonym scope: exact, related, narrow, or broad"),
    },
    "diseases_parents":           _edge_doc("parent_id", "Direct parent term ID"),
    "diseases_children":          _edge_doc("child_id", "Direct child term ID"),
    "diseases_ancestors":         _edge_doc("ancestor_id", "Transitive ancestor term ID"),
    "diseases_descendants":       _edge_doc("descendant_id", "Transitive descendant term ID"),
    "diseases_therapeutic_areas": _edge_doc("therapeutic_area_id", "Therapeutic-area term ID this disease maps to"),
    "diseases_xrefs":             _edge_doc("xref", "Cross-reference to another database (CURIE)"),
    "diseases_obsolete_terms":    _edge_doc("obsolete_term", "Obsolete term ID merged into this one"),
    "diseases_obsolete_xrefs":    _edge_doc("obsolete_xref", "Obsolete cross-reference"),
    "diseases_ontology_sources": {
        "disease_id": ColumnSpec(required=True, description="Open Targets / EFO ontology ID"),
        "url":        ColumnSpec(description="Source ontology URL"),
        "name":       ColumnSpec(description="Source ontology name"),
    },
}


@register
class OpenTargetsDisease(OpenTargetsProductPipeline):
    name = "opentargets_disease"
    raw_dirname = "disease"
    transform_module = "lacuna_etl.datasets.opentargets.disease"
    first_table = "diseases"
    tables_doc = TABLES_DOC
