"""
Transform raw OpenAlex concept records into flat Polars DataFrames.

Note: Concepts are deprecated in OpenAlex in favour of topics/fields/subfields/domains,
but the snapshot still includes them for backward compatibility.

Produces three tables per batch:
  concepts           - one row per concept
  concepts_ancestors - one row per (concept, ancestor) pair
  concepts_related   - one row per (concept, related_concept) pair with score
"""

import polars as pl

from lacuna_etl.datasets.openalex._base import OpenAlexEntityPipeline
from lacuna_etl.datasets.openalex._utils import short_id
from lacuna_etl.datasets.registry import register

_CONCEPTS_SCHEMA = {
    "concept_id":      pl.String,
    "display_name":    pl.String,
    "level":           pl.Int64,
    "description":     pl.String,
    "wikidata":        pl.String,
    "works_count":     pl.Int64,
    "cited_by_count":  pl.Int64,
    "updated_date":    pl.String,
    "created_date":    pl.String,
}

_ANCESTORS_SCHEMA = {
    "concept_id":      pl.String,
    "ancestor_id":     pl.String,
    "ancestor_level":  pl.Int64,
}

_RELATED_SCHEMA = {
    "concept_id":   pl.String,
    "related_id":   pl.String,
    "score":        pl.Float64,
}


def transform_batch(records: list[dict]) -> dict[str, pl.DataFrame]:
    concept_rows, ancestor_rows, related_rows = [], [], []
    for r in records:
        concept_id = short_id(r.get("id"))
        if concept_id is None:
            continue
        concept_rows.append({
            "concept_id":     concept_id,
            "display_name":   r.get("display_name"),
            "level":          r.get("level"),
            "description":    r.get("description"),
            "wikidata":       r.get("wikidata"),
            "works_count":    r.get("works_count"),
            "cited_by_count": r.get("cited_by_count"),
            "updated_date":   r.get("updated_date"),
            "created_date":   r.get("created_date"),
        })
        for anc in r.get("ancestors") or []:
            anc_id = short_id(anc.get("id"))
            if anc_id:
                ancestor_rows.append({
                    "concept_id":     concept_id,
                    "ancestor_id":    anc_id,
                    "ancestor_level": anc.get("level"),
                })
        for rel in r.get("related_concepts") or []:
            rel_id = short_id(rel.get("id"))
            if rel_id:
                related_rows.append({
                    "concept_id": concept_id,
                    "related_id": rel_id,
                    "score":      rel.get("score"),
                })
    return {
        "concepts":           pl.DataFrame(concept_rows,   schema=_CONCEPTS_SCHEMA),
        "concepts_ancestors": pl.DataFrame(ancestor_rows,  schema=_ANCESTORS_SCHEMA),
        "concepts_related":   pl.DataFrame(related_rows,   schema=_RELATED_SCHEMA),
    }


@register
class OpenAlexConcepts(OpenAlexEntityPipeline):
    name = "openalex_concepts"
    raw_dirname = "concepts"
    transform_module = "lacuna_etl.datasets.openalex.concepts"
    first_table = "concepts"
