"""
Transform raw OpenAlex institution records into flat Polars DataFrames.

Produces three tables per batch:
  institutions            - one row per institution, scalar fields
  institutions_topics     - one row per (institution, topic) pair
  institutions_associated - one row per (institution, associated_institution) pair
"""

import polars as pl

from lacuna_etl.datasets.openalex._base import OpenAlexEntityPipeline
from lacuna_etl.datasets.openalex._utils import short_id
from lacuna_etl.datasets.registry import register

_INSTITUTIONS_SCHEMA = {
    "institution_id":       pl.String,
    "ror":                  pl.String,
    "display_name":         pl.String,
    "country_code":         pl.String,
    "type":                 pl.String,
    "is_super_system":      pl.Boolean,
    "works_count":          pl.Int64,
    "cited_by_count":       pl.Int64,
    "h_index":              pl.Int64,
    "i10_index":            pl.Int64,
    "2yr_mean_citedness":   pl.Float64,
    "homepage_url":         pl.String,
    "latitude":             pl.Float64,
    "longitude":            pl.Float64,
    "created_date":         pl.String,
    "updated_date":         pl.String,
}

_INST_TOPICS_SCHEMA = {
    "institution_id": pl.String,
    "topic_id":       pl.String,
    "count":          pl.Int64,
}

_INST_ASSOC_SCHEMA = {
    "institution_id":            pl.String,
    "associated_institution_id": pl.String,
    "relationship":              pl.String,
}


def transform_batch(records: list[dict]) -> dict[str, pl.DataFrame]:
    inst_rows, topic_rows, assoc_rows = [], [], []
    for r in records:
        inst_id = short_id(r.get("id"))
        if inst_id is None:
            continue
        stats = r.get("summary_stats") or {}
        geo = r.get("geo") or {}
        inst_rows.append({
            "institution_id":     inst_id,
            "ror":                r.get("ror"),
            "display_name":       r.get("display_name"),
            "country_code":       r.get("country_code"),
            "type":               r.get("type"),
            "is_super_system":    r.get("is_super_system"),
            "works_count":        r.get("works_count"),
            "cited_by_count":     r.get("cited_by_count"),
            "h_index":            stats.get("h_index"),
            "i10_index":          stats.get("i10_index"),
            "2yr_mean_citedness": stats.get("2yr_mean_citedness"),
            "homepage_url":       r.get("homepage_url"),
            "latitude":           geo.get("latitude"),
            "longitude":          geo.get("longitude"),
            "created_date":       r.get("created_date"),
            "updated_date":       r.get("updated_date"),
        })
        for t in r.get("topics") or []:
            topic_id = short_id(t.get("id"))
            if topic_id:
                topic_rows.append({
                    "institution_id": inst_id,
                    "topic_id":       topic_id,
                    "count":          t.get("count"),
                })
        for assoc in r.get("associated_institutions") or []:
            assoc_id = short_id(assoc.get("id"))
            if assoc_id:
                assoc_rows.append({
                    "institution_id":            inst_id,
                    "associated_institution_id": assoc_id,
                    "relationship":              assoc.get("relationship"),
                })
    return {
        "institutions":            pl.DataFrame(inst_rows,  schema=_INSTITUTIONS_SCHEMA),
        "institutions_topics":     pl.DataFrame(topic_rows, schema=_INST_TOPICS_SCHEMA),
        "institutions_associated": pl.DataFrame(assoc_rows, schema=_INST_ASSOC_SCHEMA),
    }


@register
class OpenAlexInstitutions(OpenAlexEntityPipeline):
    name = "openalex_institutions"
    raw_dirname = "institutions"
    transform_module = "lacuna_etl.datasets.openalex.institutions"
    first_table = "institutions"
