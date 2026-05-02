"""
Transform raw OpenAlex publisher records into flat Polars DataFrames.

Produces one table per batch:
  publishers - one row per publisher
"""

import polars as pl

from lacuna_etl.datasets.openalex._base import OpenAlexEntityPipeline
from lacuna_etl.datasets.openalex._utils import short_id
from lacuna_etl.datasets.registry import register

_PUBLISHERS_SCHEMA = {
    "publisher_id":        pl.String,
    "display_name":        pl.String,
    "country_codes":       pl.List(pl.String),
    "hierarchy_level":     pl.Int64,
    "parent_publisher_id": pl.String,
    "works_count":         pl.Int64,
    "cited_by_count":      pl.Int64,
    "h_index":             pl.Int64,
    "i10_index":           pl.Int64,
    "2yr_mean_citedness":  pl.Float64,
    "created_date":        pl.String,
    "updated_date":        pl.String,
}


def transform_batch(records: list[dict]) -> dict[str, pl.DataFrame]:
    rows = []
    for r in records:
        publisher_id = short_id(r.get("id"))
        if publisher_id is None:
            continue
        stats = r.get("summary_stats") or {}
        parent = r.get("parent_publisher") or {}
        rows.append({
            "publisher_id":        publisher_id,
            "display_name":        r.get("display_name"),
            "country_codes":       r.get("country_codes") or [],
            "hierarchy_level":     r.get("hierarchy_level"),
            "parent_publisher_id": short_id(parent.get("id")),
            "works_count":         r.get("works_count"),
            "cited_by_count":      r.get("cited_by_count"),
            "h_index":             stats.get("h_index"),
            "i10_index":           stats.get("i10_index"),
            "2yr_mean_citedness":  stats.get("2yr_mean_citedness"),
            "created_date":        r.get("created_date"),
            "updated_date":        r.get("updated_date"),
        })
    return {"publishers": pl.DataFrame(rows, schema=_PUBLISHERS_SCHEMA)}


@register
class OpenAlexPublishers(OpenAlexEntityPipeline):
    name = "openalex_publishers"
    raw_dirname = "publishers"
    transform_module = "lacuna_etl.datasets.openalex.publishers"
    first_table = "publishers"
