"""fields lookup table."""

import polars as pl

from lacuna_etl.datasets.openalex._base import OpenAlexEntityPipeline
from lacuna_etl.datasets.openalex._utils import short_id
from lacuna_etl.datasets.registry import register

_FIELDS_SCHEMA = {
    "field_id":       pl.String,
    "display_name":   pl.String,
    "description":    pl.String,
    "domain_id":      pl.String,
    "works_count":    pl.Int64,
    "cited_by_count": pl.Int64,
    "updated_date":   pl.String,
}


def transform_batch(records: list[dict]) -> dict[str, pl.DataFrame]:
    rows = []
    for r in records:
        field_id = short_id(r.get("id"))
        if field_id is None:
            continue
        domain = r.get("domain") or {}
        rows.append({
            "field_id":       field_id,
            "display_name":   r.get("display_name"),
            "description":    r.get("description"),
            "domain_id":      short_id(domain.get("id")),
            "works_count":    r.get("works_count"),
            "cited_by_count": r.get("cited_by_count"),
            "updated_date":   r.get("updated_date"),
        })
    return {"fields": pl.DataFrame(rows, schema=_FIELDS_SCHEMA)}


@register
class OpenAlexFields(OpenAlexEntityPipeline):
    name = "openalex_fields"
    raw_dirname = "fields"
    transform_module = "lacuna_etl.datasets.openalex.fields"
    first_table = "fields"
