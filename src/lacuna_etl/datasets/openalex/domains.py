"""domains lookup table."""

import polars as pl

from lacuna_etl.datasets.openalex._base import OpenAlexEntityPipeline
from lacuna_etl.datasets.openalex._utils import short_id
from lacuna_etl.datasets.registry import register

_DOMAINS_SCHEMA = {
    "domain_id":      pl.String,
    "display_name":   pl.String,
    "description":    pl.String,
    "works_count":    pl.Int64,
    "cited_by_count": pl.Int64,
    "updated_date":   pl.String,
}


def transform_batch(records: list[dict]) -> dict[str, pl.DataFrame]:
    rows = []
    for r in records:
        domain_id = short_id(r.get("id"))
        if domain_id is None:
            continue
        rows.append({
            "domain_id":      domain_id,
            "display_name":   r.get("display_name"),
            "description":    r.get("description"),
            "works_count":    r.get("works_count"),
            "cited_by_count": r.get("cited_by_count"),
            "updated_date":   r.get("updated_date"),
        })
    return {"domains": pl.DataFrame(rows, schema=_DOMAINS_SCHEMA)}


@register
class OpenAlexDomains(OpenAlexEntityPipeline):
    name = "openalex_domains"
    raw_dirname = "domains"
    transform_module = "lacuna_etl.datasets.openalex.domains"
    first_table = "domains"
