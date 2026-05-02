"""licenses lookup table."""

import polars as pl

from lacuna_etl.datasets.openalex._base import OpenAlexEntityPipeline
from lacuna_etl.datasets.openalex._utils import short_id
from lacuna_etl.datasets.registry import register

_LICENSES_SCHEMA = {
    "license_id":     pl.String,
    "display_name":   pl.String,
    "url":            pl.String,
    "description":    pl.String,
    "works_count":    pl.Int64,
    "cited_by_count": pl.Int64,
    "updated_date":   pl.String,
}


def transform_batch(records: list[dict]) -> dict[str, pl.DataFrame]:
    rows = []
    for r in records:
        lic_id = short_id(r.get("id"))
        if lic_id is None:
            continue
        rows.append({
            "license_id":     lic_id,
            "display_name":   r.get("display_name"),
            "url":            r.get("url"),
            "description":    r.get("description"),
            "works_count":    r.get("works_count"),
            "cited_by_count": r.get("cited_by_count"),
            "updated_date":   r.get("updated_date"),
        })
    return {"licenses": pl.DataFrame(rows, schema=_LICENSES_SCHEMA)}


@register
class OpenAlexLicenses(OpenAlexEntityPipeline):
    name = "openalex_licenses"
    raw_dirname = "licenses"
    transform_module = "lacuna_etl.datasets.openalex.licenses"
    first_table = "licenses"
