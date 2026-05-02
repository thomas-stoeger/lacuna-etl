"""countries lookup table."""

import polars as pl

from lacuna_etl.datasets.openalex._base import OpenAlexEntityPipeline
from lacuna_etl.datasets.openalex._utils import short_id
from lacuna_etl.datasets.registry import register

_COUNTRIES_SCHEMA = {
    "country_id":       pl.String,
    "country_code":     pl.String,
    "alpha_3":          pl.String,
    "numeric":          pl.Int64,
    "display_name":     pl.String,
    "full_name":        pl.String,
    "continent_id":     pl.String,
    "is_global_south":  pl.Boolean,
    "works_count":      pl.Int64,
    "cited_by_count":   pl.Int64,
    "updated_date":     pl.String,
}


def transform_batch(records: list[dict]) -> dict[str, pl.DataFrame]:
    rows = []
    for r in records:
        country_id = short_id(r.get("id"))
        if country_id is None:
            continue
        continent = r.get("continent") or {}
        rows.append({
            "country_id":      country_id,
            "country_code":    r.get("country_code"),
            "alpha_3":         r.get("alpha_3"),
            "numeric":         r.get("numeric"),
            "display_name":    r.get("display_name"),
            "full_name":       r.get("full_name"),
            "continent_id":    short_id(continent.get("id")),
            "is_global_south": r.get("is_global_south"),
            "works_count":     r.get("works_count"),
            "cited_by_count":  r.get("cited_by_count"),
            "updated_date":    r.get("updated_date"),
        })
    return {"countries": pl.DataFrame(rows, schema=_COUNTRIES_SCHEMA)}


@register
class OpenAlexCountries(OpenAlexEntityPipeline):
    name = "openalex_countries"
    raw_dirname = "countries"
    transform_module = "lacuna_etl.datasets.openalex.countries"
    first_table = "countries"
