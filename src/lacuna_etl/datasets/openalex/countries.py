"""countries lookup table."""

import polars as pl

from lacuna_etl.core.identifiers import OpenAlexContinentId, CountryCode, CountryCodeAlpha3, OpenAlexCountryId
from lacuna_etl.core.schema import ColumnSpec
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


_COUNTRIES_DOC = {
    "country_id":      ColumnSpec(identifier=OpenAlexCountryId,         required=True, description="OpenAlex country identifier"),
    "country_code":    ColumnSpec(identifier=CountryCode,       description="ISO 3166-1 alpha-2 country code"),
    "alpha_3":         ColumnSpec(identifier=CountryCodeAlpha3, description="ISO 3166-1 alpha-3 country code"),
    "numeric":         ColumnSpec(description="ISO 3166-1 numeric country code"),
    "display_name":    ColumnSpec(description="Common country name"),
    "full_name":       ColumnSpec(description="Official country name"),
    "continent_id":    ColumnSpec(identifier=OpenAlexContinentId, description="Continent this country belongs to"),
    "is_global_south": ColumnSpec(description="Whether OpenAlex classifies the country as Global South"),
    "works_count":     ColumnSpec(description="Number of works affiliated with institutions in this country"),
    "cited_by_count":  ColumnSpec(description="Total citations to works affiliated with institutions in this country"),
    "updated_date":    ColumnSpec(description="Last time OpenAlex modified this record"),
}

TABLES_DOC = {"countries": _COUNTRIES_DOC}


@register
class OpenAlexCountries(OpenAlexEntityPipeline):
    name = "openalex_countries"
    raw_dirname = "countries"
    transform_module = "lacuna_etl.datasets.openalex.countries"
    first_table = "countries"
    tables_doc = TABLES_DOC
