"""continents + continents_countries (tiny lookup tables)."""

import polars as pl

from lacuna_etl.datasets.openalex._base import OpenAlexEntityPipeline
from lacuna_etl.datasets.openalex._utils import short_id
from lacuna_etl.datasets.registry import register

_CONTINENTS_SCHEMA = {
    "continent_id": pl.String,
    "display_name": pl.String,
    "wikidata_id":  pl.String,
    "description":  pl.String,
    "updated_date": pl.String,
}

_COUNTRIES_SCHEMA = {
    "continent_id": pl.String,
    "country_id":   pl.String,
}


def transform_batch(records: list[dict]) -> dict[str, pl.DataFrame]:
    cont_rows, country_rows = [], []
    for r in records:
        cont_id = short_id(r.get("id"))
        if cont_id is None:
            continue
        cont_rows.append({
            "continent_id": cont_id,
            "display_name": r.get("display_name"),
            "wikidata_id":  r.get("wikidata_id"),
            "description":  r.get("description"),
            "updated_date": r.get("updated_date"),
        })
        for c in r.get("countries") or []:
            country_id = short_id(c.get("id"))
            if country_id:
                country_rows.append({"continent_id": cont_id, "country_id": country_id})
    return {
        "continents":           pl.DataFrame(cont_rows,    schema=_CONTINENTS_SCHEMA),
        "continents_countries": pl.DataFrame(country_rows, schema=_COUNTRIES_SCHEMA),
    }


@register
class OpenAlexContinents(OpenAlexEntityPipeline):
    name = "openalex_continents"
    raw_dirname = "continents"
    transform_module = "lacuna_etl.datasets.openalex.continents"
    first_table = "continents"
