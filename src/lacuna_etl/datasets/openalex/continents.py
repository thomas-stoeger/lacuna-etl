"""continents + continents_countries (tiny lookup tables)."""

import polars as pl

from lacuna_etl.core.identifiers import ContinentId, CountryId, WikidataId
from lacuna_etl.core.schema import ColumnSpec
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


_CONTINENTS_DOC = {
    "continent_id": ColumnSpec(identifier=ContinentId, required=True, description="OpenAlex continent identifier"),
    "display_name": ColumnSpec(description="Human-readable continent name"),
    "wikidata_id":  ColumnSpec(identifier=WikidataId, description="Wikidata Q-identifier for this continent"),
    "description":  ColumnSpec(description="Free-text description of the continent"),
    "updated_date": ColumnSpec(description="Last time OpenAlex modified this record"),
}

_CONTINENTS_COUNTRIES_DOC = {
    "continent_id": ColumnSpec(identifier=ContinentId, required=True, description="Continent the country belongs to"),
    "country_id":   ColumnSpec(identifier=CountryId,   required=True, description="Country located on this continent"),
}

TABLES_DOC = {"continents": _CONTINENTS_DOC, "continents_countries": _CONTINENTS_COUNTRIES_DOC}


@register
class OpenAlexContinents(OpenAlexEntityPipeline):
    name = "openalex_continents"
    raw_dirname = "continents"
    transform_module = "lacuna_etl.datasets.openalex.continents"
    first_table = "continents"
    tables_doc = TABLES_DOC
