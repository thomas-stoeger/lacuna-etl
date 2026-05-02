"""languages lookup table."""

import polars as pl

from lacuna_etl.datasets.openalex._base import OpenAlexEntityPipeline
from lacuna_etl.datasets.openalex._utils import short_id
from lacuna_etl.datasets.registry import register

_LANGUAGES_SCHEMA = {
    "language_id":    pl.String,
    "display_name":   pl.String,
    "works_count":    pl.Int64,
    "cited_by_count": pl.Int64,
    "updated_date":   pl.String,
}


def transform_batch(records: list[dict]) -> dict[str, pl.DataFrame]:
    rows = []
    for r in records:
        lang_id = short_id(r.get("id"))
        if lang_id is None:
            continue
        rows.append({
            "language_id":    lang_id,
            "display_name":   r.get("display_name"),
            "works_count":    r.get("works_count"),
            "cited_by_count": r.get("cited_by_count"),
            "updated_date":   r.get("updated_date"),
        })
    return {"languages": pl.DataFrame(rows, schema=_LANGUAGES_SCHEMA)}


@register
class OpenAlexLanguages(OpenAlexEntityPipeline):
    name = "openalex_languages"
    raw_dirname = "languages"
    transform_module = "lacuna_etl.datasets.openalex.languages"
    first_table = "languages"
