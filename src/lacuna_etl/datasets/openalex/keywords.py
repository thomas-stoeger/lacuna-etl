"""keywords lookup table."""

import polars as pl

from lacuna_etl.datasets.openalex._base import OpenAlexEntityPipeline
from lacuna_etl.datasets.openalex._utils import short_id
from lacuna_etl.datasets.registry import register

_KEYWORDS_SCHEMA = {
    "keyword_id":     pl.String,
    "display_name":   pl.String,
    "works_count":    pl.Int64,
    "cited_by_count": pl.Int64,
    "updated_date":   pl.String,
}


def transform_batch(records: list[dict]) -> dict[str, pl.DataFrame]:
    rows = []
    for r in records:
        kw_id = short_id(r.get("id"))
        if kw_id is None:
            continue
        rows.append({
            "keyword_id":     kw_id,
            "display_name":   r.get("display_name"),
            "works_count":    r.get("works_count"),
            "cited_by_count": r.get("cited_by_count"),
            "updated_date":   r.get("updated_date"),
        })
    return {"keywords": pl.DataFrame(rows, schema=_KEYWORDS_SCHEMA)}


@register
class OpenAlexKeywords(OpenAlexEntityPipeline):
    name = "openalex_keywords"
    raw_dirname = "keywords"
    transform_module = "lacuna_etl.datasets.openalex.keywords"
    first_table = "keywords"
