"""institution-types lookup table."""

import polars as pl

from lacuna_etl.datasets.openalex._base import OpenAlexEntityPipeline
from lacuna_etl.datasets.openalex._utils import short_id
from lacuna_etl.datasets.registry import register

_INSTITUTION_TYPES_SCHEMA = {
    "institution_type_id": pl.String,
    "display_name":        pl.String,
    "works_count":         pl.Int64,
    "cited_by_count":      pl.Int64,
    "updated_date":        pl.String,
}


def transform_batch(records: list[dict]) -> dict[str, pl.DataFrame]:
    rows = []
    for r in records:
        type_id = short_id(r.get("id"))
        if type_id is None:
            continue
        rows.append({
            "institution_type_id": type_id,
            "display_name":        r.get("display_name"),
            "works_count":         r.get("works_count"),
            "cited_by_count":      r.get("cited_by_count"),
            "updated_date":        r.get("updated_date"),
        })
    return {"institution_types": pl.DataFrame(rows, schema=_INSTITUTION_TYPES_SCHEMA)}


@register
class OpenAlexInstitutionTypes(OpenAlexEntityPipeline):
    name = "openalex_institution_types"
    raw_dirname = "institution-types"
    transform_module = "lacuna_etl.datasets.openalex.institution_types"
    first_table = "institution_types"
