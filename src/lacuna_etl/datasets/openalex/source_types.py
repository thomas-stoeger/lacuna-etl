"""source-types lookup table."""

import polars as pl

from lacuna_etl.core.identifiers import OpenAlexSourceTypeId
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.openalex._base import OpenAlexEntityPipeline
from lacuna_etl.datasets.openalex._utils import short_id
from lacuna_etl.datasets.registry import register

_SOURCE_TYPES_SCHEMA = {
    "source_type_id": pl.String,
    "display_name":   pl.String,
    "works_count":    pl.Int64,
    "cited_by_count": pl.Int64,
    "updated_date":   pl.String,
}


def transform_batch(records: list[dict]) -> dict[str, pl.DataFrame]:
    rows = []
    for r in records:
        type_id = short_id(r.get("id"))
        if type_id is None:
            continue
        rows.append({
            "source_type_id": type_id,
            "display_name":   r.get("display_name"),
            "works_count":    r.get("works_count"),
            "cited_by_count": r.get("cited_by_count"),
            "updated_date":   r.get("updated_date"),
        })
    return {"source_types": pl.DataFrame(rows, schema=_SOURCE_TYPES_SCHEMA)}


_SOURCE_TYPES_DOC = {
    "source_type_id": ColumnSpec(identifier=OpenAlexSourceTypeId, required=True, description="OpenAlex source-type identifier (journal, repository, etc.)"),
    "display_name":   ColumnSpec(description="Human-readable type name"),
    "works_count":    ColumnSpec(description="Number of works hosted by sources of this type"),
    "cited_by_count": ColumnSpec(description="Total citations to works hosted by sources of this type"),
    "updated_date":   ColumnSpec(description="Last time OpenAlex modified this record"),
}

TABLES_DOC = {"source_types": _SOURCE_TYPES_DOC}


@register
class OpenAlexSourceTypes(OpenAlexEntityPipeline):
    name = "openalex_source_types"
    raw_dirname = "source-types"
    transform_module = "lacuna_etl.datasets.openalex.source_types"
    first_table = "source_types"
    tables_doc = TABLES_DOC
