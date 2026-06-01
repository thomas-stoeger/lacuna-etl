"""Sustainable Development Goals lookup table."""

import polars as pl

from lacuna_etl.core.identifiers import OpenAlexSdgId
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.openalex._base import OpenAlexEntityPipeline
from lacuna_etl.datasets.openalex._utils import short_id
from lacuna_etl.datasets.registry import register

_SDGS_SCHEMA = {
    "sdg_id":         pl.String,
    "display_name":   pl.String,
    "description":    pl.String,
    "works_count":    pl.Int64,
    "cited_by_count": pl.Int64,
    "updated_date":   pl.String,
}


def transform_batch(records: list[dict]) -> dict[str, pl.DataFrame]:
    rows = []
    for r in records:
        sdg_id = short_id(r.get("id"))
        if sdg_id is None:
            continue
        rows.append({
            "sdg_id":         sdg_id,
            "display_name":   r.get("display_name"),
            "description":    r.get("description"),
            "works_count":    r.get("works_count"),
            "cited_by_count": r.get("cited_by_count"),
            "updated_date":   r.get("updated_date"),
        })
    return {"sdgs": pl.DataFrame(rows, schema=_SDGS_SCHEMA)}


_SDGS_DOC = {
    "sdg_id":         ColumnSpec(identifier=OpenAlexSdgId, required=True, description="UN Sustainable Development Goal identifier"),
    "display_name":   ColumnSpec(description="Human-readable goal name"),
    "description":    ColumnSpec(description="Free-text description of the goal"),
    "works_count":    ColumnSpec(description="Number of works tagged to this SDG"),
    "cited_by_count": ColumnSpec(description="Total citations to works tagged to this SDG"),
    "updated_date":   ColumnSpec(description="Last time OpenAlex modified this record"),
}

TABLES_DOC = {"sdgs": _SDGS_DOC}


@register
class OpenAlexSdgs(OpenAlexEntityPipeline):
    name = "openalex_sdgs"
    raw_dirname = "sdgs"
    transform_module = "lacuna_etl.datasets.openalex.sdgs"
    first_table = "sdgs"
    tables_doc = TABLES_DOC
