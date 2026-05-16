"""subfields lookup table."""

import polars as pl

from lacuna_etl.core.identifiers import DomainId, FieldId, SubfieldId
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.openalex._base import OpenAlexEntityPipeline
from lacuna_etl.datasets.openalex._utils import short_id
from lacuna_etl.datasets.registry import register

_SUBFIELDS_SCHEMA = {
    "subfield_id":    pl.String,
    "display_name":   pl.String,
    "description":    pl.String,
    "field_id":       pl.String,
    "domain_id":      pl.String,
    "works_count":    pl.Int64,
    "cited_by_count": pl.Int64,
    "updated_date":   pl.String,
}


def transform_batch(records: list[dict]) -> dict[str, pl.DataFrame]:
    rows = []
    for r in records:
        subfield_id = short_id(r.get("id"))
        if subfield_id is None:
            continue
        field = r.get("field") or {}
        domain = r.get("domain") or {}
        rows.append({
            "subfield_id":    subfield_id,
            "display_name":   r.get("display_name"),
            "description":    r.get("description"),
            "field_id":       short_id(field.get("id")),
            "domain_id":      short_id(domain.get("id")),
            "works_count":    r.get("works_count"),
            "cited_by_count": r.get("cited_by_count"),
            "updated_date":   r.get("updated_date"),
        })
    return {"subfields": pl.DataFrame(rows, schema=_SUBFIELDS_SCHEMA)}


_SUBFIELDS_DOC = {
    "subfield_id":    ColumnSpec(identifier=SubfieldId, required=True, description="OpenAlex subfield identifier (third tier of the topic hierarchy)"),
    "display_name":   ColumnSpec(description="Human-readable subfield name"),
    "description":    ColumnSpec(description="Free-text description of the subfield"),
    "field_id":       ColumnSpec(identifier=FieldId, description="Parent field"),
    "domain_id":      ColumnSpec(identifier=DomainId, description="Parent domain"),
    "works_count":    ColumnSpec(description="Number of works tagged to this subfield"),
    "cited_by_count": ColumnSpec(description="Total citations received by works in this subfield"),
    "updated_date":   ColumnSpec(description="Last time OpenAlex modified this record"),
}

TABLES_DOC = {"subfields": _SUBFIELDS_DOC}


@register
class OpenAlexSubfields(OpenAlexEntityPipeline):
    name = "openalex_subfields"
    raw_dirname = "subfields"
    transform_module = "lacuna_etl.datasets.openalex.subfields"
    first_table = "subfields"
    tables_doc = TABLES_DOC
