"""topics lookup table + topics_keywords bridge."""

import polars as pl

from lacuna_etl.core.identifiers import OpenAlexDomainId, OpenAlexFieldId, OpenAlexSubfieldId, OpenAlexTopicId
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.openalex._base import OpenAlexEntityPipeline
from lacuna_etl.datasets.openalex._utils import short_id
from lacuna_etl.datasets.registry import register

_TOPICS_SCHEMA = {
    "topic_id":       pl.String,
    "display_name":   pl.String,
    "description":    pl.String,
    "subfield_id":    pl.String,
    "field_id":       pl.String,
    "domain_id":      pl.String,
    "works_count":    pl.Int64,
    "cited_by_count": pl.Int64,
    "updated_date":   pl.String,
}

_KEYWORDS_SCHEMA = {
    "topic_id": pl.String,
    "keyword":  pl.String,
}


def transform_batch(records: list[dict]) -> dict[str, pl.DataFrame]:
    topic_rows, kw_rows = [], []
    for r in records:
        topic_id = short_id(r.get("id"))
        if topic_id is None:
            continue
        subfield = r.get("subfield") or {}
        field = r.get("field") or {}
        domain = r.get("domain") or {}
        topic_rows.append({
            "topic_id":       topic_id,
            "display_name":   r.get("display_name"),
            "description":    r.get("description"),
            "subfield_id":    short_id(subfield.get("id")),
            "field_id":       short_id(field.get("id")),
            "domain_id":      short_id(domain.get("id")),
            "works_count":    r.get("works_count"),
            "cited_by_count": r.get("cited_by_count"),
            "updated_date":   r.get("updated_date"),
        })
        for kw in r.get("keywords") or []:
            if kw:
                kw_rows.append({"topic_id": topic_id, "keyword": kw})
    return {
        "topics":          pl.DataFrame(topic_rows, schema=_TOPICS_SCHEMA),
        "topics_keywords": pl.DataFrame(kw_rows,    schema=_KEYWORDS_SCHEMA),
    }


_TOPICS_DOC = {
    "topic_id":       ColumnSpec(identifier=OpenAlexTopicId,    required=True, description="OpenAlex topic identifier (leaf of the topic hierarchy)"),
    "display_name":   ColumnSpec(description="Human-readable topic name"),
    "description":    ColumnSpec(description="Free-text description of the topic"),
    "subfield_id":    ColumnSpec(identifier=OpenAlexSubfieldId, description="Parent subfield"),
    "field_id":       ColumnSpec(identifier=OpenAlexFieldId,    description="Ancestor field"),
    "domain_id":      ColumnSpec(identifier=OpenAlexDomainId,   description="Ancestor domain"),
    "works_count":    ColumnSpec(description="Number of works tagged to this topic"),
    "cited_by_count": ColumnSpec(description="Total citations received by works in this topic"),
    "updated_date":   ColumnSpec(description="Last time OpenAlex modified this record"),
}

_KEYWORDS_DOC = {
    "topic_id": ColumnSpec(identifier=OpenAlexTopicId, required=True, description="Topic the keyword belongs to"),
    "keyword":  ColumnSpec(required=True, description="Free-text keyword associated with the topic"),
}

TABLES_DOC = {"topics": _TOPICS_DOC, "topics_keywords": _KEYWORDS_DOC}


@register
class OpenAlexTopics(OpenAlexEntityPipeline):
    name = "openalex_topics"
    raw_dirname = "topics"
    transform_module = "lacuna_etl.datasets.openalex.topics"
    first_table = "topics"
    tables_doc = TABLES_DOC
