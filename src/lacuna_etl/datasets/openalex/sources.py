"""
Transform raw OpenAlex source records into flat Polars DataFrames.

Produces two tables per batch:
  sources        - one row per source (journal, repository, etc.)
  sources_topics - one row per (source, topic) pair
"""

import polars as pl

from lacuna_etl.core.identifiers import IssnL, OpenAlexSourceId, OpenAlexTopicId
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.openalex._base import OpenAlexEntityPipeline
from lacuna_etl.datasets.openalex._utils import short_id
from lacuna_etl.datasets.registry import register

_SOURCES_SCHEMA = {
    "source_id":              pl.String,
    "issn_l":                 pl.String,
    "display_name":           pl.String,
    "type":                   pl.String,
    "country_code":           pl.String,
    "host_organization_id":   pl.String,
    "host_organization_name": pl.String,
    "is_oa":                  pl.Boolean,
    "is_in_doaj":             pl.Boolean,
    "is_core":                pl.Boolean,
    "apc_usd":                pl.Int64,
    "works_count":            pl.Int64,
    "oa_works_count":         pl.Int64,
    "cited_by_count":         pl.Int64,
    "h_index":                pl.Int64,
    "i10_index":              pl.Int64,
    "2yr_mean_citedness":     pl.Float64,
    "first_publication_year": pl.Int64,
    "last_publication_year":  pl.Int64,
    "created_date":           pl.String,
    "updated_date":           pl.String,
}

_SOURCE_TOPICS_SCHEMA = {
    "source_id": pl.String,
    "topic_id":  pl.String,
    "count":     pl.Int64,
}


def transform_batch(records: list[dict]) -> dict[str, pl.DataFrame]:
    src_rows, topic_rows = [], []
    for r in records:
        src_id = short_id(r.get("id"))
        if src_id is None:
            continue
        stats = r.get("summary_stats") or {}
        src_rows.append({
            "source_id":              src_id,
            "issn_l":                 IssnL.normalize(r.get("issn_l")),
            "display_name":           r.get("display_name"),
            "type":                   r.get("type"),
            "country_code":           r.get("country_code"),
            "host_organization_id":   short_id(r.get("host_organization")),
            "host_organization_name": r.get("host_organization_name"),
            "is_oa":                  r.get("is_oa"),
            "is_in_doaj":             r.get("is_in_doaj"),
            "is_core":                r.get("is_core"),
            "apc_usd":                r.get("apc_usd"),
            "works_count":            r.get("works_count"),
            "oa_works_count":         r.get("oa_works_count"),
            "cited_by_count":         r.get("cited_by_count"),
            "h_index":                stats.get("h_index"),
            "i10_index":              stats.get("i10_index"),
            "2yr_mean_citedness":     stats.get("2yr_mean_citedness"),
            "first_publication_year": r.get("first_publication_year"),
            "last_publication_year":  r.get("last_publication_year"),
            "created_date":           r.get("created_date"),
            "updated_date":           r.get("updated_date"),
        })
        for t in r.get("topics") or []:
            topic_id = short_id(t.get("id"))
            if topic_id:
                topic_rows.append({
                    "source_id": src_id,
                    "topic_id":  topic_id,
                    "count":     t.get("count"),
                })
    return {
        "sources":        pl.DataFrame(src_rows,   schema=_SOURCES_SCHEMA),
        "sources_topics": pl.DataFrame(topic_rows, schema=_SOURCE_TOPICS_SCHEMA),
    }


_SOURCES_DOC = {
    "source_id":              ColumnSpec(identifier=OpenAlexSourceId,    required=True, description="OpenAlex source identifier (journal, repository, conference, etc.)"),
    "issn_l":                 ColumnSpec(identifier=IssnL,       description="Linking ISSN, if assigned"),
    "display_name":           ColumnSpec(description="Source name"),
    "type":                   ColumnSpec(description="Source type (journal / repository / ebook platform / book series / conference / other)"),
    "country_code":           ColumnSpec(description="Country in which the source is published (typically ISO alpha-2; OpenAlex also emits some non-ISO 3-letter codes here)"),
    "host_organization_id":   ColumnSpec(description="OpenAlex ID of the host organization (publisher or institution)"),
    "host_organization_name": ColumnSpec(description="Display name of the host organization"),
    "is_oa":                  ColumnSpec(description="Whether the source is fully open-access"),
    "is_in_doaj":             ColumnSpec(description="Whether the source is listed in the Directory of Open Access Journals"),
    "is_core":                ColumnSpec(description="Whether OpenAlex flags the source as core (fully indexed) vs auxiliary"),
    "apc_usd":                ColumnSpec(description="Article processing charge in USD, if any"),
    "works_count":            ColumnSpec(description="Number of works hosted at this source"),
    "oa_works_count":         ColumnSpec(description="Number of open-access works hosted at this source"),
    "cited_by_count":         ColumnSpec(description="Total citations received by works of this source"),
    "h_index":                ColumnSpec(description="h-index of works hosted at this source"),
    "i10_index":              ColumnSpec(description="i10-index of works hosted at this source"),
    "2yr_mean_citedness":     ColumnSpec(description="Mean citedness of works in the past 2 years"),
    "first_publication_year": ColumnSpec(description="Year of the source's earliest indexed publication"),
    "last_publication_year":  ColumnSpec(description="Year of the source's most recent indexed publication"),
    "created_date":           ColumnSpec(description="When OpenAlex created this record"),
    "updated_date":           ColumnSpec(description="Last time OpenAlex modified this record"),
}

_SOURCES_TOPICS_DOC = {
    "source_id": ColumnSpec(identifier=OpenAlexSourceId, required=True, description="Source the topic count is attributed to"),
    "topic_id":  ColumnSpec(identifier=OpenAlexTopicId,  required=True, description="Topic with non-zero presence in this source"),
    "count":     ColumnSpec(description="Number of works on this topic at this source"),
}

TABLES_DOC = {"sources": _SOURCES_DOC, "sources_topics": _SOURCES_TOPICS_DOC}


@register
class OpenAlexSources(OpenAlexEntityPipeline):
    name = "openalex_sources"
    raw_dirname = "sources"
    transform_module = "lacuna_etl.datasets.openalex.sources"
    first_table = "sources"
    tables_doc = TABLES_DOC
