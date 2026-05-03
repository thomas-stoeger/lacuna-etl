"""
Transform raw OpenAlex author records into flat Polars DataFrames.

Produces three tables per batch:
  authors              - one row per author, scalar fields
  authors_affiliations - one row per (author, institution) pair with years list
  authors_topics       - one row per (author, topic) pair
"""

import polars as pl

from lacuna_etl.core.identifiers import AuthorId, InstitutionId, Orcid, TopicId
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.openalex._base import OpenAlexEntityPipeline
from lacuna_etl.datasets.openalex._utils import short_id, short_orcid
from lacuna_etl.datasets.registry import register

_AUTHORS_SCHEMA = {
    "author_id":            pl.String,
    "display_name":         pl.String,
    "orcid":                pl.String,
    "works_count":          pl.Int64,
    "cited_by_count":       pl.Int64,
    "h_index":              pl.Int64,
    "i10_index":            pl.Int64,
    "2yr_mean_citedness":   pl.Float64,
    "created_date":         pl.String,
    "updated_date":         pl.String,
}

_AFFILIATIONS_SCHEMA = {
    "author_id":      pl.String,
    "institution_id": pl.String,
    "years":          pl.List(pl.Int64),
}

_AUTHOR_TOPICS_SCHEMA = {
    "author_id": pl.String,
    "topic_id":  pl.String,
    "count":     pl.Int64,
}


def _author_row(r: dict) -> dict:
    stats = r.get("summary_stats") or {}
    return {
        "author_id":            short_id(r.get("id")),
        "display_name":         r.get("display_name"),
        "orcid":                short_orcid(r.get("orcid")),
        "works_count":          r.get("works_count"),
        "cited_by_count":       r.get("cited_by_count"),
        "h_index":              stats.get("h_index"),
        "i10_index":            stats.get("i10_index"),
        "2yr_mean_citedness":   stats.get("2yr_mean_citedness"),
        "created_date":         r.get("created_date"),
        "updated_date":         r.get("updated_date"),
    }


def _affiliation_rows(author_id: str, r: dict) -> list[dict]:
    rows = []
    for aff in r.get("affiliations") or []:
        inst = aff.get("institution") or {}
        inst_id = short_id(inst.get("id"))
        if inst_id:
            rows.append({
                "author_id":      author_id,
                "institution_id": inst_id,
                "years":          aff.get("years") or [],
            })
    return rows


def _topic_rows(author_id: str, r: dict) -> list[dict]:
    rows = []
    for t in r.get("topics") or []:
        topic_id = short_id(t.get("id"))
        if topic_id:
            rows.append({
                "author_id": author_id,
                "topic_id":  topic_id,
                "count":     t.get("count"),
            })
    return rows


def transform_batch(records: list[dict]) -> dict[str, pl.DataFrame]:
    author_rows, aff_rows, topic_rows = [], [], []
    for r in records:
        author_id = short_id(r.get("id"))
        if author_id is None:
            continue
        author_rows.append(_author_row(r))
        aff_rows.extend(_affiliation_rows(author_id, r))
        topic_rows.extend(_topic_rows(author_id, r))
    return {
        "authors":              pl.DataFrame(author_rows, schema=_AUTHORS_SCHEMA),
        "authors_affiliations": pl.DataFrame(aff_rows,    schema=_AFFILIATIONS_SCHEMA),
        "authors_topics":       pl.DataFrame(topic_rows,  schema=_AUTHOR_TOPICS_SCHEMA),
    }


_AUTHORS_DOC = {
    "author_id":          ColumnSpec(identifier=AuthorId, required=True, description="OpenAlex author identifier"),
    "display_name":       ColumnSpec(description="Author name as it appears in OpenAlex"),
    "orcid":              ColumnSpec(identifier=Orcid, description="ORCID iD, with the URL prefix stripped"),
    "works_count":        ColumnSpec(description="Number of works attributed to this author"),
    "cited_by_count":     ColumnSpec(description="Total citations received by this author's works"),
    "h_index":            ColumnSpec(description="Author h-index"),
    "i10_index":          ColumnSpec(description="Author i10-index"),
    "2yr_mean_citedness": ColumnSpec(description="Mean citedness of works in the past 2 years"),
    "created_date":       ColumnSpec(description="When OpenAlex created this record"),
    "updated_date":       ColumnSpec(description="Last time OpenAlex modified this record"),
}

_AFFILIATIONS_DOC = {
    "author_id":      ColumnSpec(identifier=AuthorId,      required=True, description="Author whose affiliation is being described"),
    "institution_id": ColumnSpec(identifier=InstitutionId, required=True, description="Institution the author was affiliated with"),
    "years":          ColumnSpec(description="Years (list of int) during which the affiliation held"),
}

_AUTHOR_TOPICS_DOC = {
    "author_id": ColumnSpec(identifier=AuthorId, required=True, description="Author the topic count is attributed to"),
    "topic_id":  ColumnSpec(identifier=TopicId,  required=True, description="Topic with non-zero presence in the author's works"),
    "count":     ColumnSpec(description="Number of the author's works on this topic"),
}

TABLES_DOC = {
    "authors":              _AUTHORS_DOC,
    "authors_affiliations": _AFFILIATIONS_DOC,
    "authors_topics":       _AUTHOR_TOPICS_DOC,
}


@register
class OpenAlexAuthors(OpenAlexEntityPipeline):
    name = "openalex_authors"
    raw_dirname = "authors"
    transform_module = "lacuna_etl.datasets.openalex.authors"
    first_table = "authors"
    tables_doc = TABLES_DOC
