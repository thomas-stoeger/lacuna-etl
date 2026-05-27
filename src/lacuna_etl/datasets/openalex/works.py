"""
Transform raw OpenAlex work records into flat Polars DataFrames.

Produces four tables per batch:
  works             - one row per work, scalar fields only
  works_authorships - one row per (work, author) pair
  works_topics      - one row per (work, topic) pair
  works_refs        - one row per (work, cited_work) pair

OpenAlex IDs are stored in short form, e.g. "W2741809807" not the full URL.
"""

import polars as pl

from lacuna_etl.core.identifiers import (
    OpenAlexAuthorId,
    Doi,
    OpenAlexDomainId,
    OpenAlexFieldId,
    OpenAlexSourceId,
    OpenAlexSubfieldId,
    OpenAlexTopicId,
    OpenAlexWorkId,
    PubmedId,
)
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.openalex._base import OpenAlexEntityPipeline
from lacuna_etl.datasets.openalex._utils import short_id, short_pmid
from lacuna_etl.datasets.registry import register

# Explicit schemas prevent Polars from mis-inferring nullable string columns as
# Null type when the first N rows happen to all be null.
_WORKS_SCHEMA = {
    "work_id":                           pl.String,
    "doi":                               pl.String,
    "title":                             pl.String,
    "publication_year":                  pl.Int64,
    "publication_date":                  pl.String,
    "type":                              pl.String,
    "language":                          pl.String,
    "is_retracted":                      pl.Boolean,
    "is_paratext":                       pl.Boolean,
    "authors_count":                     pl.Int64,
    "cited_by_count":                    pl.Int64,
    "referenced_works_count":            pl.Int64,
    "fwci":                              pl.Float64,
    "citation_normalized_percentile":    pl.Float64,
    "is_oa":                             pl.Boolean,
    "oa_status":                         pl.String,
    "primary_source_id":                 pl.String,
    "primary_source_type":               pl.String,
    "primary_location_is_oa":            pl.Boolean,
    "primary_topic_id":                  pl.String,
    "primary_subfield_id":               pl.String,
    "primary_field_id":                  pl.String,
    "primary_domain_id":                 pl.String,
    "volume":                            pl.String,
    "issue":                             pl.String,
    "first_page":                        pl.String,
    "last_page":                         pl.String,
    "pmid":                              pl.Int64,
    "pmcid":                             pl.String,
    "created_date":                      pl.String,
    "updated_date":                      pl.String,
}

_AUTHORSHIPS_SCHEMA = {
    "work_id":          pl.String,
    "author_id":        pl.String,
    "author_position":  pl.String,
    "is_corresponding": pl.Boolean,
    "institution_ids":  pl.List(pl.String),
    "countries":        pl.List(pl.String),
}

_TOPICS_SCHEMA = {
    "work_id":      pl.String,
    "topic_id":     pl.String,
    "score":        pl.Float64,
    "subfield_id":  pl.String,
    "field_id":     pl.String,
    "domain_id":    pl.String,
}

_REFS_SCHEMA = {
    "work_id":      pl.String,
    "ref_work_id":  pl.String,
}


def _extract_pmid(ids: dict) -> int | None:
    return short_pmid(ids.get("pmid"))


def _works_row(r: dict) -> dict:
    oa = r.get("open_access") or {}
    biblio = r.get("biblio") or {}
    pl_loc = r.get("primary_location") or {}
    pl_source = pl_loc.get("source") or {}
    pt = r.get("primary_topic") or {}
    pt_subfield = pt.get("subfield") or {}
    pt_field = pt.get("field") or {}
    pt_domain = pt.get("domain") or {}
    cnpy = r.get("citation_normalized_percentile") or {}
    ids = r.get("ids") or {}

    return {
        "work_id":              short_id(r.get("id")),
        "doi":                  Doi.shorten(r.get("doi")),
        "title":                r.get("title"),
        "publication_year":     r.get("publication_year"),
        "publication_date":     r.get("publication_date"),
        "type":                 r.get("type"),
        "language":             r.get("language"),
        "is_retracted":         r.get("is_retracted"),
        "is_paratext":          r.get("is_paratext"),
        "authors_count":        r.get("authors_count"),
        "cited_by_count":       r.get("cited_by_count"),
        "referenced_works_count": r.get("referenced_works_count"),
        "fwci":                 r.get("fwci"),
        "citation_normalized_percentile": cnpy.get("value"),
        "is_oa":                oa.get("is_oa"),
        "oa_status":            oa.get("oa_status"),
        "primary_source_id":    short_id(pl_source.get("id")),
        "primary_source_type":  pl_source.get("type"),
        "primary_location_is_oa": pl_loc.get("is_oa"),
        "primary_topic_id":     short_id(pt.get("id")),
        "primary_subfield_id":  short_id(pt_subfield.get("id")),
        "primary_field_id":     short_id(pt_field.get("id")),
        "primary_domain_id":    short_id(pt_domain.get("id")),
        "volume":               biblio.get("volume"),
        "issue":                biblio.get("issue"),
        "first_page":           biblio.get("first_page"),
        "last_page":            biblio.get("last_page"),
        "pmid":                 _extract_pmid(ids),
        "pmcid":                ids.get("pmcid"),
        "created_date":         r.get("created_date"),
        "updated_date":         r.get("updated_date"),
    }


def _authorship_rows(work_id: str, r: dict) -> list[dict]:
    rows = []
    for auth in r.get("authorships") or []:
        author = auth.get("author") or {}
        author_id = short_id(author.get("id"))
        institution_ids = [
            short_id(inst.get("id"))
            for inst in (auth.get("institutions") or [])
            if inst.get("id")
        ]
        rows.append({
            "work_id":          work_id,
            "author_id":        author_id,
            "author_position":  auth.get("author_position"),
            "is_corresponding": auth.get("is_corresponding"),
            "institution_ids":  institution_ids,
            "countries":        auth.get("countries") or [],
        })
    return rows


def _topic_rows(work_id: str, r: dict) -> list[dict]:
    rows = []
    for t in r.get("topics") or []:
        rows.append({
            "work_id":      work_id,
            "topic_id":     short_id(t.get("id")),
            "score":        t.get("score"),
            "subfield_id":  short_id((t.get("subfield") or {}).get("id")),
            "field_id":     short_id((t.get("field") or {}).get("id")),
            "domain_id":    short_id((t.get("domain") or {}).get("id")),
        })
    return rows


def _ref_rows(work_id: str, r: dict) -> list[dict]:
    return [
        {"work_id": work_id, "ref_work_id": short_id(ref)}
        for ref in (r.get("referenced_works") or [])
    ]


def transform_batch(records: list[dict]) -> dict[str, pl.DataFrame]:
    works_rows, auth_rows, topic_rows, ref_rows = [], [], [], []
    for r in records:
        work_id = short_id(r.get("id"))
        if work_id is None:
            continue
        works_rows.append(_works_row(r))
        auth_rows.extend(_authorship_rows(work_id, r))
        topic_rows.extend(_topic_rows(work_id, r))
        ref_rows.extend(_ref_rows(work_id, r))
    return {
        "works":             pl.DataFrame(works_rows, schema=_WORKS_SCHEMA),
        "works_authorships": pl.DataFrame(auth_rows,  schema=_AUTHORSHIPS_SCHEMA),
        "works_topics":      pl.DataFrame(topic_rows, schema=_TOPICS_SCHEMA),
        "works_refs":        pl.DataFrame(ref_rows,   schema=_REFS_SCHEMA),
    }


def verify_batch(
    tables: dict[str, pl.DataFrame],
    gz_path: str,
    batch_idx: int = 0,
) -> None:
    from lacuna_etl.datasets.openalex._verify import verify_works_batch
    verify_works_batch(tables, gz_path, batch_idx)


_WORKS_DOC = {
    "work_id":                        ColumnSpec(identifier=OpenAlexWorkId,     required=True, description="OpenAlex work identifier"),
    "doi":                            ColumnSpec(identifier=Doi,        description="DOI with the URL prefix stripped"),
    "title":                          ColumnSpec(description="Work title"),
    "publication_year":               ColumnSpec(description="Publication year"),
    "publication_date":               ColumnSpec(description="Publication date (ISO 8601)"),
    "type":                           ColumnSpec(description="Work type (article, dataset, dissertation, ...)"),
    "language":                       ColumnSpec(description="ISO 639-1 language code of the work"),
    "is_retracted":                   ColumnSpec(description="Whether the work has been retracted"),
    "is_paratext":                    ColumnSpec(description="Whether OpenAlex flags the work as paratext (front-matter, indices, etc.)"),
    "authors_count":                  ColumnSpec(description="Number of authors on this work"),
    "cited_by_count":                 ColumnSpec(description="Number of works that cite this one"),
    "referenced_works_count":         ColumnSpec(description="Number of works this one references"),
    "fwci":                           ColumnSpec(description="Field-Weighted Citation Impact"),
    "citation_normalized_percentile": ColumnSpec(description="Citation percentile normalized within the work's field/year"),
    "is_oa":                          ColumnSpec(description="Whether the work is open-access at any location"),
    "oa_status":                      ColumnSpec(description="Open-access status (closed, gold, hybrid, green, bronze, diamond)"),
    "primary_source_id":              ColumnSpec(identifier=OpenAlexSourceId,   description="Primary publication source"),
    "primary_source_type":            ColumnSpec(description="Type of the primary source (journal, repository, etc.)"),
    "primary_location_is_oa":         ColumnSpec(description="Whether the primary location is open-access"),
    "primary_topic_id":               ColumnSpec(identifier=OpenAlexTopicId,    description="Primary topic assigned by OpenAlex"),
    "primary_subfield_id":            ColumnSpec(identifier=OpenAlexSubfieldId, description="Subfield of the primary topic"),
    "primary_field_id":               ColumnSpec(identifier=OpenAlexFieldId,    description="Field of the primary topic"),
    "primary_domain_id":              ColumnSpec(identifier=OpenAlexDomainId,   description="Domain of the primary topic"),
    "volume":                         ColumnSpec(description="Bibliographic volume"),
    "issue":                          ColumnSpec(description="Bibliographic issue"),
    "first_page":                     ColumnSpec(description="First page (string; may be non-numeric)"),
    "last_page":                      ColumnSpec(description="Last page (string; may be non-numeric)"),
    "pmid":                           ColumnSpec(identifier=PubmedId, description="PubMed identifier as carried by OpenAlex; sparse — for fuller coverage join the pmid_openalex crosswalk"),
    "pmcid":                          ColumnSpec(description="PubMed Central ID"),
    "created_date":                   ColumnSpec(description="When OpenAlex created this record"),
    "updated_date":                   ColumnSpec(description="Last time OpenAlex modified this record"),
}

_AUTHORSHIPS_DOC = {
    "work_id":          ColumnSpec(identifier=OpenAlexWorkId,   required=True, description="Work the authorship belongs to"),
    "author_id":        ColumnSpec(identifier=OpenAlexAuthorId, description="Author (may be null for anonymous / unmatched authorships)"),
    "author_position":  ColumnSpec(allowed_values={"first", "middle", "last"}, description="Position of the author in the byline"),
    "is_corresponding": ColumnSpec(description="Whether this author is marked as corresponding"),
    "institution_ids":  ColumnSpec(description="OpenAlex institution IDs the author was affiliated with for this work"),
    "countries":        ColumnSpec(description="ISO alpha-2 country codes the authorship is associated with"),
}

_TOPICS_DOC = {
    "work_id":     ColumnSpec(identifier=OpenAlexWorkId,     required=True, description="Work the topic is assigned to"),
    "topic_id":    ColumnSpec(identifier=OpenAlexTopicId,    description="Topic assigned to the work"),
    "score":       ColumnSpec(description="OpenAlex confidence score for the topic assignment"),
    "subfield_id": ColumnSpec(identifier=OpenAlexSubfieldId, description="Subfield of the topic"),
    "field_id":    ColumnSpec(identifier=OpenAlexFieldId,    description="Field of the topic"),
    "domain_id":   ColumnSpec(identifier=OpenAlexDomainId,   description="Domain of the topic"),
}

_REFS_DOC = {
    "work_id":     ColumnSpec(identifier=OpenAlexWorkId, required=True, description="Citing work"),
    "ref_work_id": ColumnSpec(identifier=OpenAlexWorkId, required=True, description="Cited work"),
}

TABLES_DOC = {
    "works":             _WORKS_DOC,
    "works_authorships": _AUTHORSHIPS_DOC,
    "works_topics":      _TOPICS_DOC,
    "works_refs":        _REFS_DOC,
}


@register
class OpenAlexWorks(OpenAlexEntityPipeline):
    name = "openalex_works"
    raw_dirname = "works"
    transform_module = "lacuna_etl.datasets.openalex.works"
    first_table = "works"
    tables_doc = TABLES_DOC
