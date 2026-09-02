"""
Transform raw OpenAlex funder records into flat Polars DataFrames.

Produces one table per batch:
  funders - one row per funder
"""

import polars as pl

from lacuna_etl.core.identifiers import CountryCode, CrossrefFunderDoi, OpenAlexFunderId, RorId
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.openalex._base import OpenAlexEntityPipeline
from lacuna_etl.datasets.openalex._utils import short_id
from lacuna_etl.datasets.registry import register

_FUNDERS_SCHEMA = {
    "funder_id":          pl.String,
    "display_name":       pl.String,
    "ror":                pl.String,
    "crossref_funder_doi": pl.String,
    "country_code":       pl.String,
    "description":        pl.String,
    "homepage_url":       pl.String,
    "works_count":        pl.Int64,
    "cited_by_count":     pl.Int64,
    "awards_count":       pl.Int64,
    "h_index":            pl.Int64,
    "i10_index":          pl.Int64,
    "2yr_mean_citedness": pl.Float64,
    "created_date":       pl.String,
    "updated_date":       pl.String,
}


def transform_batch(records: list[dict]) -> dict[str, pl.DataFrame]:
    rows = []
    for r in records:
        funder_id = short_id(r.get("id"))
        if funder_id is None:
            continue
        stats = r.get("summary_stats") or {}
        ids = r.get("ids") or {}
        rows.append({
            "funder_id":          funder_id,
            "display_name":       r.get("display_name"),
            "ror":                ids.get("ror"),
            "crossref_funder_doi": ids.get("doi"),
            "country_code":       r.get("country_code"),
            "description":        r.get("description"),
            "homepage_url":       r.get("homepage_url"),
            "works_count":        r.get("works_count"),
            "cited_by_count":     r.get("cited_by_count"),
            "awards_count":       r.get("awards_count"),
            "h_index":            stats.get("h_index"),
            "i10_index":          stats.get("i10_index"),
            "2yr_mean_citedness": stats.get("2yr_mean_citedness"),
            "created_date":       r.get("created_date"),
            "updated_date":       r.get("updated_date"),
        })
    return {"funders": pl.DataFrame(rows, schema=_FUNDERS_SCHEMA)}


_FUNDERS_DOC = {
    "funder_id":          ColumnSpec(identifier=OpenAlexFunderId,    required=True, description="OpenAlex funder identifier"),
    "display_name":       ColumnSpec(description="Human-readable funder name"),
    "ror":                ColumnSpec(identifier=RorId, description="Research Organization Registry URL; join the ror dataset. Sparse — ~55% of funders carry one"),
    "crossref_funder_doi": ColumnSpec(identifier=CrossrefFunderDoi, description="Crossref Funder Registry DOI (all on the 10.13039 registrant); identifies the organization, not an article, so it does not join a work's doi"),
    "country_code":       ColumnSpec(identifier=CountryCode, description="Funder country (ISO alpha-2)"),
    "description":        ColumnSpec(description="Free-text description of the funder"),
    "homepage_url":       ColumnSpec(description="Funder's homepage URL"),
    "works_count":        ColumnSpec(description="Number of works funded"),
    "cited_by_count":     ColumnSpec(description="Total citations received by funded works"),
    "awards_count":       ColumnSpec(description="Number of awards from this funder"),
    "h_index":            ColumnSpec(description="h-index of works funded"),
    "i10_index":          ColumnSpec(description="i10-index of works funded"),
    "2yr_mean_citedness": ColumnSpec(description="Mean citedness of works funded in the past 2 years"),
    "created_date":       ColumnSpec(description="When OpenAlex created this record"),
    "updated_date":       ColumnSpec(description="Last time OpenAlex modified this record"),
}

TABLES_DOC = {"funders": _FUNDERS_DOC}


@register
class OpenAlexFunders(OpenAlexEntityPipeline):
    name = "openalex_funders"
    raw_dirname = "funders"
    transform_module = "lacuna_etl.datasets.openalex.funders"
    first_table = "funders"
    tables_doc = TABLES_DOC
