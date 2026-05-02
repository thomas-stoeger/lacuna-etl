"""
Transform raw OpenAlex award records into flat Polars DataFrames.

Produces two tables per batch:
  awards               - one row per award
  awards_investigators - one row per (award, investigator) with role
"""

import polars as pl

from lacuna_etl.datasets.openalex._base import OpenAlexEntityPipeline
from lacuna_etl.datasets.openalex._utils import short_id
from lacuna_etl.datasets.registry import register

_AWARDS_SCHEMA = {
    "award_id":             pl.String,
    "funder_award_id":      pl.String,
    "funder_id":            pl.String,
    "funder_name":          pl.String,
    "amount":               pl.Float64,
    "currency":             pl.String,
    "funding_type":         pl.String,
    "funder_scheme":        pl.String,
    "provenance":           pl.String,
    "start_date":           pl.String,
    "end_date":             pl.String,
    "start_year":           pl.Int64,
    "end_year":             pl.Int64,
    "funded_outputs_count": pl.Int64,
    "created_date":         pl.String,
    "updated_date":         pl.String,
}

_INVESTIGATORS_SCHEMA = {
    "award_id":  pl.String,
    "author_id": pl.String,
    "role":      pl.String,
}


def _award_row(r: dict) -> dict:
    funder = r.get("funder") or {}
    amount = r.get("amount")
    return {
        "award_id":             short_id(r.get("id")),
        "funder_award_id":      r.get("funder_award_id"),
        "funder_id":            short_id(funder.get("id")),
        "funder_name":          funder.get("display_name"),
        "amount":               float(amount) if amount is not None else None,
        "currency":             r.get("currency"),
        "funding_type":         r.get("funding_type"),
        "funder_scheme":        r.get("funder_scheme"),
        "provenance":           r.get("provenance"),
        "start_date":           r.get("start_date"),
        "end_date":             r.get("end_date"),
        "start_year":           r.get("start_year"),
        "end_year":             r.get("end_year"),
        "funded_outputs_count": r.get("funded_outputs_count"),
        "created_date":         r.get("created_date"),
        "updated_date":         r.get("updated_date"),
    }


def _investigator_rows(award_id: str, r: dict) -> list[dict]:
    rows = []
    lead = r.get("lead_investigator")
    if lead and lead.get("id"):
        rows.append({"award_id": award_id, "author_id": short_id(lead["id"]), "role": "lead"})
    co_lead = r.get("co_lead_investigator")
    if co_lead and co_lead.get("id"):
        rows.append({"award_id": award_id, "author_id": short_id(co_lead["id"]), "role": "co_lead"})
    for inv in r.get("investigators") or []:
        if inv.get("id"):
            rows.append({"award_id": award_id, "author_id": short_id(inv["id"]), "role": "investigator"})
    return rows


def transform_batch(records: list[dict]) -> dict[str, pl.DataFrame]:
    award_rows, inv_rows = [], []
    for r in records:
        award_id = short_id(r.get("id"))
        if award_id is None:
            continue
        award_rows.append(_award_row(r))
        inv_rows.extend(_investigator_rows(award_id, r))
    return {
        "awards":               pl.DataFrame(award_rows, schema=_AWARDS_SCHEMA),
        "awards_investigators": pl.DataFrame(inv_rows,   schema=_INVESTIGATORS_SCHEMA),
    }


@register
class OpenAlexAwards(OpenAlexEntityPipeline):
    name = "openalex_awards"
    raw_dirname = "awards"
    transform_module = "lacuna_etl.datasets.openalex.awards"
    first_table = "awards"
