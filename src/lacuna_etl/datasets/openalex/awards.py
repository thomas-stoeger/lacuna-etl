"""
Transform raw OpenAlex award records into flat Polars DataFrames.

Produces three tables per batch:
  awards                - one row per award
  awards_investigators  - one row per (award, investigator) with role
  awards_funded_outputs - one row per (award, funded work) pair
"""

import polars as pl

from lacuna_etl.core.identifiers import Orcid, OpenAlexAwardId, OpenAlexFunderId, OpenAlexWorkId
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.openalex._base import OpenAlexEntityPipeline
from lacuna_etl.datasets.openalex._utils import short_id, short_orcid
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

# OpenAlex identifies award investigators by name, not by author ID: the nested
# objects carry given/family name, an often-null ORCID and a free-text
# affiliation, with no `id` field to resolve against openalex_authors.
_INVESTIGATORS_SCHEMA = {
    "award_id":              pl.String,
    "role":                  pl.String,
    "given_name":            pl.String,
    "family_name":           pl.String,
    "orcid":                 pl.String,
    "role_start":            pl.String,
    "affiliation_name":      pl.String,
    "affiliation_country":   pl.String,
}

_FUNDED_OUTPUTS_SCHEMA = {
    "award_id": pl.String,
    "work_id":  pl.String,
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


def _clean(value) -> str | None:
    """Trim a source string, mapping empty/whitespace-only to null."""
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def _investigator_row(award_id: str, inv: dict, role: str) -> dict:
    affiliation = inv.get("affiliation") or {}
    return {
        "award_id":            award_id,
        "role":                role,
        "given_name":          _clean(inv.get("given_name")),
        "family_name":         _clean(inv.get("family_name")),
        "orcid":               short_orcid(inv.get("orcid")),
        "role_start":          _clean(inv.get("role_start")),
        "affiliation_name":    _clean(affiliation.get("name")),
        "affiliation_country": _clean(affiliation.get("country")),
    }


def _investigator_rows(award_id: str, r: dict) -> list[dict]:
    rows = []
    for key, role in (("lead_investigator", "lead"), ("co_lead_investigator", "co_lead")):
        inv = r.get(key)
        if inv:
            rows.append(_investigator_row(award_id, inv, role))
    for inv in r.get("investigators") or []:
        rows.append(_investigator_row(award_id, inv, "investigator"))
    return rows


def _funded_output_rows(award_id: str, r: dict) -> list[dict]:
    seen = set()
    rows = []
    for work in r.get("funded_outputs") or []:
        work_id = short_id(work)
        if work_id is None or work_id in seen:
            continue
        seen.add(work_id)
        rows.append({"award_id": award_id, "work_id": work_id})
    return rows


def transform_batch(records: list[dict]) -> dict[str, pl.DataFrame]:
    award_rows, inv_rows, output_rows = [], [], []
    for r in records:
        award_id = short_id(r.get("id"))
        if award_id is None:
            continue
        award_rows.append(_award_row(r))
        inv_rows.extend(_investigator_rows(award_id, r))
        output_rows.extend(_funded_output_rows(award_id, r))
    return {
        "awards":                pl.DataFrame(award_rows,  schema=_AWARDS_SCHEMA),
        "awards_investigators":  pl.DataFrame(inv_rows,    schema=_INVESTIGATORS_SCHEMA),
        "awards_funded_outputs": pl.DataFrame(output_rows, schema=_FUNDED_OUTPUTS_SCHEMA),
    }


_AWARDS_DOC = {
    "award_id":             ColumnSpec(identifier=OpenAlexAwardId,  required=True, description="OpenAlex award identifier"),
    "funder_award_id":      ColumnSpec(description="Funder-issued award identifier (e.g. NIH grant number)"),
    "funder_id":            ColumnSpec(identifier=OpenAlexFunderId, description="Funder that issued the award"),
    "funder_name":          ColumnSpec(description="Display name of the funder"),
    "amount":               ColumnSpec(description="Award amount (numeric)"),
    "currency":             ColumnSpec(description="ISO currency code for the award amount"),
    "funding_type":         ColumnSpec(description="OpenAlex-classified funding type"),
    "funder_scheme":        ColumnSpec(description="Funder-specific scheme or program name"),
    "provenance":           ColumnSpec(description="Source from which OpenAlex ingested this award"),
    "start_date":           ColumnSpec(description="Award start date"),
    "end_date":             ColumnSpec(description="Award end date"),
    "start_year":           ColumnSpec(description="Award start year"),
    "end_year":             ColumnSpec(description="Award end year"),
    "funded_outputs_count": ColumnSpec(description="Number of works linked to this award"),
    "created_date":         ColumnSpec(description="When OpenAlex created this record"),
    "updated_date":         ColumnSpec(description="Last time OpenAlex modified this record"),
}

_INVESTIGATORS_DOC = {
    "award_id":            ColumnSpec(identifier=OpenAlexAwardId, required=True, description="Award the investigator is associated with"),
    "role":                ColumnSpec(allowed_values={"lead", "co_lead", "investigator"}, description="Investigator role"),
    "given_name":          ColumnSpec(description="Investigator given name as asserted by the funder"),
    "family_name":         ColumnSpec(description="Investigator family name as asserted by the funder"),
    "orcid":               ColumnSpec(identifier=Orcid, description="Investigator ORCID; sparse — most funders supply names only, and OpenAlex asserts no author ID here"),
    "role_start":          ColumnSpec(description="Date the investigator took the role, when the funder reports one"),
    "affiliation_name":    ColumnSpec(description="Free-text affiliation as asserted by the funder (not resolved to an OpenAlex institution)"),
    "affiliation_country": ColumnSpec(description="Free-text affiliation country as asserted by the funder (a country name, not an ISO code)"),
}

_FUNDED_OUTPUTS_DOC = {
    "award_id": ColumnSpec(identifier=OpenAlexAwardId, required=True, description="Award that funded the work"),
    "work_id":  ColumnSpec(identifier=OpenAlexWorkId,  required=True, description="Work attributed to the award; join openalex_works.works"),
}

TABLES_DOC = {
    "awards":                _AWARDS_DOC,
    "awards_investigators":  _INVESTIGATORS_DOC,
    "awards_funded_outputs": _FUNDED_OUTPUTS_DOC,
}


@register
class OpenAlexAwards(OpenAlexEntityPipeline):
    name = "openalex_awards"
    raw_dirname = "awards"
    transform_module = "lacuna_etl.datasets.openalex.awards"
    first_table = "awards"
    tables_doc = TABLES_DOC
