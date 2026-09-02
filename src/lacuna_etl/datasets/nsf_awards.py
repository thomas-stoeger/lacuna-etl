"""NSF Awards — the National Science Foundation's funded-award records.

NSF ships one JSON document per award, bundled into ~70 zips (one per fiscal year
plus a ``Historical`` bundle; ~650k awards total). Each document carries the award's
scalars plus nested investigator, institution, program, obligation, and funding
structures. This is a restartable, per-zip pipeline: ``extract`` processes each zip
once (skipping zips already marked done), reshaping its awards into a parent table
plus child tables and writing per-zip shards; ``transform`` is a no-op; ``load``
concatenates the shards per table, types, validates, and writes.

The grain key is the award id (``NsfAwardId``, the numeric ``awd_id``), one row per
award in the parent ``awards`` table (with the awardee and performance institutions
flattened in). The repeated structures explode into child tables keyed by
``award_id``: investigators, program elements, program references, year-by-year
obligations, and application-funding lines. Amounts are ``Float64``; the NSF person
id (``nsf_id``) is a documented plain string. See docs/DESIGN.md for the table
inventory.
"""
from __future__ import annotations

import json
import zipfile
from pathlib import Path

import pandas as pd

from lacuna_etl.core.identifiers import NsfAwardId
from lacuna_etl.core.pipeline import DatasetPipeline
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.registry import register

AWARDS_SCHEMA = {
    "award_id": ColumnSpec(identifier=NsfAwardId, required=True, description="NSF award id (awd_id); the grain key"),
    "title": ColumnSpec(description="Award title (awd_titl_txt)"),
    "agency_id": ColumnSpec(description="Awarding agency id (agcy_id)"),
    "transaction_type": ColumnSpec(description="Transaction type (e.g. Grant, Cooperative Agreement)"),
    "award_instrument": ColumnSpec(description="Award instrument text (awd_istr_txt)"),
    "cfda_num": ColumnSpec(description="CFDA program number"),
    "effective_date": ColumnSpec(description="Award effective (start) date"),
    "expiration_date": ColumnSpec(description="Award expiration (end) date"),
    "min_amendment_date": ColumnSpec(description="Earliest amendment letter date"),
    "max_amendment_date": ColumnSpec(description="Latest amendment letter date"),
    "total_intended_amount": ColumnSpec(description="Total intended award amount (Float64)"),
    "award_amount": ColumnSpec(description="Awarded amount to date (Float64)"),
    "arra_amount": ColumnSpec(description="ARRA-funded amount (Float64)"),
    "abstract": ColumnSpec(description="Award abstract narration (awd_abstract_narration)"),
    "directorate_abbr": ColumnSpec(description="NSF directorate abbreviation (dir_abbr)"),
    "directorate_name": ColumnSpec(description="NSF directorate name (org_dir_long_name)"),
    "division_abbr": ColumnSpec(description="NSF division abbreviation (div_abbr)"),
    "division_name": ColumnSpec(description="NSF division name (org_div_long_name)"),
    "program_officer": ColumnSpec(description="Program officer name (po_sign_block_name)"),
    "program_officer_email": ColumnSpec(description="Program officer email (po_email)"),
    "org_code": ColumnSpec(description="NSF organization code"),
    "awd_agcy_code": ColumnSpec(description="Awarding agency code"),
    "fund_agcy_code": ColumnSpec(description="Funding agency code"),
    "inst_name": ColumnSpec(description="Awardee institution name"),
    "inst_city": ColumnSpec(description="Awardee institution city"),
    "inst_state_code": ColumnSpec(description="Awardee institution state code"),
    "inst_state_name": ColumnSpec(description="Awardee institution state name"),
    "inst_country_name": ColumnSpec(description="Awardee institution country name"),
    "inst_zip_code": ColumnSpec(description="Awardee institution ZIP code"),
    "inst_cong_dist_code": ColumnSpec(description="Awardee institution congressional district code"),
    "org_uei_num": ColumnSpec(description="Awardee organization UEI number"),
    "org_parent_uei_num": ColumnSpec(description="Awardee parent organization UEI number"),
    "perf_inst_name": ColumnSpec(description="Performance-site institution name"),
    "perf_city": ColumnSpec(description="Performance-site city"),
    "perf_state_code": ColumnSpec(description="Performance-site state code"),
    "perf_country_name": ColumnSpec(description="Performance-site country name"),
    "perf_zip_code": ColumnSpec(description="Performance-site ZIP code"),
}

AWARD_PIS_SCHEMA = {
    "award_id": ColumnSpec(identifier=NsfAwardId, required=True, description="NSF award id"),
    "nsf_id": ColumnSpec(description="NSF person-profile id of the investigator (plain string)"),
    "pi_role": ColumnSpec(description="Investigator role (e.g. Principal Investigator, Co-Principal Investigator)"),
    "pi_first_name": ColumnSpec(description="Investigator first name"),
    "pi_last_name": ColumnSpec(description="Investigator last name"),
    "pi_full_name": ColumnSpec(description="Investigator full name"),
    "pi_email": ColumnSpec(description="Investigator email (pi_email_addr)"),
    "pi_start_date": ColumnSpec(description="Investigator start date on the award"),
    "pi_end_date": ColumnSpec(description="Investigator end date on the award"),
}

AWARD_PROGRAM_ELEMENTS_SCHEMA = {
    "award_id": ColumnSpec(identifier=NsfAwardId, required=True, description="NSF award id"),
    "pgm_ele_code": ColumnSpec(description="Program element code"),
    "pgm_ele_name": ColumnSpec(description="Program element name"),
}

AWARD_PROGRAM_REFERENCES_SCHEMA = {
    "award_id": ColumnSpec(identifier=NsfAwardId, required=True, description="NSF award id"),
    "pgm_ref_code": ColumnSpec(description="Program reference code"),
    "pgm_ref_txt": ColumnSpec(description="Program reference text"),
}

AWARD_OBLIGATIONS_SCHEMA = {
    "award_id": ColumnSpec(identifier=NsfAwardId, required=True, description="NSF award id"),
    "fiscal_year": ColumnSpec(description="Fiscal year of the obligation (fund_oblg_fiscal_yr; Int64)"),
    "amount": ColumnSpec(description="Amount obligated in the fiscal year (fund_oblg_amt; Float64)"),
}

AWARD_FUNDING_SCHEMA = {
    "award_id": ColumnSpec(identifier=NsfAwardId, required=True, description="NSF award id"),
    "app_code": ColumnSpec(description="Appropriation code"),
    "app_name": ColumnSpec(description="Appropriation name"),
    "fund_code": ColumnSpec(description="Fund code"),
    "fund_name": ColumnSpec(description="Fund name"),
}


def _clean(v: object) -> str | None:
    """Normalise a string-column value: None stays None, empty/whitespace -> None,
    and a non-string scalar (some code fields are JSON numbers) is coerced to str so
    the column stays a clean string. Numeric value columns (amounts, fiscal year)
    bypass this and keep their numeric type."""
    if v is None:
        return None
    if not isinstance(v, str):
        v = str(v)
    v = v.strip()
    return v or None


@register
class NsfAwards(DatasetPipeline):
    name = "nsf_awards"

    _TABLES = [
        ("awards", AWARDS_SCHEMA),
        ("award_pis", AWARD_PIS_SCHEMA),
        ("award_program_elements", AWARD_PROGRAM_ELEMENTS_SCHEMA),
        ("award_program_references", AWARD_PROGRAM_REFERENCES_SCHEMA),
        ("award_obligations", AWARD_OBLIGATIONS_SCHEMA),
        ("award_funding", AWARD_FUNDING_SCHEMA),
    ]
    _INT_COLS = {"award_obligations": ["fiscal_year"]}
    _FLOAT_COLS = {
        "awards": ["total_intended_amount", "award_amount", "arra_amount"],
        "award_obligations": ["amount"],
    }

    def _award_rows(self, award: dict, out: dict[str, list[dict]]) -> None:
        aid = _clean(award.get("awd_id"))
        inst = award.get("inst") or {}
        perf = award.get("perf_inst") or {}
        out["awards"].append({
            "award_id": aid,
            "title": _clean(award.get("awd_titl_txt")),
            "agency_id": _clean(award.get("agcy_id")),
            "transaction_type": _clean(award.get("tran_type")),
            "award_instrument": _clean(award.get("awd_istr_txt")),
            "cfda_num": _clean(award.get("cfda_num")),
            "effective_date": _clean(award.get("awd_eff_date")),
            "expiration_date": _clean(award.get("awd_exp_date")),
            "min_amendment_date": _clean(award.get("awd_min_amd_letter_date")),
            "max_amendment_date": _clean(award.get("awd_max_amd_letter_date")),
            "total_intended_amount": award.get("tot_intn_awd_amt"),
            "award_amount": award.get("awd_amount"),
            "arra_amount": award.get("awd_arra_amount"),
            "abstract": _clean(award.get("awd_abstract_narration")),
            "directorate_abbr": _clean(award.get("dir_abbr")),
            "directorate_name": _clean(award.get("org_dir_long_name")),
            "division_abbr": _clean(award.get("div_abbr")),
            "division_name": _clean(award.get("org_div_long_name")),
            "program_officer": _clean(award.get("po_sign_block_name")),
            "program_officer_email": _clean(award.get("po_email")),
            "org_code": _clean(award.get("org_code")),
            "awd_agcy_code": _clean(award.get("awd_agcy_code")),
            "fund_agcy_code": _clean(award.get("fund_agcy_code")),
            "inst_name": _clean(inst.get("inst_name")),
            "inst_city": _clean(inst.get("inst_city_name")),
            "inst_state_code": _clean(inst.get("inst_state_code")),
            "inst_state_name": _clean(inst.get("inst_state_name")),
            "inst_country_name": _clean(inst.get("inst_country_name")),
            "inst_zip_code": _clean(inst.get("inst_zip_code")),
            "inst_cong_dist_code": _clean(inst.get("cong_dist_code")),
            "org_uei_num": _clean(inst.get("org_uei_num")),
            "org_parent_uei_num": _clean(inst.get("org_prnt_uei_num")),
            "perf_inst_name": _clean(perf.get("perf_inst_name")),
            "perf_city": _clean(perf.get("perf_city_name")),
            "perf_state_code": _clean(perf.get("perf_st_code")),
            "perf_country_name": _clean(perf.get("perf_ctry_name")),
            "perf_zip_code": _clean(perf.get("perf_zip_code")),
        })
        for pi in award.get("pi") or []:
            out["award_pis"].append({
                "award_id": aid, "nsf_id": _clean(pi.get("nsf_id")), "pi_role": _clean(pi.get("pi_role")),
                "pi_first_name": _clean(pi.get("pi_first_name")), "pi_last_name": _clean(pi.get("pi_last_name")),
                "pi_full_name": _clean(pi.get("pi_full_name")), "pi_email": _clean(pi.get("pi_email_addr")),
                "pi_start_date": _clean(pi.get("pi_start_date")), "pi_end_date": _clean(pi.get("pi_end_date")),
            })
        for pe in award.get("pgm_ele") or []:
            out["award_program_elements"].append({"award_id": aid, "pgm_ele_code": _clean(pe.get("pgm_ele_code")), "pgm_ele_name": _clean(pe.get("pgm_ele_name"))})
        for pr in award.get("pgm_ref") or []:
            out["award_program_references"].append({"award_id": aid, "pgm_ref_code": _clean(pr.get("pgm_ref_code")), "pgm_ref_txt": _clean(pr.get("pgm_ref_txt"))})
        for ob in award.get("oblg_fy") or []:
            out["award_obligations"].append({"award_id": aid, "fiscal_year": ob.get("fund_oblg_fiscal_yr"), "amount": ob.get("fund_oblg_amt")})
        for af in award.get("app_fund") or []:
            out["award_funding"].append({"award_id": aid, "app_code": _clean(af.get("app_code")), "app_name": _clean(af.get("app_name")), "fund_code": _clean(af.get("fund_code")), "fund_name": _clean(af.get("fund_name"))})

    def extract(self) -> None:
        done_dir = self.intermediate_path() / "_done"
        done_dir.mkdir(parents=True, exist_ok=True)
        for stem, _ in self._TABLES:
            (self.intermediate_path() / stem).mkdir(parents=True, exist_ok=True)

        zips = sorted(self.raw_path().glob("*.zip"))
        if not zips:
            raise FileNotFoundError(f"nsf_awards: no *.zip under {self.raw_path()}")
        for zpath in zips:
            done = done_dir / f"{zpath.stem}.done"
            if done.exists():  # restartable: skip already-processed years
                continue
            out: dict[str, list[dict]] = {stem: [] for stem, _ in self._TABLES}
            with zipfile.ZipFile(zpath) as zf:
                for member in zf.namelist():
                    if not member.endswith(".json"):
                        continue
                    with zf.open(member) as fh:
                        self._award_rows(json.load(fh), out)
            for stem, schema in self._TABLES:
                self.save_parquet(pd.DataFrame(out[stem], columns=list(schema)), self.intermediate_path() / stem / f"{zpath.stem}.parquet")
            done.touch()
            print(f"[{self.name}] {zpath.stem}: {len(out['awards']):,} awards")

    def transform(self) -> None:
        # Faithful projection; parsing happens in extract, typing/validation in load.
        pass

    def load(self) -> None:
        for stem, schema in self._TABLES:
            shards = sorted((self.intermediate_path() / stem).glob("*.parquet"))
            df = pd.concat([self.load_parquet(s) for s in shards], ignore_index=True)
            df = df[list(schema)]
            for col in self._INT_COLS.get(stem, []):
                df[col] = df[col].astype("Int64")
            for col in self._FLOAT_COLS.get(stem, []):
                df[col] = df[col].astype("Float64")
            df = self.apply_schema(df, schema)
            self.save_parquet(df, self.output_path() / f"{stem}.parquet")
            self.save_schema_yaml(schema, stem)
            print(f"[{self.name}] {stem}: {len(df):,} rows")
