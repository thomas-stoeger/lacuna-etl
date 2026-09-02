"""Retraction Watch list of hijacked (cloned) journals.

A hijacked journal is a fraudulent website impersonating a legitimate journal.
Each source row pairs the fraudulent ("hijacked") title/URL/ISSNs with the
legitimate ("original") journal's title/URL/ISSNs.

The source CSV is awkward: the first record is a two-line donation banner, so
the real header is the *second* CSV record (read with ``header=1``). Its first
column is an unnamed row counter, the last column name carries a stray leading
space, and both ISSN columns hold one or two comma-separated ISSNs.

Two tables:
- ``hijacked_journals`` — one row per record, the titles and URLs.
- ``hijacked_journal_issns`` — one row per (record, role, issn); the ISSN
  columns are split on commas and normalized to canonical ISSN form, with
  ``role`` distinguishing the hijacked clone from the original journal.
"""
from __future__ import annotations

import pandas as pd

from lacuna_etl.core.identifiers import IssnL
from lacuna_etl.core.pipeline import DatasetPipeline
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.registry import register

# Source header (after stripping whitespace from the column names) -> our names.
_RENAME = {
    "Hijacked Journal Title": "hijacked_title",
    "URL (Hijacked)": "hijacked_url",
    "ISSN (Hijacked)": "_issn_hijacked",
    "Original journal": "original_title",
    "ISSN (Original)": "_issn_original",
    "URL (Original Journal)": "original_url",
}
_EXPECTED_COLS = set(_RENAME)
_ROLES = {"hijacked", "original"}

JOURNALS_SCHEMA = {
    "record_id": ColumnSpec(description="Row identifier within the hijacked-journal list", required=True),
    "hijacked_title": ColumnSpec(description="Title used by the fraudulent (hijacked) journal", required=True),
    "hijacked_url": ColumnSpec(description="URL of the fraudulent (hijacked) clone website"),
    "original_title": ColumnSpec(description="Title of the legitimate journal being impersonated"),
    "original_url": ColumnSpec(description="URL of the legitimate journal's website"),
}
ISSNS_SCHEMA = {
    "record_id": ColumnSpec(description="Row identifier within the hijacked-journal list", required=True),
    "role": ColumnSpec(allowed_values=_ROLES, description="Whether the ISSN was listed for the 'hijacked' clone or the 'original' journal", required=True),
    "issn": ColumnSpec(identifier=IssnL, description="ISSN in canonical hyphenated form", required=True),
}


@register
class RetractionWatchHijackedJournals(DatasetPipeline):
    name = "retractionwatch_hijackedjournals"
    _TABLES = [
        ("hijacked_journals", JOURNALS_SCHEMA),
        ("hijacked_journal_issns", ISSNS_SCHEMA),
    ]

    def extract(self) -> None:
        # header=1: the first CSV record is a (two-physical-line) banner; the
        # real header is the second record.
        df = pd.read_csv(self.raw_path() / "hijacked_journals.csv", header=1, dtype=str)
        df.columns = [c.strip() for c in df.columns]
        # First column is an unnamed row counter; keep it as the record id.
        df = df.rename(columns={df.columns[0]: "record_id"})
        unexpected = set(df.columns) - _EXPECTED_COLS - {"record_id"}
        if unexpected:
            raise ValueError(f"{self.name}: unexpected source columns {unexpected}")
        self.save_parquet(df, self.intermediate_path() / "raw.parquet")

    def transform(self) -> None:
        df = self.load_parquet(self.intermediate_path() / "raw.parquet")
        df = df.rename(columns=_RENAME)
        df["record_id"] = pd.to_numeric(df["record_id"], errors="raise").astype("Int64")
        if df["record_id"].isna().any() or df["record_id"].duplicated().any():
            raise ValueError(f"{self.name}: record_id must be a unique non-null integer")

        for col in ("hijacked_title", "hijacked_url", "original_title", "original_url"):
            s = df[col].astype("string").str.strip()
            df[col] = s.where(s.notna() & (s != ""), pd.NA)

        journals = df[["record_id", "hijacked_title", "hijacked_url", "original_title", "original_url"]]
        journals = journals[journals["hijacked_title"].notna()].reset_index(drop=True)
        self.save_parquet(journals, self.intermediate_path() / "hijacked_journals.parquet")

        issns = pd.concat(
            [
                self._explode_issns(df, "_issn_hijacked", "hijacked"),
                self._explode_issns(df, "_issn_original", "original"),
            ],
            ignore_index=True,
        )
        issns = issns.sort_values(["record_id", "role", "issn"]).reset_index(drop=True)
        self.save_parquet(issns, self.intermediate_path() / "hijacked_journal_issns.parquet")

    @staticmethod
    def _explode_issns(df: pd.DataFrame, src_col: str, role: str) -> pd.DataFrame:
        tmp = pd.DataFrame({
            "record_id": df["record_id"].to_numpy(),
            "issn": df[src_col].astype("string").fillna("").str.split(",").to_numpy(),
        })
        tmp = tmp.explode("issn")
        tmp["issn"] = tmp["issn"].map(IssnL.normalize).astype("string")
        tmp = tmp[tmp["issn"].notna()]
        tmp["record_id"] = tmp["record_id"].astype("Int64")
        tmp["role"] = role
        return tmp[["record_id", "role", "issn"]].drop_duplicates()

    def load(self) -> None:
        for table, schema in self._TABLES:
            df = self.load_parquet(self.intermediate_path() / f"{table}.parquet")
            df = self.apply_schema(df, schema)
            df = df[list(schema)]
            self.save_parquet(df, self.output_path() / f"{table}.parquet")
            self.save_schema_yaml(schema, table)
