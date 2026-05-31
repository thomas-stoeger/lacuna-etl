"""Retraction Watch database (Crossref-distributed export).

One parent table ``retractions`` (one row per Retraction Watch ``Record ID``)
plus child tables that explode the source's ``;``-delimited multi-valued fields,
each keyed by ``record_id``: ``retraction_reasons``, ``retraction_subjects``,
``retraction_authors``, ``retraction_countries``, ``retraction_institutions``
and ``retraction_urls``.

Source quirks handled here (see the dataset README):
- A trailing comma in the header adds an empty column; we select the 20 documented
  columns explicitly via ``usecols``.
- PubMed IDs use ``0`` or blank to mean "missing"; both become null.
- DOIs use ``unavailable`` / ``Unavailable`` / blank as "missing", and carry the
  occasional embedded whitespace (``10. 1126/...``) or leading junk
  (``<pmid> 10.1093/...``); these are repaired to canonical form, and anything
  still non-conforming becomes null rather than propagating a bad identifier.
- Multi-valued fields carry a trailing empty token from a trailing ``;``; those
  are dropped. Scalar fields (``ArticleType``, ``RetractionNature``) likewise
  carry a stray trailing ``;`` which is stripped.
"""
from __future__ import annotations

import re

import pandas as pd

from lacuna_etl.core.identifiers import (
    Doi,
    DoiVersioned,
    PubmedId,
    RetractionWatchId,
    split_doi_version_pandas,
)
from lacuna_etl.core.pipeline import DatasetPipeline
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.registry import register

_RECORD_ID = ColumnSpec(identifier=RetractionWatchId, description="Retraction Watch internal record identifier", required=True)

RETRACTIONS_SCHEMA = {
    "record_id": _RECORD_ID,
    "title": ColumnSpec(description="Title of the retracted or corrected work"),
    "journal": ColumnSpec(description="Journal in which the work appeared"),
    "publisher": ColumnSpec(description="Publisher of the journal"),
    "article_type": ColumnSpec(description="Retraction Watch article-type classification"),
    "retraction_nature": ColumnSpec(description="Nature of the notice (Retraction, Correction, Expression of concern, Reinstatement, or a ';'-joined combination)"),
    "paywalled": ColumnSpec(description="Whether the retraction notice is behind a paywall"),
    "retraction_date": ColumnSpec(description="Date the retraction/notice was issued"),
    "retraction_doi": ColumnSpec(identifier=Doi, description="DOI of the retraction notice (article-level; any publisher version suffix is split into retraction_doi_versioned), if any"),
    "retraction_doi_versioned": ColumnSpec(identifier=DoiVersioned, description="Original versioned DOI of the retraction notice when the publisher appends an article version (e.g. F1000 '.N'); null otherwise"),
    "retraction_pmid": ColumnSpec(identifier=PubmedId, description="PubMed ID of the retraction notice, if any"),
    "original_paper_date": ColumnSpec(description="Publication date of the original work"),
    "original_paper_doi": ColumnSpec(identifier=Doi, description="DOI of the original work (article-level; any publisher version suffix is split into original_paper_doi_versioned), if any"),
    "original_paper_doi_versioned": ColumnSpec(identifier=DoiVersioned, description="Original versioned DOI of the original work when the publisher appends an article version (e.g. F1000 '.N'); null otherwise"),
    "original_paper_pmid": ColumnSpec(identifier=PubmedId, description="PubMed ID of the original work, if any"),
    "notes": ColumnSpec(description="Free-text notes from Retraction Watch"),
}

REASONS_SCHEMA = {
    "record_id": _RECORD_ID,
    "reason": ColumnSpec(description="A single retraction reason (any leading '+' removed)", required=True),
}
SUBJECTS_SCHEMA = {
    "record_id": _RECORD_ID,
    "subject": ColumnSpec(description="A single subject-area classification", required=True),
}
AUTHORS_SCHEMA = {
    "record_id": _RECORD_ID,
    "author": ColumnSpec(description="A single author name as given by the source", required=True),
}
COUNTRIES_SCHEMA = {
    "record_id": _RECORD_ID,
    "country": ColumnSpec(description="A single country from the author affiliations", required=True),
}
INSTITUTIONS_SCHEMA = {
    "record_id": _RECORD_ID,
    "institution": ColumnSpec(description="A single institution/affiliation as given by the source", required=True),
}
URLS_SCHEMA = {
    "record_id": _RECORD_ID,
    "url": ColumnSpec(description="A URL linking to the retraction notice or related Retraction Watch page", required=True),
}

# The 20 documented source columns (the header has a trailing comma that adds an
# unnamed empty 21st column, which usecols ignores). usecols raises if any of
# these is missing, which is the schema-drift guard we want.
_SCALAR_COLS = [
    "Record ID", "Title", "Journal", "Publisher", "ArticleType", "RetractionNature",
    "Paywalled", "RetractionDate", "RetractionDOI", "RetractionPubMedID",
    "OriginalPaperDate", "OriginalPaperDOI", "OriginalPaperPubMedID", "Notes",
]
_MULTI_COLS = ["Subject", "Institution", "Country", "Author", "URLS", "Reason"]
_USECOLS = _SCALAR_COLS + _MULTI_COLS

_DOI_EXTRACT = re.compile(r"10\s*\.\s*\d+\s*/\s*\S+")
_DOI_CANONICAL = re.compile(r"^10\.[^/\s]+/\S+$")
# 'Unknown' is a third source value alongside Yes/No; it carries no boolean
# information, so it maps to null rather than aborting the run.
_PAYWALLED_MAP = {"Yes": True, "No": False, "Unknown": pd.NA}


def _clean_doi(value: object) -> str | None:
    """Repair a Retraction Watch DOI to canonical 10.x/... form, or None.

    Drops the 'unavailable' placeholder, extracts the DOI from any leading junk,
    and removes whitespace embedded in malformed prefixes. Anything that does not
    end up matching the canonical DOI shape becomes None.
    """
    if not isinstance(value, str):  # None / NaN / pd.NA
        return None
    s = value.strip()
    if not s or s.lower() == "unavailable":
        return None
    m = _DOI_EXTRACT.search(s)
    if not m:
        return None
    doi = re.sub(r"\s+", "", m.group(0))
    return doi if _DOI_CANONICAL.match(doi) else None


def _to_date(s: pd.Series) -> pd.Series:
    """Parse the source 'M/D/YYYY ...' timestamps to a date.

    The time portion is two different formats in the source (24-hour for most
    rows, 12-hour with an AM/PM suffix for a few hundred), and we only need the
    date, so we capture just the leading ``M/D/YYYY`` and parse that. A non-blank
    value that does not start with a date raises rather than being silently
    dropped.
    """
    cleaned = s.astype("string").str.strip()
    cleaned = cleaned.where(cleaned.notna() & (cleaned != ""), pd.NA)
    date_part = cleaned.str.extract(r"^(\d{1,2}/\d{1,2}/\d{4})\b", expand=False)
    bad = cleaned.notna() & date_part.isna()
    if bad.any():
        raise ValueError(f"{_to_date.__name__}: unparseable date(s) e.g. {cleaned[bad].head(5).tolist()}")
    return pd.to_datetime(date_part, format="%m/%d/%Y", errors="raise")


def _to_pmid(s: pd.Series) -> pd.Series:
    """Cast a PubMed ID column to Int64, mapping the source's 0/blank to null."""
    n = pd.to_numeric(s, errors="raise").astype("Int64")
    n = n.mask(n == 0)
    if (n.dropna() <= 0).any():
        raise ValueError("retraction_watch: non-positive PubMed ID after dropping the 0 sentinel")
    return n


def _clean_text(s: pd.Series) -> pd.Series:
    s = s.astype("string").str.strip()
    return s.where(s.notna() & (s != ""), pd.NA)


def _clean_scalar(s: pd.Series) -> pd.Series:
    """Strip a scalar field and drop its stray trailing ';'."""
    s = s.astype("string").str.strip().str.replace(r";+\s*$", "", regex=True).str.strip()
    return s.where(s.notna() & (s != ""), pd.NA)


@register
class RetractionWatchRetractionDatabase(DatasetPipeline):
    name = "retractionwatch_retractiondatabase"

    def extract(self) -> None:
        df = pd.read_csv(self.raw_path() / "retraction_watch.csv", dtype=str, usecols=_USECOLS)
        self.save_parquet(df, self.intermediate_path() / "raw.parquet")

    def transform(self) -> None:
        df = self.load_parquet(self.intermediate_path() / "raw.parquet")

        record_id = pd.to_numeric(df["Record ID"], errors="raise").astype("Int64")
        # A small fraction of source rows (~0.4%) are real retractions that carry
        # no Record ID. Record ID is this table's primary key and the join key for
        # every child table, so a row without one cannot be keyed in; following the
        # PubTator3 precedent, those rows are excluded and the count reported rather
        # than dropped silently or given a fabricated id.
        missing = record_id.isna()
        if missing.any():
            print(f"[{self.name}] excluding {int(missing.sum()):,} source rows with no Record ID")
            df = df[~missing].reset_index(drop=True)
            record_id = record_id[~missing].reset_index(drop=True)
        if record_id.duplicated().any():
            dups = record_id[record_id.duplicated()].unique().tolist()
            raise ValueError(f"{self.name}: Record ID is not unique, e.g. {dups[:10]}")

        parent = pd.DataFrame({"record_id": record_id})
        parent["title"] = _clean_text(df["Title"])
        parent["journal"] = _clean_text(df["Journal"])
        parent["publisher"] = _clean_text(df["Publisher"])
        parent["article_type"] = _clean_scalar(df["ArticleType"])
        parent["retraction_nature"] = _clean_scalar(df["RetractionNature"])

        paywalled = df["Paywalled"].astype("string").str.strip()
        paywalled = paywalled.where(paywalled.notna() & (paywalled != ""), pd.NA)
        unexpected = set(paywalled.dropna().unique()) - set(_PAYWALLED_MAP)
        if unexpected:
            raise ValueError(f"{self.name}: unexpected Paywalled values {unexpected}")
        parent["paywalled"] = paywalled.map(_PAYWALLED_MAP).astype("boolean")

        parent["retraction_date"] = _to_date(df["RetractionDate"])
        # Split publisher-versioned DOIs (F1000 '.N', Research Square '/vN', ...):
        # the base DOI stays article-level, the versioned form moves to a sibling.
        parent["retraction_doi"], parent["retraction_doi_versioned"] = split_doi_version_pandas(
            Doi.cast(df["RetractionDOI"].map(_clean_doi).astype("string"))
        )
        parent["retraction_pmid"] = _to_pmid(df["RetractionPubMedID"])
        parent["original_paper_date"] = _to_date(df["OriginalPaperDate"])
        parent["original_paper_doi"], parent["original_paper_doi_versioned"] = split_doi_version_pandas(
            Doi.cast(df["OriginalPaperDOI"].map(_clean_doi).astype("string"))
        )
        parent["original_paper_pmid"] = _to_pmid(df["OriginalPaperPubMedID"])
        parent["notes"] = _clean_text(df["Notes"])

        RetractionWatchId.validate(parent["record_id"])
        Doi.validate(parent["retraction_doi"])
        Doi.validate(parent["original_paper_doi"])
        DoiVersioned.validate(parent["retraction_doi_versioned"])
        DoiVersioned.validate(parent["original_paper_doi_versioned"])
        for col in ("retraction_pmid", "original_paper_pmid"):
            self._validate_nullable_pmid(parent[col])

        parent = parent[list(RETRACTIONS_SCHEMA)]
        self.save_parquet(parent, self.intermediate_path() / "retractions.parquet")

        children = {
            "retraction_reasons": ("Reason", "reason", True),
            "retraction_subjects": ("Subject", "subject", False),
            "retraction_authors": ("Author", "author", False),
            "retraction_countries": ("Country", "country", False),
            "retraction_institutions": ("Institution", "institution", False),
            "retraction_urls": ("URLS", "url", False),
        }
        for table, (src_col, out_col, strip_plus) in children.items():
            child = self._explode(record_id, df[src_col], out_col, strip_plus=strip_plus)
            self.save_parquet(child, self.intermediate_path() / f"{table}.parquet")

    @staticmethod
    def _validate_nullable_pmid(s: pd.Series) -> None:
        if not pd.api.types.is_integer_dtype(s):
            raise TypeError(f"{s.name}: expected integer dtype, got {s.dtype}")
        if (s.dropna() <= 0).any():
            raise ValueError(f"{s.name}: non-positive PubMed ID")

    @staticmethod
    def _explode(record_id: pd.Series, values: pd.Series, out_col: str, *, strip_plus: bool) -> pd.DataFrame:
        # Build positionally (both length N) so source row i keeps its record id.
        tmp = pd.DataFrame({
            "record_id": record_id.to_numpy(),
            out_col: values.astype("string").fillna("").str.split(";").to_numpy(),
        })
        tmp = tmp.explode(out_col)
        vals = tmp[out_col].astype("string").str.strip()
        if strip_plus:
            vals = vals.str.removeprefix("+").str.strip()
        tmp[out_col] = vals
        tmp = tmp[tmp[out_col].notna() & (tmp[out_col] != "")]
        tmp["record_id"] = tmp["record_id"].astype("Int64")
        return tmp.drop_duplicates().sort_values(["record_id", out_col]).reset_index(drop=True)

    def load(self) -> None:
        tables = {
            "retractions": RETRACTIONS_SCHEMA,
            "retraction_reasons": REASONS_SCHEMA,
            "retraction_subjects": SUBJECTS_SCHEMA,
            "retraction_authors": AUTHORS_SCHEMA,
            "retraction_countries": COUNTRIES_SCHEMA,
            "retraction_institutions": INSTITUTIONS_SCHEMA,
            "retraction_urls": URLS_SCHEMA,
        }
        for table, schema in tables.items():
            df = self.load_parquet(self.intermediate_path() / f"{table}.parquet")
            df = df[list(schema)]
            self.save_parquet(df, self.output_path() / f"{table}.parquet")
            self.save_schema_yaml(schema, table)
