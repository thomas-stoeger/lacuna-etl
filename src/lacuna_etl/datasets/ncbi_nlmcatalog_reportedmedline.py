"""NLM Catalog records for journals reported to MEDLINE.

The source is a single large XML file (``NLMCatalogRecordSet``) — one
``NLMCatalogRecord`` per serial (journal) catalogued by the U.S. National
Library of Medicine, restricted to titles that have at some point been reported
to / indexed for MEDLINE. Each record carries rich bibliographic metadata plus,
crucially, an ``IndexingSourceList`` describing *when* and *how* the journal was
covered by each indexing source (MEDLINE, OLDMEDLINE, Index Medicus, PubMed,
PMC, ...).

The dataset fits comfortably in memory (~15.5k records), so this is an
in-memory pandas pipeline: ``extract`` walks the XML once with
``iterparse`` and writes one intermediate Parquet per output table; ``transform``
normalizes identifiers, parses the free-text coverage strings into years, and
explodes them into the per-year table; ``load`` validates against each table's
``SCHEMA`` and writes final Parquet + sidecar.

Output tables (parent keyed by ``nlm_unique_id``; all children join back on it):

- ``journals`` — one row per NLM catalog record (the journal): titles, MEDLINE
  abbreviation, country/publisher, publication years, catalog dates, status.
- ``journal_issns`` — one row per (journal, issn, issn_type).
- ``journal_alternate_titles`` — one row per ``TitleAlternate``.
- ``journal_languages`` — one row per (journal, language, lang_type).
- ``journal_mesh_headings`` — one row per (journal, MeSH descriptor).
- ``journal_relations`` — one row per related-title link (``Preceding``,
  ``Succeeding``, ``SplitTo``, ``MergedTo``, ...); the engine for journal
  split/merge/continuation lineage. ``related_nlm_unique_id`` links to another
  ``journals`` row when NLM recorded one.
- ``indexing_coverage`` — one row per parsed indexing *frame*: which source, its
  treatment/status, the verbatim coverage string, and the start/end years (with
  an ``ongoing`` flag) recovered from it. Disjoint coverage (de-indexed then
  re-indexed) yields multiple rows per source.
- ``indexed_years`` — one row per (journal, source, year): the coverage frames
  exploded to the individual years a journal was indexed. Open-ended ("still
  indexed") coverage is expanded through the snapshot's year.

**The coverage strings are historically grown free text** with inconsistent
human formatting (``'v18n9, Sept, 1965-v45n5,May 1992'``,
``'spring 2014-Spring 2016'``, ``'v63n1, 2013-v64n4, 2014; v68n1, 2018-'``).
Rather than parse every volume/issue/month token, ``_parse_coverage_frames``
recovers exactly what is asked for — the years indexed: it splits disjoint
frames on ``;``, drops free-text notes ("selected citations only ..."), and
takes the first and last four-digit year of each frame as its span, with a
trailing ``-`` marking an open (ongoing) end. This is robust against the
hyphens that appear *inside* dates (``Jan.-Feb.``, ``v25n3-4``, ``1980-81``).
"""
from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from pathlib import Path

import pandas as pd

from lacuna_etl.core.identifiers import IssnL, NlmUniqueId
from lacuna_etl.core.pipeline import DatasetPipeline
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.registry import register

# --- controlled vocabularies (NLM catalog; small and stable) ----------------
_RECORD_STATUS = {"Completed", "Not-Our-Cataloging", "Withdrawn", "In-Process", "On-Order"}
_INDEXING_TREATMENT = {"Full", "Selective", "Unknown", "ReferencedIn"}
_INDEXING_STATUS = {
    "Currently-indexed",
    "Continued-by-another-title",
    "Deselected",
    "Ceased-publication",
}
_ISSN_TYPE = {"Print", "Electronic", "Undetermined"}

# Indexing sources that constitute the MEDLINE / Index Medicus indexing lineage
# (as opposed to PubMed deposit, PMC full-text archiving, or reference-work
# citations). Compared case-insensitively against the normalized source name.
_MEDLINE_INDEXING = {
    "medline",
    "oldmedline",
    "index medicus",
    "abridged index medicus",
    "index to dental literature",
    "hospital literature index",
    "international nursing index",
    "hospital and health administration index",
}

# A few obvious case variants seen in the wild; everything else is kept verbatim
# (trimmed) because the source-name set can grow between NLM releases.
_SOURCE_NAME_FIXES = {"medline": "MEDLINE", "pubmed": "PubMed"}

_YEAR = re.compile(r"(?<!\d)(1[6-9]\d{2}|20\d{2})(?!\d)")
# Free-text notes interleaved in coverage strings (not a real date span).
_COVERAGE_NOTE = re.compile(r"(?i)(citation|selectiv|abstracts only)")
_TRAILING_DASH = re.compile(r"-\s*$")


SCHEMA: dict[str, ColumnSpec] = {
    "nlm_unique_id": ColumnSpec(identifier=NlmUniqueId, description="NLM Catalog unique identifier (NlmUniqueID); the journal's catalog key", required=True),
    "record_status": ColumnSpec(allowed_values=_RECORD_STATUS, description="NLM catalog record status"),
    "title_main": ColumnSpec(description="Main title of the serial (TitleMain), as catalogued", required=True),
    "medline_ta": ColumnSpec(description="MEDLINE/PubMed journal title abbreviation (MedlineTA)"),
    "issn_print": ColumnSpec(identifier=IssnL, description="Print ISSN (first listed), canonical hyphenated form; full set in journal_issns"),
    "issn_electronic": ColumnSpec(identifier=IssnL, description="Electronic ISSN (first listed), canonical hyphenated form; full set in journal_issns"),
    "country": ColumnSpec(description="Country of publication (PublicationInfo/Country)"),
    "place_code": ColumnSpec(description="MARC place-of-publication code (PlaceCode)"),
    "publisher": ColumnSpec(description="Publisher / issuing entity from the original imprint (Imprint/Entity)"),
    "language_primary": ColumnSpec(description="Primary language code (Language LangType=Primary), ISO 639-2/B 3-letter"),
    "type_of_resource": ColumnSpec(description="ResourceInfo/TypeOfResource, e.g. 'Serial'"),
    "issuance": ColumnSpec(description="ResourceInfo/Issuance, e.g. 'continuing'"),
    "coden": ColumnSpec(description="CODEN serial code, if assigned"),
    "publication_first_year": ColumnSpec(description="First year of publication (PublicationFirstYear), Int64; null if not a 4-digit year"),
    "publication_end_year": ColumnSpec(description="Last year of publication (PublicationEndYear), Int64; null if ongoing (9999) or not a 4-digit year"),
    "is_ongoing_publication": ColumnSpec(description="True when PublicationEndYear is the 9999 'continuing' sentinel"),
    "currently_indexed_medline": ColumnSpec(description="True when a MEDLINE-lineage indexing source has status Currently-indexed"),
    "date_created": ColumnSpec(description="Catalog record creation date (DateCreated)"),
    "date_revised": ColumnSpec(description="Catalog record last-revision date (DateRevised)"),
    "date_authorized": ColumnSpec(description="Catalog record authorization date (DateAuthorized)"),
    "date_completed": ColumnSpec(description="Catalog record completion date (DateCompleted)"),
}

ISSNS_SCHEMA: dict[str, ColumnSpec] = {
    "nlm_unique_id": ColumnSpec(identifier=NlmUniqueId, description="Journal NLM Catalog unique identifier", required=True),
    "issn": ColumnSpec(identifier=IssnL, description="ISSN in canonical hyphenated form", required=True),
    "issn_type": ColumnSpec(allowed_values=_ISSN_TYPE, description="Medium the ISSN identifies"),
}

ALT_TITLES_SCHEMA: dict[str, ColumnSpec] = {
    "nlm_unique_id": ColumnSpec(identifier=NlmUniqueId, description="Journal NLM Catalog unique identifier", required=True),
    "title": ColumnSpec(description="Alternate title (TitleAlternate)", required=True),
    "title_type": ColumnSpec(description="TitleAlternate TitleType, e.g. 'Key', 'Abbreviated', 'Other'"),
    "owner": ColumnSpec(description="Cataloging agency that supplied the alternate title (Owner)"),
}

LANGUAGES_SCHEMA: dict[str, ColumnSpec] = {
    "nlm_unique_id": ColumnSpec(identifier=NlmUniqueId, description="Journal NLM Catalog unique identifier", required=True),
    "language": ColumnSpec(description="Language code, ISO 639-2/B 3-letter", required=True),
    "lang_type": ColumnSpec(description="Language role (Language LangType), e.g. 'Primary', 'Summary'"),
}

MESH_SCHEMA: dict[str, ColumnSpec] = {
    "nlm_unique_id": ColumnSpec(identifier=NlmUniqueId, description="Journal NLM Catalog unique identifier", required=True),
    "descriptor_name": ColumnSpec(description="MeSH descriptor describing the journal's subject scope", required=True),
    "major_topic": ColumnSpec(description="True when the descriptor is flagged a major topic (MajorTopicYN=Y)"),
}

RELATIONS_SCHEMA: dict[str, ColumnSpec] = {
    "nlm_unique_id": ColumnSpec(identifier=NlmUniqueId, description="Journal NLM Catalog unique identifier (the record stating the relation)", required=True),
    "relation_type": ColumnSpec(description="Relationship to the related title (TitleRelated TitleType): Preceding, Succeeding, MergedTo, MergerOf, SplitTo, SplitFrom, Absorbed, Supersedes, Translated, ...", required=True),
    "related_nlm_unique_id": ColumnSpec(identifier=NlmUniqueId, description="NLM Catalog unique identifier of the related journal, when NLM recorded one (joins to journals.nlm_unique_id); else null"),
    "related_title": ColumnSpec(description="Title of the related journal"),
    "related_issn": ColumnSpec(identifier=IssnL, description="ISSN of the related journal, canonical hyphenated form, when given"),
}

COVERAGE_SCHEMA: dict[str, ColumnSpec] = {
    "nlm_unique_id": ColumnSpec(identifier=NlmUniqueId, description="Journal NLM Catalog unique identifier", required=True),
    "source_index": ColumnSpec(description="0-based ordinal of the IndexingSource within the record (key part)", required=True),
    "frame_index": ColumnSpec(description="0-based ordinal of the disjoint coverage frame within this source (key part)", required=True),
    "indexing_source": ColumnSpec(description="Indexing source name (MEDLINE, OLDMEDLINE, Index medicus, PubMed, PMC, ...), trimmed; case variants of MEDLINE/PubMed normalized", required=True),
    "is_medline_indexing": ColumnSpec(description="True when the source is part of the MEDLINE / Index Medicus indexing lineage (vs PubMed deposit, PMC, or reference works)"),
    "indexing_treatment": ColumnSpec(allowed_values=_INDEXING_TREATMENT, description="Depth of indexing (IndexingTreatment)"),
    "indexing_status": ColumnSpec(allowed_values=_INDEXING_STATUS, description="Current indexing status for this source (IndexingStatus); null when not stated"),
    "coverage_raw": ColumnSpec(description="Verbatim Coverage string for this source, as catalogued (free text); null when no Coverage element"),
    "start_year": ColumnSpec(description="First year of this coverage frame (Int64); null when no year could be recovered"),
    "end_year": ColumnSpec(description="Last year of this coverage frame (Int64); null when ongoing or unparseable"),
    "ongoing": ColumnSpec(description="True when the coverage frame is open-ended (trailing '-', i.e. still indexed)"),
}

INDEXED_YEARS_SCHEMA: dict[str, ColumnSpec] = {
    "nlm_unique_id": ColumnSpec(identifier=NlmUniqueId, description="Journal NLM Catalog unique identifier", required=True),
    "indexing_source": ColumnSpec(description="Indexing source name (see indexing_coverage.indexing_source)", required=True),
    "is_medline_indexing": ColumnSpec(description="True when the source is part of the MEDLINE / Index Medicus indexing lineage"),
    "year": ColumnSpec(description="A year in which the journal was indexed by this source. Open-ended coverage is expanded through the snapshot year.", required=True),
}

_TABLES = {
    "journals": SCHEMA,
    "journal_issns": ISSNS_SCHEMA,
    "journal_alternate_titles": ALT_TITLES_SCHEMA,
    "journal_languages": LANGUAGES_SCHEMA,
    "journal_mesh_headings": MESH_SCHEMA,
    "journal_relations": RELATIONS_SCHEMA,
    "indexing_coverage": COVERAGE_SCHEMA,
    "indexed_years": INDEXED_YEARS_SCHEMA,
}


def _text(node: ET.Element | None) -> str | None:
    """Stripped text of an element, or None when absent/blank."""
    if node is None or node.text is None:
        return None
    t = node.text.strip()
    return t or None


def _year_int(raw: str | None) -> int | None:
    """A clean 4-digit year, or None (covers the 9999 / 19uu / blank sentinels)."""
    if not raw:
        return None
    raw = raw.strip()
    return int(raw) if re.fullmatch(r"\d{4}", raw) and raw != "9999" else None


def _date(node: ET.Element | None) -> pd.Timestamp:
    """Compose a Y[-M-D] catalog date; missing month/day default to 1. NaT if no year."""
    if node is None:
        return pd.NaT
    y = _text(node.find("Year"))
    if not y or not y.isdigit():
        return pd.NaT
    m = _text(node.find("Month")) or "1"
    d = _text(node.find("Day")) or "1"
    try:
        return pd.Timestamp(int(y), int(m), int(d))
    except (ValueError, TypeError):
        return pd.NaT


def _normalize_source_name(name: str | None) -> str | None:
    if not name:
        return None
    name = " ".join(name.split())
    return _SOURCE_NAME_FIXES.get(name.lower(), name)


def _parse_coverage_frames(text: str | None) -> list[tuple[int, int | None, bool]]:
    """Recover (start_year, end_year, ongoing) for each disjoint indexing frame.

    Splits the coverage string into disjoint frames on ``;``, discards free-text
    notes, and reads the first and last four-digit year of each frame as its
    span. A frame ending in an open ``-`` is flagged ongoing (end_year None).
    Robust against hyphens *inside* dates because it never splits on ``-``.
    """
    if not text:
        return []
    frames: list[tuple[int, int | None, bool]] = []
    for seg in text.split(";"):
        seg = seg.strip()
        # A note ("selected citations only before this date") may be appended to
        # a real span without a ';'. Truncate at the note rather than dropping the
        # whole segment, so the preceding span (and its open '-') is kept, while a
        # note carrying a stray year ("selected citations only before 2013")
        # collapses to nothing.
        note = _COVERAGE_NOTE.search(seg)
        if note is not None:
            seg = seg[: note.start()].strip()
        if not seg:
            continue
        years = [int(y) for y in _YEAR.findall(seg)]
        if not years:
            continue
        if _TRAILING_DASH.search(seg):
            frames.append((years[0], None, True))
        else:
            frames.append((min(years), max(years), False))
    return frames


@register
class NcbiNlmCatalogReportedMedline(DatasetPipeline):
    name = "ncbi_nlmcatalog_reportedmedline"

    def _snapshot_year(self) -> int:
        """Year of the snapshot version directory (e.g. '2026-05-29' -> 2026)."""
        return int(self.raw_path().name[:4])

    def _source_xml(self) -> Path:
        return self.raw_path() / "nlmcatalog_reportedmedline.xml"

    def extract(self) -> None:
        journals: list[dict] = []
        issns: list[dict] = []
        alt_titles: list[dict] = []
        languages: list[dict] = []
        mesh: list[dict] = []
        relations: list[dict] = []
        coverage: list[dict] = []

        ctx = ET.iterparse(self._source_xml(), events=("end",))
        for _, el in ctx:
            if el.tag != "NLMCatalogRecord":
                continue
            uid = NlmUniqueId.normalize(_text(el.find("NlmUniqueID")))
            if uid is None:
                el.clear()
                continue

            pi = el.find("PublicationInfo")
            imprint = pi.find("Imprint") if pi is not None else None
            end_raw = _text(pi.find("PublicationEndYear")) if pi is not None else None
            primary_lang = next(
                (lng for lng in el.findall("Language") if lng.get("LangType") == "Primary"),
                None,
            )
            isl = el.find("IndexingSourceList")
            medline_current = False
            if isl is not None:
                for src in isl.findall("IndexingSource"):
                    nm = src.find("IndexingSourceName")
                    name = _normalize_source_name(_text(nm))
                    if (
                        name
                        and name.lower() in _MEDLINE_INDEXING
                        and nm is not None
                        and nm.get("IndexingStatus") == "Currently-indexed"
                    ):
                        medline_current = True
                        break

            journals.append({
                "nlm_unique_id": uid,
                "record_status": el.get("Status"),
                "title_main": _text(el.find("TitleMain/Title")),
                "medline_ta": _text(el.find("MedlineTA")),
                "country": _text(pi.find("Country")) if pi is not None else None,
                "place_code": _text(pi.find("PlaceCode")) if pi is not None else None,
                "publisher": _text(imprint.find("Entity")) if imprint is not None else None,
                "language_primary": _text(primary_lang),
                "type_of_resource": _text(el.find("ResourceInfo/TypeOfResource")),
                "issuance": _text(el.find("ResourceInfo/Issuance")),
                "coden": _text(el.find("Coden")),
                "publication_first_year": _year_int(_text(pi.find("PublicationFirstYear"))) if pi is not None else None,
                "publication_end_year": _year_int(end_raw),
                "is_ongoing_publication": end_raw == "9999",
                "currently_indexed_medline": medline_current,
                "date_created": _date(el.find("DateCreated")),
                "date_revised": _date(el.find("DateRevised")),
                "date_authorized": _date(el.find("DateAuthorized")),
                "date_completed": _date(el.find("DateCompleted")),
            })

            for issn in el.findall("ISSN"):
                t = _text(issn)
                if t:
                    issns.append({"nlm_unique_id": uid, "_issn_raw": t, "issn_type": issn.get("IssnType")})

            for ta in el.findall("TitleAlternate"):
                title = _text(ta.find("Title"))
                if title:
                    alt_titles.append({
                        "nlm_unique_id": uid,
                        "title": title,
                        "title_type": ta.get("TitleType"),
                        "owner": ta.get("Owner"),
                    })

            for lng in el.findall("Language"):
                code = _text(lng)
                if code:
                    languages.append({"nlm_unique_id": uid, "language": code, "lang_type": lng.get("LangType")})

            mh = el.find("MeshHeadingList")
            if mh is not None:
                for desc in mh.iter("DescriptorName"):
                    name = _text(desc)
                    if name:
                        mesh.append({
                            "nlm_unique_id": uid,
                            "descriptor_name": name,
                            "major_topic": desc.get("MajorTopicYN") == "Y",
                        })

            for tr in el.findall("TitleRelated"):
                rid = next((r for r in tr.findall("RecordID") if r.get("Source") == "NLM"), None)
                relations.append({
                    "nlm_unique_id": uid,
                    "relation_type": tr.get("TitleType"),
                    "_related_raw": _text(rid),
                    "related_title": _text(tr.find("Title")),
                    "_related_issn_raw": _text(tr.find("ISSN")),
                })

            if isl is not None:
                for s_idx, src in enumerate(isl.findall("IndexingSource")):
                    nm = src.find("IndexingSourceName")
                    name = _normalize_source_name(_text(nm))
                    if name is None:
                        continue
                    treatment = nm.get("IndexingTreatment") if nm is not None else None
                    status = nm.get("IndexingStatus") if nm is not None else None
                    cov_raw = _text(src.find("Coverage"))
                    is_medline = name.lower() in _MEDLINE_INDEXING
                    frames = _parse_coverage_frames(cov_raw)
                    if not frames:
                        coverage.append({
                            "nlm_unique_id": uid, "source_index": s_idx, "frame_index": 0,
                            "indexing_source": name, "is_medline_indexing": is_medline,
                            "indexing_treatment": treatment, "indexing_status": status,
                            "coverage_raw": cov_raw, "start_year": None, "end_year": None, "ongoing": False,
                        })
                    else:
                        for f_idx, (start, end, ongoing) in enumerate(frames):
                            coverage.append({
                                "nlm_unique_id": uid, "source_index": s_idx, "frame_index": f_idx,
                                "indexing_source": name, "is_medline_indexing": is_medline,
                                "indexing_treatment": treatment, "indexing_status": status,
                                "coverage_raw": cov_raw, "start_year": start, "end_year": end, "ongoing": ongoing,
                            })

            el.clear()

        inter = self.intermediate_path()
        self.save_parquet(pd.DataFrame(journals), inter / "journals.parquet")
        self.save_parquet(pd.DataFrame(issns), inter / "issns.parquet")
        self.save_parquet(pd.DataFrame(alt_titles), inter / "alt_titles.parquet")
        self.save_parquet(pd.DataFrame(languages), inter / "languages.parquet")
        self.save_parquet(pd.DataFrame(mesh), inter / "mesh.parquet")
        self.save_parquet(pd.DataFrame(relations), inter / "relations.parquet")
        self.save_parquet(pd.DataFrame(coverage), inter / "coverage.parquet")
        print(f"[{self.name}] parsed {len(journals)} journals, {len(coverage)} coverage frames")

    def transform(self) -> None:
        inter = self.intermediate_path()
        snapshot_year = self._snapshot_year()

        # --- journals: dtype the year/flag/date columns, dedupe the key -------
        journals = self.load_parquet(inter / "journals.parquet")
        for col in ("publication_first_year", "publication_end_year"):
            journals[col] = journals[col].astype("Int64")
        for col in ("issn_print", "issn_electronic"):
            journals[col] = pd.NA  # filled from the per-ISSN table below
        if journals["nlm_unique_id"].duplicated().any():
            raise ValueError(f"{self.name}: duplicate nlm_unique_id in journals")

        # --- issns: normalize, attach first print/electronic to the parent ----
        issns = self.load_parquet(inter / "issns.parquet")
        issns["issn"] = issns["_issn_raw"].map(IssnL.normalize).astype("string")
        issns = issns[issns["issn"].notna()].copy()
        issns = issns[["nlm_unique_id", "issn", "issn_type"]].drop_duplicates()
        issns = issns.sort_values(["nlm_unique_id", "issn_type", "issn"]).reset_index(drop=True)

        first_by_type = (
            issns.drop_duplicates(["nlm_unique_id", "issn_type"])
            .pivot(index="nlm_unique_id", columns="issn_type", values="issn")
        )
        for medium, col in (("Print", "issn_print"), ("Electronic", "issn_electronic")):
            if medium in first_by_type.columns:
                mapping = first_by_type[medium]
                journals[col] = journals["nlm_unique_id"].map(mapping).astype("string")

        # --- relations: normalize the related NLM id and ISSN (null if dirty) -
        relations = self.load_parquet(inter / "relations.parquet")
        relations["related_nlm_unique_id"] = relations["_related_raw"].map(NlmUniqueId.normalize).astype("string")
        relations["related_issn"] = relations["_related_issn_raw"].map(IssnL.normalize).astype("string")
        relations = relations[relations["relation_type"].notna()].copy()
        relations = relations[["nlm_unique_id", "relation_type", "related_nlm_unique_id", "related_title", "related_issn"]]

        # --- coverage: type the integer key/year columns ----------------------
        coverage = self.load_parquet(inter / "coverage.parquet")
        for col in ("source_index", "frame_index", "start_year", "end_year"):
            coverage[col] = coverage[col].astype("Int64")

        # --- indexed_years: explode each frame's span into individual years ----
        year_rows: list[dict] = []
        for r in coverage.itertuples(index=False):
            if pd.isna(r.start_year):
                continue
            lo = int(r.start_year)
            hi = snapshot_year if r.ongoing else int(r.end_year)
            if hi < lo:
                hi = lo
            for year in range(lo, min(hi, snapshot_year) + 1):
                year_rows.append({
                    "nlm_unique_id": r.nlm_unique_id,
                    "indexing_source": r.indexing_source,
                    "is_medline_indexing": r.is_medline_indexing,
                    "year": year,
                })
        indexed_years = pd.DataFrame(year_rows, columns=["nlm_unique_id", "indexing_source", "is_medline_indexing", "year"])
        indexed_years["year"] = indexed_years["year"].astype("Int64")
        indexed_years = indexed_years.drop_duplicates(["nlm_unique_id", "indexing_source", "year"])
        indexed_years = indexed_years.sort_values(["nlm_unique_id", "indexing_source", "year"]).reset_index(drop=True)

        # --- remaining children: just pass through (text already stripped) -----
        alt_titles = self.load_parquet(inter / "alt_titles.parquet")
        languages = self.load_parquet(inter / "languages.parquet").drop_duplicates(
            ["nlm_unique_id", "language", "lang_type"]
        ).reset_index(drop=True)
        mesh = self.load_parquet(inter / "mesh.parquet").drop_duplicates(
            ["nlm_unique_id", "descriptor_name"]
        ).reset_index(drop=True)

        self.save_parquet(journals, inter / "out_journals.parquet")
        self.save_parquet(issns, inter / "out_journal_issns.parquet")
        self.save_parquet(alt_titles, inter / "out_journal_alternate_titles.parquet")
        self.save_parquet(languages, inter / "out_journal_languages.parquet")
        self.save_parquet(mesh, inter / "out_journal_mesh_headings.parquet")
        self.save_parquet(relations, inter / "out_journal_relations.parquet")
        self.save_parquet(coverage, inter / "out_indexing_coverage.parquet")
        self.save_parquet(indexed_years, inter / "out_indexed_years.parquet")

    def load(self) -> None:
        inter = self.intermediate_path()
        for table, schema in _TABLES.items():
            df = self.load_parquet(inter / f"out_{table}.parquet")
            df = self.apply_schema(df, schema)
            df = df[list(schema)]
            self.save_parquet(df, self.output_path() / f"{table}.parquet")
            self.save_schema_yaml(schema, table)
