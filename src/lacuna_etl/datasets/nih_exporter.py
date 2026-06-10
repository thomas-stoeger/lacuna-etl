"""NIH ExPORTER — NIH's RePORTER export of funded projects and their outputs.

NIH ExPORTER ships one CSV per fiscal year for projects (``RePORTER_PRJ_C_FY*``),
project abstracts (``RePORTER_PRJABS_C_FY*``), and project↔publication links
(``RePORTER_PUBLNK_C_FY*``), each inside a yearly zip (~41 years), plus two
single-file catalogs (``Patents.csv``, ``ClinicalStudies.csv``). This is a
restartable, per-file pipeline: ``extract`` reads each source unit and writes one
intermediate shard, skipping units whose shard already exists; ``transform`` is a
no-op; ``load`` concatenates the shards per table, types, validates, and writes.

The grain key of ``projects`` is the ``application_id`` (``NihApplicationId``, the
unique project-year row id); the ``core_project_num`` (``NihCoreProjectNum``) is the
grant's stable identifier that links projects to abstracts, publications, patents, and
clinical studies. ``pubmed_id`` is ``PubmedId``. The PI list columns are kept as raw
delimited strings (the source's ``;``-separated ``PI_IDS``/``PI_NAMEs``, robust across
41 years of formatting). Both ExPORTER publication files are ingested: the project↔PMID
*linkage* (``PUBLNK`` → ``project_publications``) and the publication *metadata*
(``PUB_C`` → ``publications``, deduplicated on PMID since a publication is re-listed
under each linked fiscal year). The files are latin-1 encoded. See docs/DESIGN.md for
the table inventory.
"""
from __future__ import annotations

import zipfile
from pathlib import Path

import pandas as pd

from lacuna_etl.core.identifiers import NihApplicationId, NihCoreProjectNum, PubmedId
from lacuna_etl.core.pipeline import DatasetPipeline
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.registry import register

# Source header (normalised: UPPER, spaces->'_') -> snake_case output column.
_PROJECTS_RENAME = {
    "APPLICATION_ID": "application_id", "ACTIVITY": "activity", "ADMINISTERING_IC": "administering_ic",
    "APPLICATION_TYPE": "application_type", "ARRA_FUNDED": "arra_funded", "AWARD_NOTICE_DATE": "award_notice_date",
    "BUDGET_START": "budget_start", "BUDGET_END": "budget_end", "CFDA_CODE": "cfda_code",
    "CORE_PROJECT_NUM": "core_project_num", "ED_INST_TYPE": "ed_inst_type", "OPPORTUNITY_NUMBER": "opportunity_number",
    "FULL_PROJECT_NUM": "full_project_num", "FUNDING_ICS": "funding_ics", "FUNDING_MECHANISM": "funding_mechanism",
    "FY": "fy", "IC_NAME": "ic_name", "NIH_SPENDING_CATS": "nih_spending_cats", "ORG_CITY": "org_city",
    "ORG_COUNTRY": "org_country", "ORG_DEPT": "org_dept", "ORG_DISTRICT": "org_district", "ORG_DUNS": "org_duns",
    "ORG_FIPS": "org_fips", "ORG_IPF_CODE": "org_ipf_code", "ORG_NAME": "org_name", "ORG_STATE": "org_state",
    "ORG_ZIPCODE": "org_zipcode", "PHR": "phr", "PI_IDS": "pi_ids", "PI_NAMES": "pi_names",
    "PROGRAM_OFFICER_NAME": "program_officer_name", "PROJECT_START": "project_start", "PROJECT_END": "project_end",
    "PROJECT_TERMS": "project_terms", "PROJECT_TITLE": "project_title", "SERIAL_NUMBER": "serial_number",
    "STUDY_SECTION": "study_section", "STUDY_SECTION_NAME": "study_section_name", "SUBPROJECT_ID": "subproject_id",
    "SUFFIX": "suffix", "SUPPORT_YEAR": "support_year", "DIRECT_COST_AMT": "direct_cost_amt",
    "INDIRECT_COST_AMT": "indirect_cost_amt", "TOTAL_COST": "total_cost", "TOTAL_COST_SUB_PROJECT": "total_cost_sub_project",
}

PROJECTS_SCHEMA = {
    "application_id": ColumnSpec(identifier=NihApplicationId, required=True, description="NIH RePORTER application id; the grain key (unique per project-year row)"),
    "core_project_num": ColumnSpec(identifier=NihCoreProjectNum, description="Core project number; the grant's stable id that links across tables"),
    "full_project_num": ColumnSpec(description="Full project number (activity+IC+serial+support-year+suffix)"),
    "fy": ColumnSpec(description="Fiscal year (Int64)"),
    "activity": ColumnSpec(description="Activity code (e.g. R01, U54)"),
    "application_type": ColumnSpec(description="Application type code"),
    "administering_ic": ColumnSpec(description="Administering NIH Institute/Center abbreviation"),
    "ic_name": ColumnSpec(description="Administering Institute/Center name"),
    "funding_ics": ColumnSpec(description="Funding Institutes/Centers and amounts (raw delimited string)"),
    "funding_mechanism": ColumnSpec(description="Funding mechanism category"),
    "arra_funded": ColumnSpec(description="Whether the project was ARRA-funded (Y/N)"),
    "award_notice_date": ColumnSpec(description="Award notice date"),
    "budget_start": ColumnSpec(description="Budget period start date"),
    "budget_end": ColumnSpec(description="Budget period end date"),
    "project_start": ColumnSpec(description="Project period start date"),
    "project_end": ColumnSpec(description="Project period end date"),
    "cfda_code": ColumnSpec(description="CFDA program code"),
    "opportunity_number": ColumnSpec(description="Funding opportunity announcement number"),
    "ed_inst_type": ColumnSpec(description="Educational institution type"),
    "org_name": ColumnSpec(description="Awardee organization name"),
    "org_dept": ColumnSpec(description="Awardee organization department"),
    "org_city": ColumnSpec(description="Organization city"),
    "org_state": ColumnSpec(description="Organization state"),
    "org_zipcode": ColumnSpec(description="Organization ZIP code"),
    "org_country": ColumnSpec(description="Organization country"),
    "org_district": ColumnSpec(description="Congressional district"),
    "org_fips": ColumnSpec(description="Organization FIPS country code"),
    "org_duns": ColumnSpec(description="Organization DUNS number(s)"),
    "org_ipf_code": ColumnSpec(description="NIH IPF organization code"),
    "pi_ids": ColumnSpec(description="PI person-profile id(s), raw ';'-delimited (with '(contact)' marker); kept verbatim"),
    "pi_names": ColumnSpec(description="PI name(s), raw ';'-delimited; kept verbatim"),
    "program_officer_name": ColumnSpec(description="Program officer name"),
    "study_section": ColumnSpec(description="Scientific review group (study section) code"),
    "study_section_name": ColumnSpec(description="Scientific review group name"),
    "serial_number": ColumnSpec(description="Grant serial number"),
    "subproject_id": ColumnSpec(description="Subproject id (for multi-project grants)"),
    "suffix": ColumnSpec(description="Grant suffix (amendment/supplement)"),
    "support_year": ColumnSpec(description="Support (budget) year of the project (Int64)"),
    "direct_cost_amt": ColumnSpec(description="Direct cost amount (Float64)"),
    "indirect_cost_amt": ColumnSpec(description="Indirect cost amount (Float64)"),
    "total_cost": ColumnSpec(description="Total cost (Float64)"),
    "total_cost_sub_project": ColumnSpec(description="Total cost of the subproject (Float64)"),
    "nih_spending_cats": ColumnSpec(description="NIH spending categories (raw delimited string)"),
    "project_terms": ColumnSpec(description="Project terms / RCDC terms (raw delimited string)"),
    "project_title": ColumnSpec(description="Project title"),
    "phr": ColumnSpec(description="Public health relevance statement (free text)"),
}

ABSTRACTS_SCHEMA = {
    "application_id": ColumnSpec(identifier=NihApplicationId, required=True, description="NIH RePORTER application id"),
    "abstract_text": ColumnSpec(description="Project abstract text"),
}
_ABSTRACTS_RENAME = {"APPLICATION_ID": "application_id", "ABSTRACT_TEXT": "abstract_text"}

PUBLICATIONS_SCHEMA = {
    "pubmed_id": ColumnSpec(identifier=PubmedId, required=True, description="PubMed id of a publication linked to the project"),
    "core_project_num": ColumnSpec(identifier=NihCoreProjectNum, required=True, description="Core project number the publication is linked to"),
}
_PUBLICATIONS_RENAME = {"PMID": "pubmed_id", "PROJECT_NUMBER": "core_project_num"}

PATENTS_SCHEMA = {
    "patent_id": ColumnSpec(required=True, description="US patent id"),
    "patent_title": ColumnSpec(description="Patent title"),
    "core_project_num": ColumnSpec(identifier=NihCoreProjectNum, description="Core project number credited with the patent"),
    "patent_org_name": ColumnSpec(description="Patent assignee organization name"),
}
_PATENTS_RENAME = {"PATENT_ID": "patent_id", "PATENT_TITLE": "patent_title", "PROJECT_ID": "core_project_num", "PATENT_ORG_NAME": "patent_org_name"}

CLINICAL_STUDIES_SCHEMA = {
    "core_project_num": ColumnSpec(identifier=NihCoreProjectNum, required=True, description="Core project number linked to the clinical study"),
    "nct_id": ColumnSpec(required=True, description="ClinicalTrials.gov identifier (NCT…); kept as a plain string"),
    "study": ColumnSpec(description="Clinical study title"),
    "study_status": ColumnSpec(description="Clinical study status"),
}
_CLINICAL_RENAME = {"CORE_PROJECT_NUMBER": "core_project_num", "CLINICALTRIALS.GOV_ID": "nct_id", "STUDY": "study", "STUDY_STATUS": "study_status"}

# Publication metadata (RePORTER_PUB_C). Grain is one row per PMID — the same
# publication is listed under each fiscal year a linked project was active, so the
# concatenated shards are deduplicated on pubmed_id in load.
PUBLICATION_METADATA_SCHEMA = {
    "pubmed_id": ColumnSpec(identifier=PubmedId, required=True, description="PubMed id; the grain key (deduplicated across fiscal-year files)"),
    "pub_title": ColumnSpec(description="Publication title"),
    "author_list": ColumnSpec(description="Author list (raw delimited string)"),
    "affiliation": ColumnSpec(description="Author affiliation(s)"),
    "country": ColumnSpec(description="Publication country"),
    "journal_title": ColumnSpec(description="Journal title"),
    "journal_title_abbr": ColumnSpec(description="Journal title abbreviation"),
    "journal_volume": ColumnSpec(description="Journal volume"),
    "journal_issue": ColumnSpec(description="Journal issue"),
    "issn": ColumnSpec(description="Journal ISSN (kept as a plain string)"),
    "lang": ColumnSpec(description="Publication language"),
    "page_number": ColumnSpec(description="Page number(s)"),
    "pmc_id": ColumnSpec(description="PubMed Central id (PMC…), where assigned"),
    "pub_date": ColumnSpec(description="Publication date"),
    "pub_year": ColumnSpec(description="Publication year (Int64)"),
}
_PUBLICATION_METADATA_RENAME = {
    "PMID": "pubmed_id", "PUB_TITLE": "pub_title", "AUTHOR_LIST": "author_list", "AFFILIATION": "affiliation",
    "COUNTRY": "country", "JOURNAL_TITLE": "journal_title", "JOURNAL_TITLE_ABBR": "journal_title_abbr",
    "JOURNAL_VOLUME": "journal_volume", "JOURNAL_ISSUE": "journal_issue", "ISSN": "issn", "LANG": "lang",
    "PAGE_NUMBER": "page_number", "PMC_ID": "pmc_id", "PUB_DATE": "pub_date", "PUB_YEAR": "pub_year",
}

# Per-table: (schema, rename map, file glob — yearly zips — or a single filename).
_PROJECTS = ("projects", PROJECTS_SCHEMA, _PROJECTS_RENAME, "RePORTER_PRJ_C_FY*.zip")
_ABSTRACTS = ("project_abstracts", ABSTRACTS_SCHEMA, _ABSTRACTS_RENAME, "RePORTER_PRJABS_C_FY*.zip")
_PUBLICATIONS = ("project_publications", PUBLICATIONS_SCHEMA, _PUBLICATIONS_RENAME, "RePORTER_PUBLNK_C_FY*.zip")
_PUBLICATION_METADATA = ("publications", PUBLICATION_METADATA_SCHEMA, _PUBLICATION_METADATA_RENAME, "RePORTER_PUB_C_FY*.zip")
_PATENTS = ("patents", PATENTS_SCHEMA, _PATENTS_RENAME, "Patents.csv")
_CLINICAL = ("clinical_studies", CLINICAL_STUDIES_SCHEMA, _CLINICAL_RENAME, "ClinicalStudies.csv")

_INT_COLS = {"projects": ["fy", "support_year"], "publications": ["pub_year"]}
_FLOAT_COLS = {"projects": ["direct_cost_amt", "indirect_cost_amt", "total_cost", "total_cost_sub_project"]}
# Tables whose concatenated shards are deduplicated on a key in load.
_DEDUP_KEY = {"publications": "pubmed_id"}


@register
class NihExporter(DatasetPipeline):
    name = "nih_exporter"

    _TABLES = [_PROJECTS, _ABSTRACTS, _PUBLICATIONS, _PUBLICATION_METADATA, _PATENTS, _CLINICAL]

    def _read_csv(self, source: Path) -> pd.DataFrame:
        """Read an ExPORTER CSV (plain or the single CSV inside a yearly zip), latin-1."""
        kwargs = dict(encoding="latin-1", dtype=str, keep_default_na=False, na_values=[""])
        if source.suffix == ".zip":
            with zipfile.ZipFile(source) as zf:
                members = [n for n in zf.namelist() if n.lower().endswith(".csv") or n.lower().endswith(".txt")]
                if len(members) != 1:
                    raise ValueError(f"nih_exporter: expected one CSV in {source.name}, found {members}")
                with zf.open(members[0]) as fh:
                    return pd.read_csv(fh, **kwargs)
        return pd.read_csv(source, **kwargs)

    def _shard(self, df: pd.DataFrame, rename: dict[str, str], schema: dict) -> pd.DataFrame:
        """Normalise source headers, project onto the schema columns, empty -> null."""
        df = df.rename(columns={c: rename[c.strip().upper().replace(" ", "_")]
                                for c in df.columns if c.strip().upper().replace(" ", "_") in rename})
        out = pd.DataFrame({col: (df[col] if col in df.columns else pd.Series([None] * len(df), dtype="object"))
                            for col in schema})
        for col in schema:
            s = out[col].astype("string")
            out[col] = s.str.strip().where(lambda x: x != "", None)
        return out

    def _units(self, glob: str) -> list[Path]:
        if glob.endswith(".zip"):
            return sorted(self.raw_path().glob(glob))
        path = self.raw_path() / glob
        return [path] if path.exists() else []

    def extract(self) -> None:
        for stem, schema, rename, glob in self._TABLES:
            shard_dir = self.intermediate_path() / stem
            shard_dir.mkdir(parents=True, exist_ok=True)
            units = self._units(glob)
            if not units:
                raise FileNotFoundError(f"nih_exporter: no source files matching {glob} under {self.raw_path()}")
            for unit in units:
                shard = shard_dir / f"{unit.stem}.parquet"
                if shard.exists():  # restartable: skip already-extracted units
                    continue
                df = self._shard(self._read_csv(unit), rename, schema)
                self.save_parquet(df, shard)
            print(f"[{self.name}] {stem}: {len(units)} unit(s) extracted")

    def transform(self) -> None:
        # Faithful projection; reading/normalisation in extract, validation in load.
        pass

    def load(self) -> None:
        for stem, schema, _rename, _glob in self._TABLES:
            shard_dir = self.intermediate_path() / stem
            shards = sorted(shard_dir.glob("*.parquet"))
            df = pd.concat([self.load_parquet(s) for s in shards], ignore_index=True)
            df = df[list(schema)]
            if stem in _DEDUP_KEY:  # same publication is listed under each linked FY
                df = df.drop_duplicates(subset=_DEDUP_KEY[stem], ignore_index=True)
            for col in _INT_COLS.get(stem, []):
                df[col] = df[col].astype("Int64")
            for col in _FLOAT_COLS.get(stem, []):
                df[col] = df[col].astype("Float64")
            df = self.apply_schema(df, schema)
            self.save_parquet(df, self.output_path() / f"{stem}.parquet")
            self.save_schema_yaml(schema, stem)
            print(f"[{self.name}] {stem}: {len(df):,} rows")
