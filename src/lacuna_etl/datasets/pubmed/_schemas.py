"""Polars dtypes and column documentation for the PubMed ETL outputs.

`POLARS_SCHEMAS` is used at write time to keep each shard's dtypes consistent
even when a column is all-null for a given file. `TABLES_DOC` is what gets
serialised to the side-car yaml files alongside each output parquet.
"""

from __future__ import annotations

import polars as pl

from lacuna_etl.core.identifiers import Doi, Orcid, PubmedId
from lacuna_etl.core.schema import ColumnSpec


POLARS_SCHEMAS: dict[str, dict[str, pl.DataType]] = {
    "articles": {
        "pmid":               pl.Int64,
        "pmid_version":       pl.Int32,
        "title":              pl.String,
        "abstract":           pl.String,
        "pub_year":           pl.Int32,
        "pub_month":          pl.String,
        "pub_day":            pl.Int32,
        "medline_date":       pl.String,
        "pub_model":          pl.String,
        "cited_medium":       pl.String,
        "journal_title":      pl.String,
        "journal_iso":        pl.String,
        "volume":             pl.String,
        "issue":              pl.String,
        "pagination":         pl.String,
        "issn":               pl.String,
        "issn_type":          pl.String,
        "issn_linking":       pl.String,
        "nlm_unique_id":      pl.String,
        "medline_ta":         pl.String,
        "country":            pl.String,
        "language":           pl.String,
        "doi":                pl.String,
        "pmc_id":             pl.String,
        "pii":                pl.String,
        "citation_status":    pl.String,
        "indexing_method":    pl.String,
        "owner":              pl.String,
        "date_completed":     pl.String,
        "date_revised":       pl.String,
        "publication_status": pl.String,
    },
    "authors": {
        "pmid":            pl.Int64,
        "position":        pl.Int32,
        "last_name":       pl.String,
        "fore_name":       pl.String,
        "initials":        pl.String,
        "suffix":          pl.String,
        "collective_name": pl.String,
        "orcid":           pl.String,
        "valid":           pl.Boolean,
    },
    "affiliations": {
        "pmid":            pl.Int64,
        "author_position": pl.Int32,
        "affiliation":     pl.String,
    },
    "mesh_headings": {
        "pmid":             pl.Int64,
        "descriptor_ui":    pl.String,
        "descriptor_name":  pl.String,
        "descriptor_major": pl.Boolean,
        "qualifier_ui":     pl.String,
        "qualifier_name":   pl.String,
        "qualifier_major":  pl.Boolean,
    },
    "chemicals": {
        "pmid":            pl.Int64,
        "registry_number": pl.String,
        "ui":              pl.String,
        "name":            pl.String,
    },
    "publication_types": {
        "pmid": pl.Int64,
        "ui":   pl.String,
        "type": pl.String,
    },
    "grants": {
        "pmid":     pl.Int64,
        "grant_id": pl.String,
        "acronym":  pl.String,
        "agency":   pl.String,
        "country":  pl.String,
    },
    "keywords": {
        "pmid":    pl.Int64,
        "keyword": pl.String,
        "major":   pl.Boolean,
    },
    "article_ids": {
        "pmid":    pl.Int64,
        "id_type": pl.String,
        "value":   pl.String,
    },
    "references": {
        "pmid":     pl.Int64,
        "ref_pmid": pl.Int64,
        "citation": pl.String,
    },
    "deleted_pmids": {
        "pmid": pl.Int64,
    },
}


# Tables other than `articles` whose rows are tied to a particular version of
# the article record and must be replaced wholesale when the article is revised.
CHILD_TABLES: tuple[str, ...] = (
    "authors",
    "affiliations",
    "mesh_headings",
    "chemicals",
    "publication_types",
    "grants",
    "keywords",
    "article_ids",
    "references",
)


TABLES_DOC: dict[str, dict[str, ColumnSpec]] = {
    "articles": {
        "pmid":               ColumnSpec(identifier=PubmedId,  required=True, description="PubMed identifier"),
        "pmid_version":       ColumnSpec(description="PMID version number; usually 1"),
        "title":              ColumnSpec(description="Article title"),
        "abstract":           ColumnSpec(description="Abstract, with labelled sections joined by newline as 'LABEL: text'"),
        "pub_year":           ColumnSpec(description="Year from Article/Journal/JournalIssue/PubDate"),
        "pub_month":          ColumnSpec(description="Month from PubDate (often a 3-letter abbreviation)"),
        "pub_day":            ColumnSpec(description="Day from PubDate"),
        "medline_date":       ColumnSpec(description="Non-standard PubDate string used when Year/Month/Day are unavailable"),
        "pub_model":          ColumnSpec(description="Article@PubModel: Print, Electronic, Print-Electronic, Electronic-Print, Electronic-eCollection"),
        "cited_medium":       ColumnSpec(description="JournalIssue@CitedMedium: Print or Internet"),
        "journal_title":      ColumnSpec(description="Full journal title"),
        "journal_iso":        ColumnSpec(description="ISO journal abbreviation"),
        "volume":             ColumnSpec(description="Journal volume"),
        "issue":              ColumnSpec(description="Journal issue"),
        "pagination":         ColumnSpec(description="MedlinePgn pagination string"),
        "issn":               ColumnSpec(description="ISSN from <Journal>"),
        "issn_type":          ColumnSpec(description="ISSN IssnType (Print or Electronic)"),
        "issn_linking":       ColumnSpec(description="Linking ISSN from MedlineJournalInfo"),
        "nlm_unique_id":      ColumnSpec(description="NLM unique ID for the journal"),
        "medline_ta":         ColumnSpec(description="MedlineTA abbreviation"),
        "country":            ColumnSpec(description="Country from MedlineJournalInfo"),
        "language":           ColumnSpec(description="Comma-separated ISO 639-2 language codes"),
        "doi":                ColumnSpec(identifier=Doi, description="DOI (URL prefix stripped), preferring ELocationID then ArticleIdList"),
        "pmc_id":             ColumnSpec(description="PubMed Central ID, e.g. PMC1234567"),
        "pii":                ColumnSpec(description="Publisher Item Identifier from ELocationID"),
        "citation_status":    ColumnSpec(description="MedlineCitation@Status: e.g. MEDLINE, PubMed-not-MEDLINE, In-Process, Publisher"),
        "indexing_method":    ColumnSpec(description="MedlineCitation@IndexingMethod: Manual or Automated"),
        "owner":              ColumnSpec(description="MedlineCitation@Owner: usually NLM"),
        "date_completed":     ColumnSpec(description="DateCompleted formatted as YYYY-MM-DD"),
        "date_revised":       ColumnSpec(description="DateRevised formatted as YYYY-MM-DD"),
        "publication_status": ColumnSpec(description="PubmedData/PublicationStatus, e.g. ppublish, epublish, aheadofprint"),
    },
    "authors": {
        "pmid":            ColumnSpec(identifier=PubmedId, required=True, description="PubMed identifier of the article"),
        "position":        ColumnSpec(description="1-based author position within the article's AuthorList"),
        "last_name":       ColumnSpec(description="Author last name"),
        "fore_name":       ColumnSpec(description="Author fore name"),
        "initials":        ColumnSpec(description="Author initials"),
        "suffix":          ColumnSpec(description="Author suffix (Jr., III, etc.)"),
        "collective_name": ColumnSpec(description="Collective / group name, when the author is an organisation rather than a person"),
        "orcid":           ColumnSpec(identifier=Orcid, description="ORCID iD, normalised to canonical hyphenated form; null if missing or fails checksum"),
        "valid":           ColumnSpec(description="False when Author@ValidYN='N' (NLM flagged as invalid byline)"),
    },
    "affiliations": {
        "pmid":            ColumnSpec(identifier=PubmedId, required=True, description="PubMed identifier of the article"),
        "author_position": ColumnSpec(description="Author position the affiliation belongs to"),
        "affiliation":     ColumnSpec(description="Affiliation text as it appears in the citation"),
    },
    "mesh_headings": {
        "pmid":             ColumnSpec(identifier=PubmedId, required=True, description="PubMed identifier of the article"),
        "descriptor_ui":    ColumnSpec(description="MeSH descriptor UI, e.g. D000445"),
        "descriptor_name":  ColumnSpec(description="MeSH descriptor name"),
        "descriptor_major": ColumnSpec(description="MajorTopicYN on the descriptor"),
        "qualifier_ui":     ColumnSpec(description="MeSH qualifier UI, e.g. Q000378; null when the descriptor has no qualifier"),
        "qualifier_name":   ColumnSpec(description="MeSH qualifier name"),
        "qualifier_major":  ColumnSpec(description="MajorTopicYN on the qualifier"),
    },
    "chemicals": {
        "pmid":            ColumnSpec(identifier=PubmedId, required=True, description="PubMed identifier of the article"),
        "registry_number": ColumnSpec(description="CAS or EC registry number; '0' if unassigned"),
        "ui":              ColumnSpec(description="MeSH supplementary concept UI"),
        "name":            ColumnSpec(description="Substance name"),
    },
    "publication_types": {
        "pmid": ColumnSpec(identifier=PubmedId, required=True, description="PubMed identifier of the article"),
        "ui":   ColumnSpec(description="MeSH publication-type UI, e.g. D016428"),
        "type": ColumnSpec(description="Publication type name, e.g. Journal Article"),
    },
    "grants": {
        "pmid":     ColumnSpec(identifier=PubmedId, required=True, description="PubMed identifier of the article"),
        "grant_id": ColumnSpec(description="GrantID"),
        "acronym":  ColumnSpec(description="Funding acronym"),
        "agency":   ColumnSpec(description="Funding agency"),
        "country":  ColumnSpec(description="Funding country"),
    },
    "keywords": {
        "pmid":    ColumnSpec(identifier=PubmedId, required=True, description="PubMed identifier of the article"),
        "keyword": ColumnSpec(description="Author- or publisher-supplied keyword"),
        "major":   ColumnSpec(description="Keyword MajorTopicYN flag"),
    },
    "article_ids": {
        "pmid":    ColumnSpec(identifier=PubmedId, required=True, description="PubMed identifier of the article"),
        "id_type": ColumnSpec(description="IdType from ArticleIdList: pubmed, doi, pmc, pii, mid, etc."),
        "value":   ColumnSpec(description="Identifier value; for doi the URL prefix is stripped"),
    },
    "references": {
        "pmid":     ColumnSpec(identifier=PubmedId, required=True, description="Citing article PMID"),
        "ref_pmid": ColumnSpec(identifier=PubmedId, description="Cited article PMID when present in the reference's ArticleIdList"),
        "citation": ColumnSpec(description="Free-text citation as it appears in the article"),
    },
    "deleted_pmids": {
        "pmid": ColumnSpec(identifier=PubmedId, required=True, description="PMID withdrawn via <DeleteCitation> in an update file"),
    },
}
