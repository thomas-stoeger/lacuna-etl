"""Polars dtypes and column documentation for the ORCID ETL outputs.

``POLARS_SCHEMAS`` pins each shard's dtypes so a batch in which a column is all-null
still writes a consistent shard. ``TABLES_DOC`` is the ``ColumnSpec`` set serialised
to the side-car ``.yml`` files and validated against the consolidated outputs.

Ten tables, all keyed by ``orcid`` (the canonical hyphenated ORCID iD):

  records                      one row per ORCID record (person-level scalars)
  other_names                  alternative name forms
  researcher_urls              personal/professional URLs
  keywords                     self-declared keywords
  addresses                    country of residence
  person_external_identifiers  person-level external IDs (Scopus, ResearcherID, …)
  affiliations                 the seven affiliation sections, unified by a
                               ``affiliation_type`` discriminator
  fundings                     funding/grant summaries
  works                        work (publication) summaries
  work_external_identifiers    a work's external IDs (DOI, EID, ISSN, …): the
                               bibliographic crosswalk, keyed by (orcid, put_code)
"""
from __future__ import annotations

import polars as pl

from lacuna_etl.core.identifiers import CountryCode, Orcid
from lacuna_etl.core.schema import ColumnSpec

# The seven ORCID activity sections that share the identical ``affiliation-summary``
# shape, unified into one table: (section element local-name, affiliation_type value).
AFFILIATION_SECTIONS: tuple[tuple[str, str], ...] = (
    ("distinctions", "distinction"),
    ("educations", "education"),
    ("employments", "employment"),
    ("invited-positions", "invited-position"),
    ("memberships", "membership"),
    ("qualifications", "qualification"),
    ("services", "service"),
)
_AFFILIATION_TYPES = {atype for _, atype in AFFILIATION_SECTIONS}


POLARS_SCHEMAS: dict[str, dict[str, pl.DataType]] = {
    "records": {
        "orcid":                  pl.String,
        "given_names":            pl.String,
        "family_name":            pl.String,
        "credit_name":            pl.String,
        "name_visibility":        pl.String,
        "locale":                 pl.String,
        "creation_method":        pl.String,
        "submission_date":        pl.String,
        "last_modified_date":     pl.String,
        "claimed":                pl.Boolean,
        "verified_email":         pl.Boolean,
        "verified_primary_email": pl.Boolean,
    },
    "other_names": {
        "orcid":   pl.String,
        "content": pl.String,
    },
    "researcher_urls": {
        "orcid":    pl.String,
        "url_name": pl.String,
        "url":      pl.String,
    },
    "keywords": {
        "orcid":   pl.String,
        "content": pl.String,
    },
    "addresses": {
        "orcid":   pl.String,
        "country": pl.String,
    },
    "person_external_identifiers": {
        "orcid":                    pl.String,
        "external_id_type":         pl.String,
        "external_id_value":        pl.String,
        "external_id_url":          pl.String,
        "external_id_relationship": pl.String,
    },
    "affiliations": {
        "orcid":                                 pl.String,
        "affiliation_type":                      pl.String,
        "put_code":                              pl.Int64,
        "department_name":                       pl.String,
        "role_title":                            pl.String,
        "start_date":                            pl.String,
        "end_date":                              pl.String,
        "organization_name":                     pl.String,
        "organization_city":                     pl.String,
        "organization_region":                   pl.String,
        "organization_country":                  pl.String,
        "disambiguated_organization_identifier": pl.String,
        "disambiguation_source":                 pl.String,
    },
    "fundings": {
        "orcid":                                 pl.String,
        "put_code":                              pl.Int64,
        "title":                                 pl.String,
        "funding_type":                          pl.String,
        "start_date":                            pl.String,
        "end_date":                              pl.String,
        "organization_name":                     pl.String,
        "organization_city":                     pl.String,
        "organization_region":                   pl.String,
        "organization_country":                  pl.String,
        "disambiguated_organization_identifier": pl.String,
        "disambiguation_source":                 pl.String,
    },
    "works": {
        "orcid":            pl.String,
        "put_code":         pl.Int64,
        "title":            pl.String,
        "subtitle":         pl.String,
        "work_type":        pl.String,
        "journal_title":    pl.String,
        "publication_date": pl.String,
        "url":              pl.String,
        "source_name":      pl.String,
    },
    "work_external_identifiers": {
        "orcid":                    pl.String,
        "put_code":                 pl.Int64,
        "external_id_type":         pl.String,
        "external_id_value":        pl.String,
        "external_id_normalized":   pl.String,
        "external_id_relationship": pl.String,
    },
}


TABLES_DOC: dict[str, dict[str, ColumnSpec]] = {
    "records": {
        "orcid":                  ColumnSpec(identifier=Orcid, required=True, description="ORCID iD, canonical hyphenated form (the record's path); the grain key"),
        "given_names":            ColumnSpec(description="Given (first) names"),
        "family_name":            ColumnSpec(description="Family (last) name"),
        "credit_name":            ColumnSpec(description="Published / credit name, when the researcher set one"),
        "name_visibility":        ColumnSpec(description="Visibility of the name block (public, limited, private)"),
        "locale":                 ColumnSpec(description="Account locale, e.g. 'en'"),
        "creation_method":        ColumnSpec(description="How the record was created (Direct, Member-referred, Website, API, …)"),
        "submission_date":        ColumnSpec(description="Record creation/submission timestamp (ISO 8601 string)"),
        "last_modified_date":     ColumnSpec(description="Record last-modified timestamp (ISO 8601 string)"),
        "claimed":                ColumnSpec(description="Whether the record has been claimed by its owner (boolean)"),
        "verified_email":         ColumnSpec(description="Whether the record has at least one verified email (boolean)"),
        "verified_primary_email": ColumnSpec(description="Whether the record's primary email is verified (boolean)"),
    },
    "other_names": {
        "orcid":   ColumnSpec(identifier=Orcid, required=True, description="ORCID iD"),
        "content": ColumnSpec(required=True, description="An alternative name the researcher publishes under"),
    },
    "researcher_urls": {
        "orcid":    ColumnSpec(identifier=Orcid, required=True, description="ORCID iD"),
        "url_name": ColumnSpec(description="Label for the URL"),
        "url":      ColumnSpec(required=True, description="A personal or professional URL"),
    },
    "keywords": {
        "orcid":   ColumnSpec(identifier=Orcid, required=True, description="ORCID iD"),
        "content": ColumnSpec(required=True, description="A self-declared keyword / research area"),
    },
    "addresses": {
        "orcid":   ColumnSpec(identifier=Orcid, required=True, description="ORCID iD"),
        "country": ColumnSpec(identifier=CountryCode, required=True, description="Country of residence (ISO 3166-1 alpha-2)"),
    },
    "person_external_identifiers": {
        "orcid":                    ColumnSpec(identifier=Orcid, required=True, description="ORCID iD"),
        "external_id_type":         ColumnSpec(required=True, description="External identifier system, e.g. 'Scopus Author ID', 'ResearcherID', 'Loop profile'"),
        "external_id_value":        ColumnSpec(description="The external identifier value (heterogeneous across systems; plain string)"),
        "external_id_url":          ColumnSpec(description="URL the external identifier resolves to"),
        "external_id_relationship": ColumnSpec(description="Relationship of the identifier to the record (self, part-of)"),
    },
    "affiliations": {
        "orcid":                                 ColumnSpec(identifier=Orcid, required=True, description="ORCID iD"),
        "affiliation_type":                      ColumnSpec(allowed_values=_AFFILIATION_TYPES, required=True, description="Which affiliation section the row comes from: distinction, education, employment, invited-position, membership, qualification, or service"),
        "put_code":                              ColumnSpec(description="ORCID put-code, the affiliation's stable id within the record (Int64)"),
        "department_name":                       ColumnSpec(description="Department / division name"),
        "role_title":                            ColumnSpec(description="Role or title held"),
        "start_date":                            ColumnSpec(description="Start date, partial ISO (year, optionally -month, -day)"),
        "end_date":                              ColumnSpec(description="End date, partial ISO (year, optionally -month, -day); null if ongoing"),
        "organization_name":                     ColumnSpec(description="Affiliated organization name"),
        "organization_city":                     ColumnSpec(description="Organization city"),
        "organization_region":                   ColumnSpec(description="Organization region / state"),
        "organization_country":                  ColumnSpec(identifier=CountryCode, description="Organization country (ISO 3166-1 alpha-2)"),
        "disambiguated_organization_identifier": ColumnSpec(description="Disambiguated organization id (value heterogeneous by source; plain string)"),
        "disambiguation_source":                 ColumnSpec(description="Source of the disambiguated id (ROR, GRID, RINGGOLD, FUNDREF, LEI, …)"),
    },
    "fundings": {
        "orcid":                                 ColumnSpec(identifier=Orcid, required=True, description="ORCID iD"),
        "put_code":                              ColumnSpec(description="ORCID put-code, the funding's stable id within the record (Int64)"),
        "title":                                 ColumnSpec(description="Funding / grant title"),
        "funding_type":                          ColumnSpec(description="Funding type (grant, contract, award, salary-award)"),
        "start_date":                            ColumnSpec(description="Start date, partial ISO (year, optionally -month, -day)"),
        "end_date":                              ColumnSpec(description="End date, partial ISO (year, optionally -month, -day)"),
        "organization_name":                     ColumnSpec(description="Funding organization name"),
        "organization_city":                     ColumnSpec(description="Funding organization city"),
        "organization_region":                   ColumnSpec(description="Funding organization region / state"),
        "organization_country":                  ColumnSpec(identifier=CountryCode, description="Funding organization country (ISO 3166-1 alpha-2)"),
        "disambiguated_organization_identifier": ColumnSpec(description="Disambiguated organization id (value heterogeneous by source; plain string)"),
        "disambiguation_source":                 ColumnSpec(description="Source of the disambiguated id (ROR, GRID, RINGGOLD, FUNDREF, …)"),
    },
    "works": {
        "orcid":            ColumnSpec(identifier=Orcid, required=True, description="ORCID iD"),
        "put_code":         ColumnSpec(description="ORCID put-code, the work's stable id within the record (Int64); joins to work_external_identifiers"),
        "title":            ColumnSpec(description="Work title"),
        "subtitle":         ColumnSpec(description="Work subtitle"),
        "work_type":        ColumnSpec(description="Work type (journal-article, book-chapter, conference-paper, patent, …; a documented free string)"),
        "journal_title":    ColumnSpec(description="Containing journal / book / conference title"),
        "publication_date": ColumnSpec(description="Publication date, partial ISO (year, optionally -month, -day)"),
        "url":              ColumnSpec(description="URL for the work"),
        "source_name":      ColumnSpec(description="Name of the source that asserted the work (the researcher, or a member organisation like 'Scopus - Elsevier', 'Crossref')"),
    },
    "work_external_identifiers": {
        "orcid":                    ColumnSpec(identifier=Orcid, required=True, description="ORCID iD"),
        "put_code":                 ColumnSpec(description="ORCID put-code of the work this identifier belongs to (joins to works.put_code; Int64)"),
        "external_id_type":         ColumnSpec(required=True, description="Identifier system, e.g. 'doi', 'eid', 'issn', 'isbn', 'pmid', 'pmc', 'wosuid'"),
        "external_id_value":        ColumnSpec(description="The identifier as supplied (heterogeneous by type; plain string — a consumer crosswalk types e.g. the DOIs)"),
        "external_id_normalized":   ColumnSpec(description="ORCID's normalized form of the identifier, when it produced one"),
        "external_id_relationship": ColumnSpec(description="Relationship of the identifier to the work (self, part-of, version-of)"),
    },
}
