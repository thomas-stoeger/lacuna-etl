"""Streaming XML parser for PubMed MEDLINE distributions.

Each baseline / update file is a gzipped <PubmedArticleSet> containing many
<PubmedArticle> entries and (in update files) a trailing <DeleteCitation>
listing PMIDs to remove.

`iter_pubmed_records` walks the file with `ElementTree.iterparse`, yielding
fully-formed top-level elements one at a time and clearing the document
between yields so memory stays bounded by a single article.

`parse_article` flattens one <PubmedArticle> into a dict of row lists, one
list per output table. Identifiers (DOI, ORCID) are normalised here so the
shapes match the column specs in `_schemas`.
"""

from __future__ import annotations

import gzip
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Iterator

from lacuna_etl.core.identifiers import Doi, Orcid, PubmedId


_DOI_WHITESPACE = re.compile(r"\s+")
_FOUR_DIGIT = re.compile(r"\d{4}")


def _clean_doi(value: str | None) -> str | None:
    """Strip URL prefix + internal whitespace, and reject values that don't look like real DOIs.

    Some PubMed records label `pii`-style strings as ELocationID@EIdType='doi'
    (e.g. 'e25.00390'). The Doi column validator requires '10.<registrant>/<id>',
    so we drop anything that doesn't begin with '10.' here. A few hundred records
    also contain stray spaces inside otherwise valid DOIs ('10.1302/0301-620x.82b2 .10033');
    spaces aren't legal in DOIs, so we collapse them out.
    """
    short = Doi.shorten(value)
    if short is None:
        return None
    short = _DOI_WHITESPACE.sub("", short)
    if not short.startswith("10."):
        return None
    registrant, sep, suffix = short.partition("/")
    if not sep or not suffix:
        return None
    return short


def _text(elem: ET.Element | None, tag: str) -> str | None:
    if elem is None:
        return None
    child = elem.find(tag)
    if child is None or child.text is None:
        return None
    s = child.text.strip()
    return s or None


def _all_text(elem: ET.Element | None) -> str | None:
    if elem is None:
        return None
    s = "".join(elem.itertext()).strip()
    return s or None


def _parse_int(s: str | None) -> int | None:
    if s is None:
        return None
    try:
        return int(s)
    except (TypeError, ValueError):
        return None


def _format_date(date_elem: ET.Element | None) -> str | None:
    if date_elem is None:
        return None
    y = _parse_int(_text(date_elem, "Year"))
    m = _parse_int(_text(date_elem, "Month"))
    d = _parse_int(_text(date_elem, "Day"))
    if y is None or m is None or d is None:
        return None
    return f"{y:04d}-{m:02d}-{d:02d}"


def parse_article(elem: ET.Element) -> dict:
    """Flatten one <PubmedArticle> into row dicts grouped by output table."""
    mc = elem.find("MedlineCitation")
    if mc is None:
        return _empty_rows()

    pmid_el = mc.find("PMID")
    pmid = PubmedId.parse(pmid_el.text) if pmid_el is not None else None
    if pmid is None:
        return _empty_rows()
    pmid_version = _parse_int(pmid_el.get("Version")) if pmid_el is not None else None

    article = mc.find("Article")
    journal = article.find("Journal") if article is not None else None
    journal_issue = journal.find("JournalIssue") if journal is not None else None
    pub_date = journal_issue.find("PubDate") if journal_issue is not None else None

    title_el = article.find("ArticleTitle") if article is not None else None
    title = _all_text(title_el)

    abstract_parts: list[str] = []
    abstract_el = article.find("Abstract") if article is not None else None
    if abstract_el is not None:
        for at in abstract_el.findall("AbstractText"):
            text = _all_text(at)
            if not text:
                continue
            label = at.get("Label")
            abstract_parts.append(f"{label}: {text}" if label else text)
    abstract = "\n".join(abstract_parts) if abstract_parts else None

    pub_year = _parse_int(_text(pub_date, "Year")) if pub_date is not None else None
    pub_month = _text(pub_date, "Month") if pub_date is not None else None
    pub_day = _parse_int(_text(pub_date, "Day")) if pub_date is not None else None
    medline_date = _text(pub_date, "MedlineDate") if pub_date is not None else None
    if pub_year is None and medline_date is not None:
        # Only infer when the free-text date carries an unambiguous single year;
        # ranges like "1947-1948" are left null rather than picking a side.
        years = _FOUR_DIGIT.findall(medline_date)
        if len(years) == 1:
            pub_year = int(years[0])

    issn_el = journal.find("ISSN") if journal is not None else None
    issn = (issn_el.text.strip() if issn_el is not None and issn_el.text else None)
    issn_type = issn_el.get("IssnType") if issn_el is not None else None

    journal_title = _text(journal, "Title")
    journal_iso = _text(journal, "ISOAbbreviation")
    volume = _text(journal_issue, "Volume") if journal_issue is not None else None
    issue = _text(journal_issue, "Issue") if journal_issue is not None else None
    cited_medium = journal_issue.get("CitedMedium") if journal_issue is not None else None

    pagination_el = article.find("Pagination") if article is not None else None
    pagination = _text(pagination_el, "MedlinePgn") if pagination_el is not None else None

    doi: str | None = None
    pii: str | None = None
    if article is not None:
        for eid in article.findall("ELocationID"):
            value = (eid.text.strip() if eid.text else None) or None
            if not value:
                continue
            if eid.get("EIdType") == "doi" and doi is None:
                doi = _clean_doi(value)
            elif eid.get("EIdType") == "pii" and pii is None:
                pii = value

    langs: list[str] = []
    if article is not None:
        for lang in article.findall("Language"):
            if lang.text:
                t = lang.text.strip()
                if t:
                    langs.append(t)
    language = ",".join(langs) if langs else None

    mji = mc.find("MedlineJournalInfo")
    country = _text(mji, "Country")
    medline_ta = _text(mji, "MedlineTA")
    nlm_unique_id = _text(mji, "NlmUniqueID")
    issn_linking = _text(mji, "ISSNLinking")

    date_completed = _format_date(mc.find("DateCompleted"))
    date_revised = _format_date(mc.find("DateRevised"))

    pmdata = elem.find("PubmedData")
    publication_status = _text(pmdata, "PublicationStatus") if pmdata is not None else None

    article_id_rows: list[dict] = []
    pmc_id: str | None = None
    if pmdata is not None:
        aid_list = pmdata.find("ArticleIdList")
        if aid_list is not None:
            for aid in aid_list.findall("ArticleId"):
                if not aid.text:
                    continue
                value = aid.text.strip()
                if not value:
                    continue
                id_type = aid.get("IdType")
                if id_type == "doi":
                    cleaned = _clean_doi(value)
                    if cleaned is None:
                        continue  # skip mislabelled non-DOIs from ArticleIdList
                    value = cleaned
                article_id_rows.append({"pmid": pmid, "id_type": id_type, "value": value})
                if id_type == "pmc" and pmc_id is None:
                    pmc_id = value
                elif id_type == "doi" and doi is None:
                    doi = value

    article_row = {
        "pmid": pmid,
        "pmid_version": pmid_version,
        "title": title,
        "abstract": abstract,
        "pub_year": pub_year,
        "pub_month": pub_month,
        "pub_day": pub_day,
        "medline_date": medline_date,
        "pub_model": article.get("PubModel") if article is not None else None,
        "cited_medium": cited_medium,
        "journal_title": journal_title,
        "journal_iso": journal_iso,
        "volume": volume,
        "issue": issue,
        "pagination": pagination,
        "issn": issn,
        "issn_type": issn_type,
        "issn_linking": issn_linking,
        "nlm_unique_id": nlm_unique_id,
        "medline_ta": medline_ta,
        "country": country,
        "language": language,
        "doi": doi,
        "pmc_id": pmc_id,
        "pii": pii,
        "citation_status": mc.get("Status"),
        "indexing_method": mc.get("IndexingMethod"),
        "owner": mc.get("Owner"),
        "date_completed": date_completed,
        "date_revised": date_revised,
        "publication_status": publication_status,
    }

    author_rows: list[dict] = []
    affiliation_rows: list[dict] = []
    if article is not None:
        alist = article.find("AuthorList")
        if alist is not None:
            for pos, au in enumerate(alist.findall("Author"), start=1):
                orcid_raw = None
                for ident in au.findall("Identifier"):
                    if ident.get("Source") == "ORCID" and ident.text:
                        orcid_raw = ident.text.strip() or None
                        break
                author_rows.append({
                    "pmid": pmid,
                    "position": pos,
                    "last_name": _text(au, "LastName"),
                    "fore_name": _text(au, "ForeName"),
                    "initials": _text(au, "Initials"),
                    "suffix": _text(au, "Suffix"),
                    "collective_name": _text(au, "CollectiveName"),
                    "orcid": Orcid.normalize(orcid_raw),
                    "valid": au.get("ValidYN") != "N",
                })
                for aff_info in au.findall("AffiliationInfo"):
                    aff = _text(aff_info, "Affiliation")
                    if aff:
                        affiliation_rows.append({
                            "pmid": pmid,
                            "author_position": pos,
                            "affiliation": aff,
                        })

    mesh_rows: list[dict] = []
    mh_list = mc.find("MeshHeadingList")
    if mh_list is not None:
        for mh in mh_list.findall("MeshHeading"):
            dn = mh.find("DescriptorName")
            d_ui = dn.get("UI") if dn is not None else None
            d_name = dn.text if dn is not None else None
            d_major = (dn.get("MajorTopicYN") == "Y") if dn is not None else False
            quals = mh.findall("QualifierName")
            if quals:
                for qn in quals:
                    mesh_rows.append({
                        "pmid": pmid,
                        "descriptor_ui": d_ui,
                        "descriptor_name": d_name,
                        "descriptor_major": d_major,
                        "qualifier_ui": qn.get("UI"),
                        "qualifier_name": qn.text,
                        "qualifier_major": qn.get("MajorTopicYN") == "Y",
                    })
            else:
                mesh_rows.append({
                    "pmid": pmid,
                    "descriptor_ui": d_ui,
                    "descriptor_name": d_name,
                    "descriptor_major": d_major,
                    "qualifier_ui": None,
                    "qualifier_name": None,
                    "qualifier_major": False,
                })

    chemical_rows: list[dict] = []
    cl = mc.find("ChemicalList")
    if cl is not None:
        for c in cl.findall("Chemical"):
            nos = c.find("NameOfSubstance")
            chemical_rows.append({
                "pmid": pmid,
                "registry_number": _text(c, "RegistryNumber"),
                "ui": nos.get("UI") if nos is not None else None,
                "name": nos.text if nos is not None else None,
            })

    pub_type_rows: list[dict] = []
    if article is not None:
        ptl = article.find("PublicationTypeList")
        if ptl is not None:
            for pt in ptl.findall("PublicationType"):
                pub_type_rows.append({
                    "pmid": pmid,
                    "ui": pt.get("UI"),
                    "type": pt.text,
                })

    grant_rows: list[dict] = []
    if article is not None:
        gl = article.find("GrantList")
        if gl is not None:
            for g in gl.findall("Grant"):
                grant_rows.append({
                    "pmid": pmid,
                    "grant_id": _text(g, "GrantID"),
                    "acronym": _text(g, "Acronym"),
                    "agency": _text(g, "Agency"),
                    "country": _text(g, "Country"),
                })

    keyword_rows: list[dict] = []
    for klist in mc.findall("KeywordList"):
        for kw in klist.findall("Keyword"):
            t = _all_text(kw)
            if t:
                keyword_rows.append({
                    "pmid": pmid,
                    "keyword": t,
                    "major": kw.get("MajorTopicYN") == "Y",
                })

    reference_rows: list[dict] = []
    if pmdata is not None:
        for ref in pmdata.iter("Reference"):
            citation = _text(ref, "Citation")
            ref_pmid: int | None = None
            rid_list = ref.find("ArticleIdList")
            if rid_list is not None:
                for rid in rid_list.findall("ArticleId"):
                    if rid.get("IdType") == "pubmed" and rid.text:
                        # Reference pubmed ArticleIds are a dirty best-effort field:
                        # alongside real PMIDs, NCBI emits unresolved-reference
                        # sentinels (e.g. 'NOT_FOUND;INVALID_JOURNAL') and, rarely,
                        # ids mislabeled into this slot (e.g. a 'PMC…' id under
                        # IdType="pubmed"). PubmedId.parse accepts only a positive
                        # integer; treat anything it rejects as "no resolved link"
                        # (null) rather than aborting the run — this is a
                        # cross-reference, not a validated key. The reference itself
                        # is still kept if it has citation text.
                        try:
                            ref_pmid = PubmedId.parse(rid.text)
                        except ValueError:
                            ref_pmid = None
                        if ref_pmid is not None:
                            break
            if citation or ref_pmid is not None:
                reference_rows.append({
                    "pmid": pmid,
                    "ref_pmid": ref_pmid,
                    "citation": citation,
                })

    return {
        "articles":          [article_row],
        "authors":           author_rows,
        "affiliations":      affiliation_rows,
        "mesh_headings":     mesh_rows,
        "chemicals":         chemical_rows,
        "publication_types": pub_type_rows,
        "grants":            grant_rows,
        "keywords":          keyword_rows,
        "article_ids":       article_id_rows,
        "references":        reference_rows,
    }


def _empty_rows() -> dict:
    return {
        "articles": [],
        "authors": [],
        "affiliations": [],
        "mesh_headings": [],
        "chemicals": [],
        "publication_types": [],
        "grants": [],
        "keywords": [],
        "article_ids": [],
        "references": [],
    }


def parse_delete_citation(elem: ET.Element) -> list[int]:
    """Extract PMIDs listed in a <DeleteCitation> element."""
    pmids: list[int] = []
    for pmid_el in elem.findall("PMID"):
        n = PubmedId.parse(pmid_el.text)
        if n is not None:
            pmids.append(n)
    return pmids


def iter_pubmed_records(gz_path: Path) -> Iterator[tuple[str, ET.Element]]:
    """Stream top-level entries from a gzipped PubMed XML file.

    Yields ('article', <PubmedArticle>) or ('delete', <DeleteCitation>).
    The yielded element is cleared immediately after the consumer returns,
    so callers must finish reading it before requesting the next.
    """
    with gzip.open(gz_path, "rb") as fh:
        ctx = iter(ET.iterparse(fh, events=("start", "end")))
        _, root = next(ctx)
        for event, el in ctx:
            if event != "end":
                continue
            if el.tag == "PubmedArticle":
                yield "article", el
                el.clear()
                root.clear()
            elif el.tag == "DeleteCitation":
                yield "delete", el
                el.clear()
                root.clear()
