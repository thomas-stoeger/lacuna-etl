"""Streaming parser for the ORCID Public Data File summaries tarball.

The summaries snapshot is a single ~43 GB ``*_summaries.tar.gz`` holding ~20M tiny
members, one XML file per record, laid out as
``ORCID_<release>_summaries/<NNN>/<orcid>.xml`` where ``<NNN>`` is a 000–999
directory bucket. Because gzip is not seekable, the archive is read once in
streaming mode (``r|gz``); ``iter_members`` yields ``(prefix, data)`` for every
member in archive (directory) order, and ``parse_record`` flattens one record's
small XML document into a dict of row lists, one list per output table.

Each record is a ``<record:record>`` with namespaced children: an
``orcid-identifier`` (the iD), ``history`` (creation/claim flags), a ``person``
block (name + the other-name/url/keyword/address/external-identifier lists), and an
``activities-summary`` block (the seven affiliation sections, fundings, and works).
Tags are matched by local name so the many ORCID namespaces don't have to be tracked
explicitly.
"""
from __future__ import annotations

import re
import tarfile
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Iterator

from lacuna_etl.datasets.orcid._schemas import AFFILIATION_SECTIONS, POLARS_SCHEMAS

# C0 control bytes illegal in XML 1.0 (everything but tab/LF/CR). A few records
# carry stray control bytes; stripping them lets an otherwise well-formed document
# parse instead of being dropped.
_ILLEGAL_XML = re.compile(rb"[\x00-\x08\x0b\x0c\x0e-\x1f]")

_TABLES = tuple(POLARS_SCHEMAS)


def iter_members(tar_path: Path) -> Iterator[tuple[str, bytes]]:
    """Yield ``(directory_prefix, xml_bytes)`` for each record file in the tarball.

    Streaming mode (``r|gz``, no random access): each member must be read before the
    iterator advances. Non-file members and the bundled ``_license`` are skipped.
    """
    with tarfile.open(tar_path, "r|gz") as tf:
        for member in tf:
            if not member.isfile() or not member.name.endswith(".xml"):
                continue
            parts = member.name.split("/")
            if len(parts) < 2:
                continue
            prefix = parts[-2]
            fileobj = tf.extractfile(member)
            if fileobj is None:
                continue
            yield prefix, fileobj.read()


# --- local-name XML helpers ------------------------------------------------

def _ln(el: ET.Element) -> str:
    """Local tag name (namespace stripped)."""
    tag = el.tag
    return tag.rsplit("}", 1)[-1] if isinstance(tag, str) else ""


def _child(el: ET.Element | None, name: str) -> ET.Element | None:
    if el is None:
        return None
    for c in el:
        if _ln(c) == name:
            return c
    return None


def _children(el: ET.Element | None, name: str) -> list[ET.Element]:
    if el is None:
        return []
    return [c for c in el if _ln(c) == name]


def _text(el: ET.Element | None) -> str | None:
    if el is None or not el.text:
        return None
    t = el.text.strip()
    return t or None


def _path(el: ET.Element | None, *names: str) -> str | None:
    """Descend by successive child local-names and return the leaf text."""
    cur = el
    for n in names:
        cur = _child(cur, n)
        if cur is None:
            return None
    return _text(cur)


def _date(el: ET.Element | None) -> str | None:
    """A start/end/publication date element -> partial ISO string ('YYYY[-MM[-DD]]')."""
    if el is None:
        return None
    year = _path(el, "year")
    if not year:
        return None
    out = year
    month = _path(el, "month")
    if month:
        out += f"-{month}"
        day = _path(el, "day")
        if day:
            out += f"-{day}"
    return out


def _bool(value: str | None) -> bool | None:
    if value == "true":
        return True
    if value == "false":
        return False
    return None


def _put_code(summary: ET.Element) -> int | None:
    pc = summary.attrib.get("put-code")
    return int(pc) if pc and pc.isdigit() else None


def _org(summary: ET.Element) -> dict[str, str | None]:
    """The shared ``organization`` block -> the six org columns (always all present)."""
    org = _child(summary, "organization")
    addr = _child(org, "address")
    dis = _child(org, "disambiguated-organization")
    return {
        "organization_name": _path(org, "name"),
        "organization_city": _path(addr, "city"),
        "organization_region": _path(addr, "region"),
        "organization_country": _path(addr, "country"),
        "disambiguated_organization_identifier": _path(dis, "disambiguated-organization-identifier"),
        "disambiguation_source": _path(dis, "disambiguation-source"),
    }


def _summaries(section: ET.Element | None, summary_name: str) -> list[ET.Element]:
    """All ``<*-summary>`` elements within an activity section (skips the group
    wrappers). Safe to scan by local name because a section holds only one type."""
    if section is None:
        return []
    return [el for el in section.iter() if _ln(el) == summary_name]


def parse_bytes(data: bytes) -> ET.Element | None:
    """Parse one record's XML bytes into its root element, or None if unparseable.

    Retries once with illegal control bytes stripped before giving up."""
    try:
        return ET.fromstring(data)
    except ET.ParseError:
        cleaned = _ILLEGAL_XML.sub(b"", data)
        try:
            return ET.fromstring(cleaned)
        except ET.ParseError:
            return None


def parse_record(root: ET.Element) -> dict[str, list[dict]] | None:
    """Flatten one ``<record:record>`` into a dict of row lists keyed by table name.

    Returns None for a document that is not a record or carries no ORCID iD (e.g. a
    deactivated/locked stub), so the caller can count and skip it.
    """
    if _ln(root) != "record":
        return None
    orcid = _path(root, "orcid-identifier", "path") or root.attrib.get("path", "").lstrip("/")
    if not orcid:
        return None

    out: dict[str, list[dict]] = {t: [] for t in _TABLES}

    history = _child(root, "history")
    person = _child(root, "person")
    name = _child(person, "name")
    out["records"].append({
        "orcid": orcid,
        "given_names": _path(name, "given-names"),
        "family_name": _path(name, "family-name"),
        "credit_name": _path(name, "credit-name"),
        "name_visibility": name.attrib.get("visibility") if name is not None else None,
        "locale": _path(root, "preferences", "locale"),
        "creation_method": _path(history, "creation-method"),
        "submission_date": _path(history, "submission-date"),
        "last_modified_date": _path(history, "last-modified-date"),
        "claimed": _bool(_path(history, "claimed")),
        "verified_email": _bool(_path(history, "verified-email")),
        "verified_primary_email": _bool(_path(history, "verified-primary-email")),
    })

    # Person-level lists. Rows whose required value is missing (ORCID occasionally
    # carries an empty <keyword/>, <other-name/>, address without a country, etc.)
    # are skipped rather than emitted as a null in a required column.
    for o in _children(_child(person, "other-names"), "other-name"):
        content = _path(o, "content")
        if content is not None:
            out["other_names"].append({"orcid": orcid, "content": content})
    for u in _children(_child(person, "researcher-urls"), "researcher-url"):
        url = _path(u, "url")
        if url is not None:
            out["researcher_urls"].append({"orcid": orcid, "url_name": _path(u, "url-name"), "url": url})
    for k in _children(_child(person, "keywords"), "keyword"):
        content = _path(k, "content")
        if content is not None:
            out["keywords"].append({"orcid": orcid, "content": content})
    for a in _children(_child(person, "addresses"), "address"):
        country = _path(a, "country")
        if country is not None:
            out["addresses"].append({"orcid": orcid, "country": country})
    for e in _children(_child(person, "external-identifiers"), "external-identifier"):
        id_type = _path(e, "external-id-type")
        if id_type is None:
            continue
        out["person_external_identifiers"].append({
            "orcid": orcid,
            "external_id_type": id_type,
            "external_id_value": _path(e, "external-id-value"),
            "external_id_url": _path(e, "external-id-url"),
            "external_id_relationship": _path(e, "external-id-relationship"),
        })

    activities = _child(root, "activities-summary")

    # Seven affiliation sections share one schema, unified by affiliation_type.
    for section_name, affiliation_type in AFFILIATION_SECTIONS:
        section = _child(activities, section_name)
        for summ in _summaries(section, f"{affiliation_type}-summary"):
            out["affiliations"].append({
                "orcid": orcid,
                "affiliation_type": affiliation_type,
                "put_code": _put_code(summ),
                "department_name": _path(summ, "department-name"),
                "role_title": _path(summ, "role-title"),
                "start_date": _date(_child(summ, "start-date")),
                "end_date": _date(_child(summ, "end-date")),
                **_org(summ),
            })

    for summ in _summaries(_child(activities, "fundings"), "funding-summary"):
        out["fundings"].append({
            "orcid": orcid,
            "put_code": _put_code(summ),
            "title": _path(summ, "title", "title"),
            "funding_type": _path(summ, "type"),
            "start_date": _date(_child(summ, "start-date")),
            "end_date": _date(_child(summ, "end-date")),
            **_org(summ),
        })

    for summ in _summaries(_child(activities, "works"), "work-summary"):
        put_code = _put_code(summ)
        out["works"].append({
            "orcid": orcid,
            "put_code": put_code,
            "title": _path(summ, "title", "title"),
            "subtitle": _path(summ, "title", "subtitle"),
            "work_type": _path(summ, "type"),
            "journal_title": _path(summ, "journal-title"),
            "publication_date": _date(_child(summ, "publication-date")),
            "url": _path(summ, "url"),
            "source_name": _path(_child(summ, "source"), "source-name"),
        })
        for eid in _children(_child(summ, "external-ids"), "external-id"):
            id_type = _path(eid, "external-id-type")
            if id_type is None:
                continue
            out["work_external_identifiers"].append({
                "orcid": orcid,
                "put_code": put_code,
                "external_id_type": id_type,
                "external_id_value": _path(eid, "external-id-value"),
                "external_id_normalized": _path(eid, "external-id-normalized"),
                "external_id_relationship": _path(eid, "external-id-relationship"),
            })

    return out
