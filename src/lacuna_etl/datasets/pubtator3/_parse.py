"""Streaming parser for PubTator3 BioC-XML tar.gz archives.

Each `BioCXML.<n>.tar.gz` holds many members `output/BioCXML/<m>.BioC.XML`, and
each member is a BioC `<collection>` of `<document>` elements. One `<document>`
is one article: `<id>` is the PMID, and it contains a list of `<passage>`s, each
with `<infon>` metadata, an `<offset>`, body `<text>` (which we drop), and
`<annotation>` children. `<relation>` elements sit directly under the document.

`iter_documents` streams `<document>` elements across every member of one
archive with `ElementTree.iterparse`, clearing each element after the consumer
returns so memory stays bounded by a single document.

`parse_document` flattens one `<document>` into a dict of row lists, one list per
output table, with identifiers (DOI, PMID) normalised so the shapes match the
column specs in `_schemas`.
"""

from __future__ import annotations

import io
import re
import sys
import tarfile
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Iterator

from lacuna_etl.core.identifiers import Doi, PubmedId


_DOI_WHITESPACE = re.compile(r"\s+")

# Characters illegal in XML 1.0 (the C0 controls other than tab, LF, CR). A few
# PubTator records contain stray control bytes; stripping them lets an otherwise
# well-formed document parse.
_ILLEGAL_XML = re.compile(rb"[\x00-\x08\x0b\x0c\x0e-\x1f]")
_DOC_END = b"</document>"

# Passage `type` carrying the article's own bibliographic metadata: `front` for
# full-text (PMC) records, `title` for abstract-only (PubMed) records.
_META_PASSAGE_TYPES = {"front", "title"}

# Tables emitted per document; kept in sync with POLARS_SCHEMAS keys.
_TABLES = ("articles", "annotations", "relations")


def _clean_doi(value: str | None) -> str | None:
    """Strip URL prefix + internal whitespace, reject values that aren't real DOIs.

    Mirrors `pubmed/_parse.py::_clean_doi`: the Doi column validator requires
    '10.<registrant>/<id>', so anything that doesn't fit that shape is dropped to
    null rather than failing the run.
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


def _normalize_pmc(value: str | None) -> str | None:
    """Canonicalise a PMC id to 'PMC<digits>'. Bare digits gain the prefix."""
    if value is None:
        return None
    s = value.strip()
    if not s:
        return None
    if s.isdigit():
        return f"PMC{s}"
    return s


def _infon(passage: ET.Element, key: str) -> str | None:
    """Value of a passage/annotation/relation `<infon key=..>`, stripped; '' -> None."""
    el = passage.find(f"infon[@key='{key}']")
    if el is None or el.text is None:
        return None
    s = el.text.strip()
    return s or None


def _parse_int(s: str | None) -> int | None:
    if s is None:
        return None
    try:
        return int(s)
    except (TypeError, ValueError):
        return None


def _parse_float(s: str | None) -> float | None:
    if s is None:
        return None
    try:
        return float(s)
    except (TypeError, ValueError):
        return None


def _split_role(value: str | None) -> tuple[str | None, str | None]:
    """Split a relation role 'EntityType|ConceptId' on the FIRST '|'.

    Concept ids for variants contain '|' themselves (e.g.
    'ProteinMutation|tmVar:p|SUB|G|177|D;...'), so only the first separator
    delimits the entity type from the identifier.
    """
    if value is None:
        return None, None
    head, sep, tail = value.partition("|")
    if not sep:
        return (head or None), None
    return (head or None), (tail or None)


def parse_document(elem: ET.Element) -> dict[str, list[dict]]:
    """Flatten one BioC <document> into row dicts grouped by output table.

    A small fraction of documents are PMC full-text articles keyed by a `PMC…`
    id with no PMID anywhere in the record. This dataset is keyed on PMID (the
    join key to PubMed/OpenAlex/iCite), so such documents are skipped — returning
    empty row lists; the pipeline counts and reports them.
    """
    raw_id = (elem.findtext("id") or "").strip()
    pmid = PubmedId.parse(raw_id) if raw_id.isdigit() else None
    if pmid is None:
        return {t: [] for t in _TABLES}

    passages = elem.findall("passage")

    # Article-level metadata comes only from the front/title passage, never from
    # `ref` passages (those carry the *cited* work's metadata). Fall back to the
    # first passage if no front/title is present.
    meta_passage: ET.Element | None = None
    for p in passages:
        if _infon(p, "type") in _META_PASSAGE_TYPES:
            meta_passage = p
            break
    if meta_passage is None and passages:
        meta_passage = passages[0]

    def m(key: str) -> str | None:
        return _infon(meta_passage, key) if meta_passage is not None else None

    has_full_text = any(_infon(p, "type") == "paragraph" for p in passages)

    article_row = {
        "pmid":          pmid,
        "pmc_id":        _normalize_pmc(m("article-id_pmc")),
        "doi":           _clean_doi(m("article-id_doi")),
        "year":          _parse_int(m("year")),
        "volume":        m("volume"),
        "issue":         m("issue"),
        "first_page":    m("fpage"),
        "last_page":     m("lpage"),
        "license":       m("license"),
        "has_full_text": has_full_text,
        "n_passages":    len(passages),
    }

    annotation_rows: list[dict] = []
    for p in passages:
        # Skip entities tagged inside the cited-reference list.
        if _infon(p, "type") == "ref":
            continue
        section_type = _infon(p, "section_type")
        passage_type = _infon(p, "type")
        for ann in p.findall("annotation"):
            loc = ann.find("location")
            identifier = _infon(ann, "identifier")
            if identifier == "-":
                identifier = None
            text_el = ann.find("text")
            annotation_rows.append({
                "pmid":          pmid,
                "annotation_id": ann.get("id"),
                "section_type":  section_type,
                "passage_type":  passage_type,
                "offset":        _parse_int(loc.get("offset")) if loc is not None else None,
                "length":        _parse_int(loc.get("length")) if loc is not None else None,
                "mention":       (text_el.text.strip() if text_el is not None and text_el.text else None),
                "entity_type":   _infon(ann, "type"),
                "identifier":    identifier,
            })

    relation_rows: list[dict] = []
    for rel in elem.findall("relation"):
        role1_type, role1_id = _split_role(_infon(rel, "role1"))
        role2_type, role2_id = _split_role(_infon(rel, "role2"))
        relation_rows.append({
            "pmid":             pmid,
            "relation_id":      rel.get("id"),
            "relation_type":    _infon(rel, "type"),
            "score":            _parse_float(_infon(rel, "score")),
            "role1_type":       role1_type,
            "role1_identifier": role1_id,
            "role2_type":       role2_type,
            "role2_identifier": role2_id,
        })

    return {
        "articles":    [article_row],
        "annotations": annotation_rows,
        "relations":   relation_rows,
    }


def _recover_documents(data: bytes) -> Iterator[ET.Element]:
    """Yield parseable <document> elements from a member by segmenting on tags.

    Fallback for a member that `iterparse` rejects as not well-formed (a rare
    PubTator record is corrupt — e.g. a run of NUL bytes that clobbers a
    document's tags). Each `<document>…</document>` span is parsed on its own with
    illegal control chars stripped, so one corrupt document only drops itself
    instead of aborting the whole member. Yields documents in source order.
    """
    pos = 0
    while True:
        start = data.find(b"<document>", pos)
        if start == -1:
            return
        end = data.find(_DOC_END, start)
        if end == -1:
            return
        end += len(_DOC_END)
        pos = end
        chunk = _ILLEGAL_XML.sub(b"", data[start:end])
        try:
            yield ET.fromstring(chunk)
        except ET.ParseError:
            continue  # genuinely corrupt document; drop just this one


def iter_documents(tar_path: Path) -> Iterator[ET.Element]:
    """Stream <document> elements from every member of one BioCXML tar.gz.

    The archive is read in streaming mode (`r|gz`, no random access). Each member
    is a BioC <collection>; we read it into memory (members are ~10-15 MB),
    iterparse it, and yield each <document>, clearing it after the consumer
    returns so peak memory stays bounded by a single member.

    If a member is not well-formed, we fall back to per-document recovery
    (`_recover_documents`), skipping the documents already yielded so none are
    emitted twice, and log a warning naming the member.
    """
    with tarfile.open(tar_path, "r|gz") as tf:
        for member in tf:
            if not member.isfile():
                continue
            fileobj = tf.extractfile(member)
            if fileobj is None:
                continue
            data = fileobj.read()

            yielded = 0
            try:
                for _event, el in ET.iterparse(io.BytesIO(data), events=("end",)):
                    if el.tag == "document":
                        yield el
                        yielded += 1
                        el.clear()
                continue
            except ET.ParseError as e:
                print(
                    f"WARNING [{tar_path.name}:{member.name}] not well-formed "
                    f"({e}); recovering documents individually",
                    file=sys.stderr,
                )

            recovered = 0
            for el in _recover_documents(data):
                recovered += 1
                if recovered <= yielded:
                    continue  # already emitted before the parse failure
                yield el
            print(
                f"WARNING [{tar_path.name}:{member.name}] recovered {recovered} "
                f"documents ({recovered - yielded} after the failure point)",
                file=sys.stderr,
            )
