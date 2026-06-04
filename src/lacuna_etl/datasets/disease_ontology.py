"""Human Disease Ontology (DO), the standardized ontology of human disease.

The DO ships a single OBO-format file (``doid.obo``): a flat sequence of
``[Term]`` stanzas (plus two ``[Typedef]`` relation declarations we ignore), each
a set of ``key: value`` tag lines. At ~7 MB / ~14.7k terms it fits comfortably in
memory, so this is an in-memory pandas pipeline: ``extract`` walks the file once
into per-table row lists and writes one intermediate Parquet per table,
``transform`` is a no-op (faithful projection), and ``load`` validates against
each ``SCHEMA`` and writes final Parquet + sidecar.

The native term ID is the ``DOID:`` curie, typed ``DiseaseOntologyId``; it keys
the parent ``terms`` table and every edge that references a term. The ontology's
external cross-references (``xref`` and the SKOS ``*Match`` mappings — MESH,
UMLS_CUI, ORDO, MIM, SNOMEDCT, …) are heterogeneous CURIEs with no single
canonical form, so they stay documented plain strings (the Open Targets
disease-ID precedent). DO uses only ``is_a`` for its term graph (no ``relationship``
stanzas), so ``term_parents`` is the single edge table; obsolete terms carry their
``replaced_by`` / ``consider`` pointers in their own small tables. See
docs/DESIGN.md for the full table inventory.
"""
from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from lacuna_etl.core.identifiers import DiseaseOntologyId
from lacuna_etl.core.pipeline import DatasetPipeline
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.registry import register

_SYNONYM_SCOPES = {"EXACT", "BROAD", "NARROW", "RELATED"}
_SKOS_MATCHES = {"exactMatch", "broadMatch", "closeMatch", "narrowMatch", "relatedMatch"}

# --------------------------------------------------------------------------
# Parent: one row per term.
# --------------------------------------------------------------------------
TERMS_SCHEMA = {
    "doid": ColumnSpec(identifier=DiseaseOntologyId, required=True, description="Disease Ontology term ID (the OBO 'id' tag)"),
    "name": ColumnSpec(required=True, description="Primary term name (obsolete terms are named 'obsolete <name>')"),
    "definition": ColumnSpec(description="Textual definition (the OBO 'def' tag, provenance refs dropped); null where the term carries no definition"),
    "is_obsolete": ColumnSpec(required=True, description="Whether the term is obsolete (the OBO 'is_obsolete' tag; False when absent)"),
    "comment": ColumnSpec(description="Curator comment / usage note"),
    "created_by": ColumnSpec(description="Curator who created the term"),
    "creation_date": ColumnSpec(description="UTC timestamp the term was created"),
}

# --------------------------------------------------------------------------
# Edge / multi-valued child tables, each keyed by the parent DOID.
# --------------------------------------------------------------------------
TERM_ALT_IDS_SCHEMA = {
    "doid": ColumnSpec(identifier=DiseaseOntologyId, required=True, description="Disease Ontology term ID"),
    "alt_id": ColumnSpec(identifier=DiseaseOntologyId, required=True, description="A secondary/merged DOID that resolves to this term (the OBO 'alt_id' tag)"),
}

TERM_PARENTS_SCHEMA = {
    "doid": ColumnSpec(identifier=DiseaseOntologyId, required=True, description="Disease Ontology term ID (the child)"),
    "parent_id": ColumnSpec(identifier=DiseaseOntologyId, required=True, description="DOID of a direct is_a parent term"),
}

TERM_SYNONYMS_SCHEMA = {
    "doid": ColumnSpec(identifier=DiseaseOntologyId, required=True, description="Disease Ontology term ID"),
    "synonym": ColumnSpec(required=True, description="A synonym string for the term"),
    "scope": ColumnSpec(allowed_values=_SYNONYM_SCOPES, required=True, description="Synonym scope: EXACT, BROAD, NARROW, or RELATED"),
    "synonym_type": ColumnSpec(description="Synonym type curie when given (e.g. OMO:0003012 = acronym), else null"),
}

TERM_XREFS_SCHEMA = {
    "doid": ColumnSpec(identifier=DiseaseOntologyId, required=True, description="Disease Ontology term ID"),
    "xref": ColumnSpec(required=True, description="A cross-reference CURIE to an external vocabulary (e.g. MESH:D006394, UMLS_CUI:C0018923, ORDO:99825); heterogeneous, kept verbatim"),
}

TERM_SUBSETS_SCHEMA = {
    "doid": ColumnSpec(identifier=DiseaseOntologyId, required=True, description="Disease Ontology term ID"),
    "subset": ColumnSpec(required=True, description="A DO subset/slim the term belongs to (e.g. DO_cancer_slim, DO_rare_slim)"),
}

TERM_SKOS_MATCHES_SCHEMA = {
    "doid": ColumnSpec(identifier=DiseaseOntologyId, required=True, description="Disease Ontology term ID"),
    "match_type": ColumnSpec(allowed_values=_SKOS_MATCHES, required=True, description="SKOS mapping predicate: exactMatch, broadMatch, closeMatch, narrowMatch, or relatedMatch"),
    "match_id": ColumnSpec(required=True, description="The matched external CURIE (heterogeneous: MESH, UMLS_CUI, ORDO, MIM, SNOMEDCT, …); kept verbatim"),
}

TERM_DISJOINT_FROM_SCHEMA = {
    "doid": ColumnSpec(identifier=DiseaseOntologyId, required=True, description="Disease Ontology term ID"),
    "disjoint_from_id": ColumnSpec(identifier=DiseaseOntologyId, required=True, description="DOID of a term declared disjoint from this one"),
}

TERM_REPLACED_BY_SCHEMA = {
    "doid": ColumnSpec(identifier=DiseaseOntologyId, required=True, description="Obsolete Disease Ontology term ID"),
    "replaced_by_id": ColumnSpec(identifier=DiseaseOntologyId, required=True, description="DOID of the term that definitively replaces this obsolete term"),
}

TERM_CONSIDER_SCHEMA = {
    "doid": ColumnSpec(identifier=DiseaseOntologyId, required=True, description="Obsolete Disease Ontology term ID"),
    "consider_id": ColumnSpec(identifier=DiseaseOntologyId, required=True, description="DOID of a term suggested for consideration in place of this obsolete term"),
}

# OBO value parsing -----------------------------------------------------------
# `def: "text" [refs] {modifiers}` and `synonym: "text" SCOPE [TYPE] [refs] {mods}`.
# The DO file has no escaped quotes, so the quoted text never contains a `"`
# internally; capture it non-greedily (the text itself may contain `[...]`, and the
# trailing dbxref list / `{...}` modifier block — which we ignore — may contain `"`).
_DEF_RE = re.compile(r'^"(.*?)"')
_SYNONYM_RE = re.compile(
    r'^"(?P<text>.*?)"\s+(?P<scope>EXACT|BROAD|NARROW|RELATED)(?:\s+(?P<type>[^\s\[]+))?'
)
# Some synonyms carry their type not as the standard token but as a trailing OBO
# modifier `{synonymtypedef="<label>"}`; the header `synonymtypedef:` lines map that
# label back to its curie, so both encodings land as the same curie.
_SYN_TYPEDEF_RE = re.compile(r'^synonymtypedef:\s+(?P<curie>\S+)\s+"(?P<label>[^"]*)"')
_SYN_MODIFIER_RE = re.compile(r'synonymtypedef="(?P<label>[^"]+)"')
# `property_value: <predicate> "<curie>" xsd:string` (SKOS mappings).
_PROP_RE = re.compile(r'^(?P<pred>\S+)\s+"(?P<value>[^"]*)"')


def _ref(value: str) -> str:
    """Strip a trailing OBO ' ! label' comment from an ID-reference value."""
    return value.split(" ! ", 1)[0].strip()


@register
class DiseaseOntology(DatasetPipeline):
    name = "disease_ontology"

    _TABLES = [
        ("terms", TERMS_SCHEMA),
        ("term_alt_ids", TERM_ALT_IDS_SCHEMA),
        ("term_parents", TERM_PARENTS_SCHEMA),
        ("term_synonyms", TERM_SYNONYMS_SCHEMA),
        ("term_xrefs", TERM_XREFS_SCHEMA),
        ("term_subsets", TERM_SUBSETS_SCHEMA),
        ("term_skos_matches", TERM_SKOS_MATCHES_SCHEMA),
        ("term_disjoint_from", TERM_DISJOINT_FROM_SCHEMA),
        ("term_replaced_by", TERM_REPLACED_BY_SCHEMA),
        ("term_consider", TERM_CONSIDER_SCHEMA),
    ]

    def _obo_file(self) -> Path:
        matches = sorted(self.raw_path().glob("*.obo"))
        if len(matches) != 1:
            raise FileNotFoundError(f"expected exactly one *.obo file under {self.raw_path()}, found {len(matches)}")
        return matches[0]

    def _flush_term(self, tags: dict[str, list[str]]) -> None:
        """Turn one accumulated [Term] stanza's tags into output rows."""
        doid = tags["id"][0]
        self._terms.append({
            "doid": doid,
            "name": tags["name"][0],
            "definition": self._parse_def(tags.get("def")),
            "is_obsolete": tags.get("is_obsolete", ["false"])[0] == "true",
            "comment": tags["comment"][0] if "comment" in tags else None,
            "created_by": tags["created_by"][0] if "created_by" in tags else None,
            "creation_date": tags["creation_date"][0] if "creation_date" in tags else None,
        })
        for v in tags.get("alt_id", []):
            self._alt_ids.append({"doid": doid, "alt_id": _ref(v)})
        for v in tags.get("is_a", []):
            self._parents.append({"doid": doid, "parent_id": _ref(v)})
        for v in tags.get("subset", []):
            self._subsets.append({"doid": doid, "subset": v})
        for v in tags.get("xref", []):
            self._xrefs.append({"doid": doid, "xref": v})
        for v in tags.get("disjoint_from", []):
            self._disjoint.append({"doid": doid, "disjoint_from_id": _ref(v)})
        for v in tags.get("replaced_by", []):
            self._replaced.append({"doid": doid, "replaced_by_id": _ref(v)})
        for v in tags.get("consider", []):
            self._consider.append({"doid": doid, "consider_id": _ref(v)})
        for v in tags.get("synonym", []):
            m = _SYNONYM_RE.match(v)
            if not m:
                raise ValueError(f"{doid}: unparseable synonym line: {v!r}")
            syn_type = m["type"]
            if syn_type is None:
                mod = _SYN_MODIFIER_RE.search(v)
                if mod:
                    syn_type = self._syn_typedefs.get(mod["label"], mod["label"])
            self._synonyms.append({
                "doid": doid,
                "synonym": m["text"],
                "scope": m["scope"],
                "synonym_type": syn_type,
            })
        for v in tags.get("property_value", []):
            m = _PROP_RE.match(v)
            if m and m["pred"] in _SKOS_MATCHES:
                self._skos.append({"doid": doid, "match_type": m["pred"], "match_id": m["value"]})

    @staticmethod
    def _parse_def(values: list[str] | None) -> str | None:
        if not values:
            return None
        m = _DEF_RE.match(values[0])
        return m.group(1) if m else values[0] or None

    def extract(self) -> None:
        self._terms: list[dict] = []
        self._alt_ids: list[dict] = []
        self._parents: list[dict] = []
        self._synonyms: list[dict] = []
        self._xrefs: list[dict] = []
        self._subsets: list[dict] = []
        self._skos: list[dict] = []
        self._disjoint: list[dict] = []
        self._replaced: list[dict] = []
        self._consider: list[dict] = []
        self._syn_typedefs: dict[str, str] = {}  # synonym-type label -> curie (from header)

        tags: dict[str, list[str]] = {}
        in_term = False
        with open(self._obo_file(), encoding="utf-8") as fh:
            for raw in fh:
                line = raw.rstrip("\n")
                if line.startswith("["):
                    if in_term and tags:
                        self._flush_term(tags)
                    in_term = line == "[Term]"
                    tags = {}
                    continue
                if not in_term:
                    td = _SYN_TYPEDEF_RE.match(line)  # header synonymtypedef declarations
                    if td:
                        self._syn_typedefs[td["label"]] = td["curie"]
                if not in_term or not line or ":" not in line:
                    continue
                key, _, value = line.partition(":")
                tags.setdefault(key.strip(), []).append(value.strip())
            if in_term and tags:
                self._flush_term(tags)

        outputs = {
            "terms": self._terms,
            "term_alt_ids": self._alt_ids,
            "term_parents": self._parents,
            "term_synonyms": self._synonyms,
            "term_xrefs": self._xrefs,
            "term_subsets": self._subsets,
            "term_skos_matches": self._skos,
            "term_disjoint_from": self._disjoint,
            "term_replaced_by": self._replaced,
            "term_consider": self._consider,
        }
        for stem, rows in outputs.items():
            self.save_parquet(pd.DataFrame(rows, columns=list(dict(self._TABLES)[stem])), self.intermediate_path() / f"{stem}.parquet")
        print(
            f"[{self.name}] terms: {len(self._terms):,} "
            f"({sum(t['is_obsolete'] for t in self._terms):,} obsolete), "
            f"parents: {len(self._parents):,}, synonyms: {len(self._synonyms):,}, "
            f"xrefs: {len(self._xrefs):,}, skos_matches: {len(self._skos):,}"
        )

    def transform(self) -> None:
        # Faithful projection; parsing happens in extract, typing/validation in load.
        pass

    def load(self) -> None:
        for stem, schema in self._TABLES:
            df = self.load_parquet(self.intermediate_path() / f"{stem}.parquet")
            df = df[list(schema)]
            if stem == "terms":
                df["is_obsolete"] = df["is_obsolete"].astype(bool)
                df["creation_date"] = pd.to_datetime(df["creation_date"], format="ISO8601", utc=True).dt.tz_localize(None)
            df = self.apply_schema(df, schema)
            self.save_parquet(df, self.output_path() / f"{stem}.parquet")
            self.save_schema_yaml(schema, stem)
