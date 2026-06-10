"""Gene Ontology (the ``go-basic`` release), the standard ontology of gene function.

GO ships ``go-basic.obo``: the filtered, cycle-free release that keeps only the
relationships safe for propagation (``is_a``, ``part_of``, and the three
``regulates`` variants) and whose edges never leave the ontology. It is a flat
sequence of ``[Term]`` stanzas of ``key: value`` tag lines. At ~31 MB / ~48k terms
it fits in memory, so this is an in-memory pandas pipeline of the same shape as
``disease_ontology``: ``extract`` walks the file once into per-table row lists,
``transform`` is a no-op (faithful projection), and ``load`` validates against each
``SCHEMA`` and writes final Parquet + sidecar.

The native term ID is the ``GO:`` curie, typed ``GoId``; it keys the parent ``terms``
table and every GO-referencing edge (``term_parents`` is the ``is_a`` hierarchy,
``term_relationships`` the typed ``part_of``/``regulates`` edges). Each term carries a
``namespace`` — ``molecular_function``, ``biological_process``, ``cellular_component``,
or ``external`` (terms imported from other ontologies). The heterogeneous external
cross-references (``xref`` Reactome/EC/MetaCyc CURIEs, the SKOS ``skos:*Match``
mappings) have no single canonical form and stay documented plain strings (the
disease_ontology / Open Targets precedent). See docs/DESIGN.md for the table inventory.
"""
from __future__ import annotations

import re

import pandas as pd

from lacuna_etl.core.identifiers import GoId
from lacuna_etl.core.obo import find_obo_file, iter_stanzas, ref
from lacuna_etl.core.pipeline import DatasetPipeline
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.registry import register

_SYNONYM_SCOPES = {"EXACT", "BROAD", "NARROW", "RELATED"}
_NAMESPACES = {"molecular_function", "biological_process", "cellular_component", "external"}
# go-basic keeps only these typed (non-is_a) relations; a free string in case a
# release adds one, so a new relation type does not abort the run.
_SKOS_MATCHES = {"exactMatch", "broadMatch", "closeMatch", "narrowMatch", "relatedMatch"}

# --------------------------------------------------------------------------
# Parent: one row per term.
# --------------------------------------------------------------------------
TERMS_SCHEMA = {
    "go_id": ColumnSpec(identifier=GoId, required=True, description="GO term ID (the OBO 'id' tag); the grain key"),
    "name": ColumnSpec(required=True, description="Primary term name (obsolete terms are named 'obsolete <name>')"),
    "namespace": ColumnSpec(allowed_values=_NAMESPACES, required=True, description="GO aspect: molecular_function, biological_process, cellular_component, or external (imported term)"),
    "definition": ColumnSpec(description="Textual definition (the OBO 'def' tag, provenance refs dropped); null where absent"),
    "is_obsolete": ColumnSpec(required=True, description="Whether the term is obsolete (the OBO 'is_obsolete' tag; False when absent)"),
    "comment": ColumnSpec(description="Curator comment / usage note"),
}

# --------------------------------------------------------------------------
# Edge / multi-valued child tables, each keyed by the parent GO id.
# --------------------------------------------------------------------------
TERM_ALT_IDS_SCHEMA = {
    "go_id": ColumnSpec(identifier=GoId, required=True, description="GO term ID"),
    "alt_id": ColumnSpec(identifier=GoId, required=True, description="A secondary/merged GO id that resolves to this term (the OBO 'alt_id' tag)"),
}

TERM_PARENTS_SCHEMA = {
    "go_id": ColumnSpec(identifier=GoId, required=True, description="GO term ID (the child)"),
    "parent_id": ColumnSpec(identifier=GoId, required=True, description="GO id of a direct is_a parent term"),
}

TERM_RELATIONSHIPS_SCHEMA = {
    "go_id": ColumnSpec(identifier=GoId, required=True, description="GO term ID (the subject)"),
    "relation_type": ColumnSpec(required=True, description="The OBO relationship type (go-basic: part_of, regulates, positively_regulates, negatively_regulates); a documented free string"),
    "related_id": ColumnSpec(identifier=GoId, required=True, description="GO id of the related (object) term"),
}

TERM_SYNONYMS_SCHEMA = {
    "go_id": ColumnSpec(identifier=GoId, required=True, description="GO term ID"),
    "synonym": ColumnSpec(required=True, description="A synonym string for the term"),
    "scope": ColumnSpec(allowed_values=_SYNONYM_SCOPES, required=True, description="Synonym scope: EXACT, BROAD, NARROW, or RELATED"),
    "synonym_type": ColumnSpec(description="Synonym type label when given (e.g. systematic_synonym), else null"),
}

TERM_XREFS_SCHEMA = {
    "go_id": ColumnSpec(identifier=GoId, required=True, description="GO term ID"),
    "xref": ColumnSpec(required=True, description="A cross-reference CURIE to an external resource (e.g. Reactome:R-HSA-449718, EC:2.5.1.30, MetaCyc:…); heterogeneous, the trailing quoted label dropped"),
}

TERM_SUBSETS_SCHEMA = {
    "go_id": ColumnSpec(identifier=GoId, required=True, description="GO term ID"),
    "subset": ColumnSpec(required=True, description="A GO subset/slim the term belongs to (e.g. goslim_generic, gocheck_do_not_annotate)"),
}

TERM_SKOS_MATCHES_SCHEMA = {
    "go_id": ColumnSpec(identifier=GoId, required=True, description="GO term ID"),
    "match_type": ColumnSpec(allowed_values=_SKOS_MATCHES, required=True, description="SKOS mapping predicate (skos: prefix dropped): exactMatch, broadMatch, closeMatch, narrowMatch, or relatedMatch"),
    "match_id": ColumnSpec(required=True, description="The matched external CURIE (heterogeneous: EC, RHEA, CHEBI, …); kept verbatim"),
}

TERM_REPLACED_BY_SCHEMA = {
    "go_id": ColumnSpec(identifier=GoId, required=True, description="Obsolete GO term ID"),
    "replaced_by_id": ColumnSpec(identifier=GoId, required=True, description="GO id of the term that definitively replaces this obsolete term"),
}

TERM_CONSIDER_SCHEMA = {
    "go_id": ColumnSpec(identifier=GoId, required=True, description="Obsolete GO term ID"),
    # The 'consider' pointer is a non-authoritative curator suggestion, and the
    # release carries a malformed value (e.g. 'GO:000666', a dropped digit), so it
    # is kept as a documented plain string rather than a typed GoId.
    "consider_id": ColumnSpec(required=True, description="GO id (as written) of a term suggested in place of this obsolete term; an advisory pointer kept verbatim — the release contains a non-canonical value, so it is not typed GoId"),
}

# OBO value parsing -----------------------------------------------------------
_DEF_RE = re.compile(r'^"(.*?)"')
_SYNONYM_RE = re.compile(
    r'^"(?P<text>.*?)"\s+(?P<scope>EXACT|BROAD|NARROW|RELATED)(?:\s+(?P<type>[^\s\[]+))?'
)
# `property_value: skos:exactMatch EC:2.5.1.30` (the SKOS mappings; no quotes).
_SKOS_RE = re.compile(r"^skos:(?P<pred>\w+Match)\s+(?P<value>\S+)")


def _xref(value: str) -> str:
    """Keep just the CURIE token, dropping any trailing quoted description."""
    return value.split(" ", 1)[0].split('"', 1)[0].strip()


@register
class GeneOntologyBasic(DatasetPipeline):
    name = "geneontology_basic"

    _TABLES = [
        ("terms", TERMS_SCHEMA),
        ("term_alt_ids", TERM_ALT_IDS_SCHEMA),
        ("term_parents", TERM_PARENTS_SCHEMA),
        ("term_relationships", TERM_RELATIONSHIPS_SCHEMA),
        ("term_synonyms", TERM_SYNONYMS_SCHEMA),
        ("term_xrefs", TERM_XREFS_SCHEMA),
        ("term_subsets", TERM_SUBSETS_SCHEMA),
        ("term_skos_matches", TERM_SKOS_MATCHES_SCHEMA),
        ("term_replaced_by", TERM_REPLACED_BY_SCHEMA),
        ("term_consider", TERM_CONSIDER_SCHEMA),
    ]

    def _flush_term(self, tags: dict[str, list[str]]) -> None:
        """Turn one accumulated [Term] stanza's tags into output rows."""
        go_id = tags["id"][0]
        self._terms.append({
            "go_id": go_id,
            "name": tags["name"][0],
            "namespace": tags["namespace"][0] if "namespace" in tags else None,
            "definition": self._parse_def(tags.get("def")),
            "is_obsolete": tags.get("is_obsolete", ["false"])[0] == "true",
            "comment": tags["comment"][0] if "comment" in tags else None,
        })
        for v in tags.get("alt_id", []):
            self._alt_ids.append({"go_id": go_id, "alt_id": ref(v)})
        for v in tags.get("is_a", []):
            self._parents.append({"go_id": go_id, "parent_id": ref(v)})
        for v in tags.get("relationship", []):
            rel_type, _, rest = v.partition(" ")
            self._relationships.append({"go_id": go_id, "relation_type": rel_type, "related_id": ref(rest)})
        for v in tags.get("subset", []):
            self._subsets.append({"go_id": go_id, "subset": v})
        for v in tags.get("xref", []):
            self._xrefs.append({"go_id": go_id, "xref": _xref(v)})
        for v in tags.get("replaced_by", []):
            self._replaced.append({"go_id": go_id, "replaced_by_id": ref(v)})
        for v in tags.get("consider", []):
            self._consider.append({"go_id": go_id, "consider_id": ref(v)})
        for v in tags.get("synonym", []):
            m = _SYNONYM_RE.match(v)
            if not m:
                raise ValueError(f"{go_id}: unparseable synonym line: {v!r}")
            self._synonyms.append({
                "go_id": go_id,
                "synonym": m["text"],
                "scope": m["scope"],
                "synonym_type": m["type"],
            })
        for v in tags.get("property_value", []):
            m = _SKOS_RE.match(v)
            if m:
                self._skos.append({"go_id": go_id, "match_type": m["pred"], "match_id": m["value"]})

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
        self._relationships: list[dict] = []
        self._synonyms: list[dict] = []
        self._xrefs: list[dict] = []
        self._subsets: list[dict] = []
        self._skos: list[dict] = []
        self._replaced: list[dict] = []
        self._consider: list[dict] = []

        for stanza_type, tags in iter_stanzas(find_obo_file(self.raw_path())):
            if stanza_type == "Term" and tags:
                self._flush_term(tags)

        outputs = {
            "terms": self._terms,
            "term_alt_ids": self._alt_ids,
            "term_parents": self._parents,
            "term_relationships": self._relationships,
            "term_synonyms": self._synonyms,
            "term_xrefs": self._xrefs,
            "term_subsets": self._subsets,
            "term_skos_matches": self._skos,
            "term_replaced_by": self._replaced,
            "term_consider": self._consider,
        }
        for stem, rows in outputs.items():
            self.save_parquet(pd.DataFrame(rows, columns=list(dict(self._TABLES)[stem])), self.intermediate_path() / f"{stem}.parquet")
        print(
            f"[{self.name}] terms: {len(self._terms):,} "
            f"({sum(t['is_obsolete'] for t in self._terms):,} obsolete), "
            f"parents: {len(self._parents):,}, relationships: {len(self._relationships):,}, "
            f"synonyms: {len(self._synonyms):,}, xrefs: {len(self._xrefs):,}, skos: {len(self._skos):,}"
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
            df = self.apply_schema(df, schema)
            self.save_parquet(df, self.output_path() / f"{stem}.parquet")
            self.save_schema_yaml(schema, stem)
