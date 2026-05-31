"""Namespace synonyms and CURIE normalization for Harmonizome KG targets.

The Harmonizome KG `nodes.tsv` files declare a `namespace` per node, but spellings
are inconsistent across collections ("GO" vs "Gene Ontology", "DO" vs "DOID" vs
"Disease Ontology", etc.) and identifier forms are mixed ("GO_0070060" alongside
"GO:0050830"). This module produces:

  - `resolve_namespace(ns)` : str   -- collapses synonyms to a single label
  - `normalize_target_id(ns, raw)` : str
      -- for namespaces in `_CURIE_PREFIXES`, emits canonical `PREFIX:ID` CURIEs;
         for everything else, returns the raw KG string unchanged.
"""

from __future__ import annotations

import re

# Synonym -> canonical namespace label. Anything not present passes through.
_SYNONYMS: dict[str, str] = {
    "Gene Ontology": "GO",
    "Disease Ontology": "DO",
    "DOID": "DO",
    "Cell Ontology": "CL",
    "Provisional Cell Ontology": "PCL",
    "HPO": "HP",
    "MPO": "MP",
    "SNOMED CT": "SNOMED",
    "Pubchem": "PubChem",
    "Allen Brain Atlas Aging Brain Map": "ABA-Aging",
    "LINCS L1000 CMAP": "BRD",
}

# Canonical-namespace -> CURIE prefix to emit. Members of this map undergo
# `<PREFIX>_<id>` / `<PREFIX>:<id>` -> `<curie>:<id>` rewriting; the bare-numeric
# form (e.g. PubChem CIDs) gets the prefix prepended.
_CURIE_PREFIXES: dict[str, str] = {
    "GO": "GO",
    "HP": "HP",
    "MP": "MP",
    "CL": "CL",
    "PCL": "PCL",
    "MONDO": "MONDO",
    "DO": "DOID",
    "EFO": "EFO",
    "UBERON": "UBERON",
    "BTO": "BTO",
    "MeSH": "MeSH",
    "OMIM": "OMIM",
    "MedGen": "MedGen",
    "Orphanet": "Orphanet",
    "UMLS": "UMLS",
    "SNOMED": "SNOMED",
    "PubChem": "PubChem",
    "JASPAR": "JASPAR",
    "DepMap": "DepMap",
    "BRD": "BRD",
}

# Some IDs arrive with redundant inner prefixes ("Orphanet_ORPHA51083"); strip them.
_INNER_PREFIX_STRIP: dict[str, re.Pattern[str]] = {
    "Orphanet": re.compile(r"^ORPHA"),
}


def resolve_namespace(ns: str | None) -> str:
    """Return a canonical namespace label. Empty/'gene' map to 'unknown'."""
    if ns is None:
        return "unknown"
    ns = ns.strip()
    if not ns or ns.lower() in {"gene", "kinase"}:
        return "unknown"
    return _SYNONYMS.get(ns, ns)


def normalize_target_id(namespace: str, raw_id: str) -> str:
    """Normalize a KG target identifier to canonical CURIE form when applicable.

    For namespaces in `_CURIE_PREFIXES`, strips a leading `<NAMESPACE>_` /
    `<NAMESPACE>:` prefix (case-insensitive on the prefix), removes any inner
    redundant prefix (e.g. `ORPHA` for Orphanet), and re-emits as `PREFIX:<id>`.
    For all other namespaces, returns `raw_id` unchanged.
    """
    if raw_id is None:
        return raw_id
    prefix = _CURIE_PREFIXES.get(namespace)
    if prefix is None:
        return raw_id

    rid = raw_id.strip()
    # Strip the leading namespace token if present (handle both `_` and `:` and
    # synonyms like MESH_/SNOMED CT_).
    for tag in {namespace, prefix, *_synonyms_pointing_to(namespace)}:
        for sep in ("_", ":", " "):
            head = f"{tag}{sep}"
            if rid.startswith(head):
                rid = rid[len(head):]
                break
    inner = _INNER_PREFIX_STRIP.get(namespace)
    if inner is not None:
        rid = inner.sub("", rid)
    return f"{prefix}:{rid}"


def _synonyms_pointing_to(canonical: str) -> set[str]:
    return {src for src, dst in _SYNONYMS.items() if dst == canonical}
