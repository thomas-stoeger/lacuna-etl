"""OLS — the EMBL-EBI Ontology Lookup Service ontology dumps.

The snapshot is one ~10 GB tarball of 342 OLS4 "linked" JSON files, one per
ontology, totalling ~269 GB uncompressed (a single file, NCBITaxon, is 71 GB), so
this is a streaming pipeline: each ontology JSON is read once, straight out of the
tarball, with an ``ijson`` event router that reconstructs each entity (class,
property, individual) on the fly plus the ontology's scalar metadata — never
loading a whole file into memory. Output follows the OpenAlex sharded layout:
``<dataset>/<table>/<ontology>_<part>.parquet`` with one ``<table>.yml`` sidecar.

OLS4 wraps every annotation value as ``{"type": [...], "value": "..."}`` (or a list
thereof, or a bare string for IRIs); ``_vals`` unwraps any of these shapes. Each
ontology file also carries *imported* classes referenced from other ontologies, so
terms are tagged with `is_defining_ontology` for consumers that want only an
ontology's own terms. Identifiers (IRIs, CURIEs, short forms) are heterogeneous
across 342 ontologies with no single canonical form, so they are documented plain
strings (the PubTator/Open Targets precedent).

Eight tables: `ontologies`, `terms` (classes) with `term_parents` (direct
subClassOf edges), `term_synonyms`, `term_xrefs`, plus `properties` and
`individuals`.
"""
from __future__ import annotations

import tarfile
from pathlib import Path

import ijson
import pandas as pd
from ijson.common import ObjectBuilder

from lacuna_etl.core.pipeline import DatasetPipeline
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.registry import register

_SYNONYM_TYPES = {"exact", "related", "narrow", "broad"}

ONTOLOGIES_SCHEMA = {
    "ontology_id": ColumnSpec(description="OLS ontology ID (lowercase short name, e.g. 'go', 'ceph')", required=True),
    "iri": ColumnSpec(description="Ontology IRI"),
    "title": ColumnSpec(description="Ontology title"),
    "description": ColumnSpec(description="Ontology description"),
    "homepage": ColumnSpec(description="Ontology homepage URL"),
    "version_iri": ColumnSpec(description="Version IRI of the loaded release"),
    "num_classes": ColumnSpec(description="Number of classes the ontology declares"),
    "num_properties": ColumnSpec(description="Number of properties the ontology declares"),
    "num_individuals": ColumnSpec(description="Number of individuals the ontology declares"),
    "is_obsolete": ColumnSpec(description="Whether OLS marks the ontology obsolete"),
}

TERMS_SCHEMA = {
    "ontology_id": ColumnSpec(description="OLS ontology this row comes from", required=True),
    "iri": ColumnSpec(description="Term (class) IRI", required=True),
    "curie": ColumnSpec(description="Compact identifier, e.g. 'GO:0008150'"),
    "short_form": ColumnSpec(description="Short form, e.g. 'GO_0008150'"),
    "label": ColumnSpec(description="Primary label (first rdfs:label)"),
    "definition": ColumnSpec(description="Definition (first definition annotation)"),
    "is_obsolete": ColumnSpec(description="Whether the term is obsolete"),
    "is_defining_ontology": ColumnSpec(description="Whether this ontology is the term's defining ontology (false for imported/referenced terms)"),
}

TERM_PARENTS_SCHEMA = {
    "ontology_id": ColumnSpec(description="OLS ontology this row comes from", required=True),
    "term_iri": ColumnSpec(description="Child term IRI", required=True),
    "parent_iri": ColumnSpec(description="Direct parent (subClassOf) term IRI", required=True),
}

TERM_SYNONYMS_SCHEMA = {
    "ontology_id": ColumnSpec(description="OLS ontology this row comes from", required=True),
    "term_iri": ColumnSpec(description="Term IRI", required=True),
    "synonym": ColumnSpec(description="Synonym string", required=True),
    "synonym_type": ColumnSpec(allowed_values=_SYNONYM_TYPES, description="Synonym scope: exact, related, narrow, or broad (from the oboInOwl synonym properties)"),
}

TERM_XREFS_SCHEMA = {
    "ontology_id": ColumnSpec(description="OLS ontology this row comes from", required=True),
    "term_iri": ColumnSpec(description="Term IRI", required=True),
    "xref": ColumnSpec(description="Database cross-reference (oboInOwl hasDbXref)", required=True),
}

PROPERTIES_SCHEMA = {
    "ontology_id": ColumnSpec(description="OLS ontology this row comes from", required=True),
    "iri": ColumnSpec(description="Property IRI", required=True),
    "curie": ColumnSpec(description="Compact identifier"),
    "short_form": ColumnSpec(description="Short form"),
    "label": ColumnSpec(description="Primary label"),
    "is_obsolete": ColumnSpec(description="Whether the property is obsolete"),
    "property_type": ColumnSpec(description="Kind of property: objectProperty, datatypeProperty, or annotationProperty"),
}

INDIVIDUALS_SCHEMA = {
    "ontology_id": ColumnSpec(description="OLS ontology this row comes from", required=True),
    "iri": ColumnSpec(description="Individual IRI", required=True),
    "curie": ColumnSpec(description="Compact identifier"),
    "short_form": ColumnSpec(description="Short form"),
    "label": ColumnSpec(description="Primary label"),
    "is_obsolete": ColumnSpec(description="Whether the individual is obsolete"),
}

_TABLES = {
    "ontologies": ONTOLOGIES_SCHEMA,
    "terms": TERMS_SCHEMA,
    "term_parents": TERM_PARENTS_SCHEMA,
    "term_synonyms": TERM_SYNONYMS_SCHEMA,
    "term_xrefs": TERM_XREFS_SCHEMA,
    "properties": PROPERTIES_SCHEMA,
    "individuals": INDIVIDUALS_SCHEMA,
}

_BOOL_COLS = {"is_obsolete", "is_defining_ontology"}
_INT_COLS = {"num_classes", "num_properties", "num_individuals"}

# Typed synonym properties -> our synonym_type vocabulary.
_OBO = "http://www.geneontology.org/formats/oboInOwl#"
_SYNONYM_PROPS = {
    f"{_OBO}hasExactSynonym": "exact",
    f"{_OBO}hasRelatedSynonym": "related",
    f"{_OBO}hasNarrowSynonym": "narrow",
    f"{_OBO}hasBroadSynonym": "broad",
}
_XREF_PROP = f"{_OBO}hasDbXref"
_PROPERTY_TYPES = {"objectProperty", "datatypeProperty", "dataProperty", "annotationProperty"}

# Single-pass router config.
_ITEM_PREFIXES = {
    "ontologies.item.classes.item": "class",
    "ontologies.item.properties.item": "property",
    "ontologies.item.individuals.item": "individual",
}
_META_FIELDS = {
    "ontologies.item.iri": "iri",
    "ontologies.item.title": "title",
    "ontologies.item.description": "description",
    "ontologies.item.homepage": "homepage",
    "ontologies.item.http://www.w3.org/2002/07/owl#versionIRI": "version_iri",
    "ontologies.item.numberOfClasses.value": "num_classes",
    "ontologies.item.numberOfProperties.value": "num_properties",
    "ontologies.item.numberOfIndividuals.value": "num_individuals",
    "ontologies.item.isObsolete": "is_obsolete",
}

# Flush a table's row buffer to a shard once it reaches this many rows, to bound a
# worker's memory on the giant ontologies (NCBITaxon has millions of classes).
_FLUSH_ROWS = 500_000


def _vals(x: object) -> list[str]:
    """Unwrap an OLS annotation value into its string value(s).

    Handles a bare IRI string, a ``{"value": ...}`` object, and lists nesting either.
    """
    if isinstance(x, str):
        return [x]
    if isinstance(x, dict):
        v = x.get("value")
        return [v] if isinstance(v, str) else []
    if isinstance(x, list):
        out: list[str] = []
        for e in x:
            out.extend(_vals(e))
        return out
    return []


def _val(x: object) -> str | None:
    vs = _vals(x)
    return vs[0] if vs else None


def _stream_entities(fileobj):
    """Single pass over one OLS ontology JSON.

    Yields ``(kind, obj)`` for kind in {class, property, individual} as each entity
    completes, then a final ``("meta", dict)`` with the ontology's scalar metadata.
    Memory is bounded to one entity at a time via an incremental ObjectBuilder.
    """
    meta: dict = {}
    builder: ObjectBuilder | None = None
    kind: str | None = None
    depth = 0
    for prefix, event, value in ijson.parse(fileobj, use_float=True):
        if builder is not None:
            builder.event(event, value)
            if event in ("start_map", "start_array"):
                depth += 1
            elif event in ("end_map", "end_array"):
                depth -= 1
                if depth == 0:
                    yield kind, builder.value
                    builder = None
            continue
        if event == "start_map" and prefix in _ITEM_PREFIXES:
            kind = _ITEM_PREFIXES[prefix]
            builder = ObjectBuilder()
            builder.event(event, value)
            depth = 1
        elif prefix in _META_FIELDS and event in ("string", "number", "boolean"):
            meta[_META_FIELDS[prefix]] = value
    yield "meta", meta


def _class_rows(c: dict, oid: str):
    iri = c.get("iri")
    term = {
        "ontology_id": oid,
        "iri": iri,
        "curie": _val(c.get("curie")),
        "short_form": _val(c.get("shortForm")),
        "label": _val(c.get("label")),
        "definition": _val(c.get("definition")),
        "is_obsolete": bool(c.get("isObsolete", False)),
        "is_defining_ontology": bool(c.get("isDefiningOntology", False)),
    }
    parents = [{"ontology_id": oid, "term_iri": iri, "parent_iri": p} for p in _vals(c.get("directParent"))]
    syns = [
        {"ontology_id": oid, "term_iri": iri, "synonym": s, "synonym_type": styp}
        for prop, styp in _SYNONYM_PROPS.items()
        for s in _vals(c.get(prop))
    ]
    xrefs = [{"ontology_id": oid, "term_iri": iri, "xref": x} for x in _vals(c.get(_XREF_PROP))]
    return term, parents, syns, xrefs


def _property_row(p: dict, oid: str) -> dict:
    types = p.get("type") or []
    ptype = next((t for t in types if t in _PROPERTY_TYPES), None)
    return {
        "ontology_id": oid,
        "iri": p.get("iri"),
        "curie": _val(p.get("curie")),
        "short_form": _val(p.get("shortForm")),
        "label": _val(p.get("label")),
        "is_obsolete": bool(p.get("isObsolete", False)),
        "property_type": ptype,
    }


def _individual_row(i: dict, oid: str) -> dict:
    return {
        "ontology_id": oid,
        "iri": i.get("iri"),
        "curie": _val(i.get("curie")),
        "short_form": _val(i.get("shortForm")),
        "label": _val(i.get("label")),
        "is_obsolete": bool(i.get("isObsolete", False)),
    }


def _ontology_row(meta: dict, oid: str) -> dict:
    return {
        "ontology_id": oid,
        "iri": meta.get("iri"),
        "title": meta.get("title"),
        "description": meta.get("description"),
        "homepage": meta.get("homepage"),
        "version_iri": meta.get("version_iri"),
        "num_classes": meta.get("num_classes"),
        "num_properties": meta.get("num_properties"),
        "num_individuals": meta.get("num_individuals"),
        "is_obsolete": bool(meta.get("is_obsolete", False)),
    }


@register
class Ols(DatasetPipeline):
    name = "ols"

    def _tgz(self) -> Path:
        matches = sorted(self.raw_path().glob("*.tgz")) + sorted(self.raw_path().glob("*.tar.gz"))
        if len(matches) != 1:
            raise FileNotFoundError(f"expected exactly one ontology tarball under {self.raw_path()}, found {len(matches)}")
        return matches[0]

    def _finalize(self, rows: list[dict], schema: dict) -> pd.DataFrame:
        df = pd.DataFrame(rows)[list(schema)]
        for col in df.columns:
            if col in _INT_COLS:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            elif col in _BOOL_COLS:
                df[col] = df[col].astype("boolean")
        return self.apply_schema(df, schema)

    def _flush(self, oid: str, table: str, rows: list[dict], part: int) -> None:
        if not rows:
            return
        out_dir = self.output_path() / table
        out_dir.mkdir(parents=True, exist_ok=True)
        df = self._finalize(rows, _TABLES[table])
        self.save_parquet(df, out_dir / f"{oid}_{part:03d}.parquet")

    def _clear_shards(self, oid: str) -> None:
        """Remove any stale shards from a previously-interrupted run of this ontology."""
        for table in _TABLES:
            for shard in (self.output_path() / table).glob(f"{oid}_*.parquet"):
                shard.unlink()

    def _process_ontology(self, oid: str, fileobj) -> None:
        self._clear_shards(oid)
        buffers: dict[str, list[dict]] = {t: [] for t in _TABLES}
        parts: dict[str, int] = {t: 0 for t in _TABLES}
        meta: dict = {}

        def maybe_flush(table: str) -> None:
            if len(buffers[table]) >= _FLUSH_ROWS:
                self._flush(oid, table, buffers[table], parts[table])
                buffers[table] = []
                parts[table] += 1

        for kind, obj in _stream_entities(fileobj):
            if kind == "meta":
                meta = obj
            elif kind == "class":
                term, parents, syns, xrefs = _class_rows(obj, oid)
                buffers["terms"].append(term)
                buffers["term_parents"].extend(parents)
                buffers["term_synonyms"].extend(syns)
                buffers["term_xrefs"].extend(xrefs)
                for t in ("terms", "term_parents", "term_synonyms", "term_xrefs"):
                    maybe_flush(t)
            elif kind == "property":
                buffers["properties"].append(_property_row(obj, oid))
                maybe_flush("properties")
            elif kind == "individual":
                buffers["individuals"].append(_individual_row(obj, oid))
                maybe_flush("individuals")

        for table in _TABLES:
            if table == "ontologies":
                continue
            self._flush(oid, table, buffers[table], parts[table])
        self._flush(oid, "ontologies", [_ontology_row(meta, oid)], 0)

    def extract(self) -> None:
        done_dir = self.intermediate_path() / "_done"
        done_dir.mkdir(parents=True, exist_ok=True)
        n_done = n_new = 0
        with tarfile.open(self._tgz(), "r|gz") as tar:
            for member in tar:
                if not member.name.endswith("_linked.json"):
                    continue
                oid = Path(member.name).name.removesuffix("_linked.json")
                marker = done_dir / oid
                if marker.exists():
                    n_done += 1
                    continue
                fh = tar.extractfile(member)
                if fh is None:
                    continue
                self._process_ontology(oid, fh)
                marker.touch()
                n_new += 1
                if n_new % 20 == 0:
                    print(f"[{self.name}] processed {n_new} ontologies (skipped {n_done} already done)")
        print(f"[{self.name}] done: {n_new} processed, {n_done} skipped")

    def transform(self) -> None:
        # Each ontology file is an immutable slice written as final shards in extract;
        # nothing to reconcile across them.
        pass

    def load(self) -> None:
        for table, schema in _TABLES.items():
            self.save_schema_yaml(schema, table)
