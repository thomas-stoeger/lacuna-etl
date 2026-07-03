"""Harmonizome ETL pipeline -- defensive variant.

Only processes collections whose edges follow a clearly identifiable
`source = NCBI Entrez gene -> target = non-gene attribute` shape. Anything
ambiguous (gene-gene relationships like PPI / kinase-substrate / TF-target;
KGs whose gene-side is in the target column; collections lacking an explicit
Entrez gene-id column) is SKIPPED and recorded in the processing report
together with a human-readable reason.

Source priority per collection:
  1. <slug>__kg_serializations.tar.gz -- accepted only if KG classifier
     returns 'gene_to_attr'
  2. <slug>__gene_attribute_edges.txt.gz -- accepted only if edges-file
     classifier returns 'gene_to_attr'

Output layout under `<output_root>/harmonizome/`:

  collections.parquet                                    # one row per processed collection
  edges/collection_slug=<slug>/part.parquet              # gene -> attribute edges
  genes/collection_slug=<slug>/part.parquet              # background gene universe
  attributes/collection_slug=<slug>/part.parquet         # background attribute universe
  processing_report.parquet                              # per-slug audit trail
  processing_report.tsv                                  # human-readable TSV mirror
  *.yml                                                  # schema documentation
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml
from tqdm import tqdm

from lacuna_etl.core.identifiers import NcbiGeneId
from lacuna_etl.core.pipeline import DatasetPipeline
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.harmonizome._dicts import read_attribute_list, read_gene_list
from lacuna_etl.datasets.harmonizome._edges_fallback import (
    classify_edges_file,
    read_edges_fallback,
)
from lacuna_etl.datasets.harmonizome._kg import (
    classify_orientation,
    read_edges as kg_read_edges,
    read_nodes as kg_read_nodes,
)
from lacuna_etl.datasets.harmonizome._namespaces import normalize_target_id, resolve_namespace
from lacuna_etl.datasets.ncbi_gene_history import update_entrez_ids
from lacuna_etl.datasets.registry import register

EDGES_SCHEMA = {
    "source_entrez_id":   ColumnSpec(identifier=NcbiGeneId, required=True,
                                     description="Gene-side NCBI/Entrez Gene ID (history-updated)"),
    "relation":           ColumnSpec(description="Semantic relation from the KG (e.g. 'has phenotype', 'over-expressed in'); null for KG-less collections"),
    "target_id":          ColumnSpec(description="Attribute-side identifier; CURIE-normalized for known ontologies (GO/HP/MP/CL/MONDO/...), otherwise the raw KG/edges string"),
    "target_namespace":   ColumnSpec(description="Canonical namespace tag for target_id (e.g. 'GO', 'MP', 'CL', 'MedGen'); 'unknown' when missing"),
    "target_label":       ColumnSpec(description="Human-readable attribute label"),
    "standardized_value": ColumnSpec(description="Continuous association value when provided (z-score, percentile, count, ...); null if only binary association"),
    "threshold":          ColumnSpec(description="Binary association indicator (1 = significant); null when not provided"),
}

GENES_SCHEMA = {
    "entrez_id":         ColumnSpec(identifier=NcbiGeneId, description="NCBI/Entrez Gene ID"),
    "symbol":            ColumnSpec(description="Gene symbol as reported by the collection"),
    "secondary_id":      ColumnSpec(description="Secondary identifier from the collection (UniProtACC, Ensembl, ProbeID, ...)"),
    "secondary_id_type": ColumnSpec(description="Header label naming the secondary_id column; null when absent"),
}

ATTRIBUTES_SCHEMA = {
    "attribute_id":         ColumnSpec(description="Attribute identifier (raw KG string or fall-back textual attribute)"),
    "attribute_label":      ColumnSpec(description="Human-readable attribute label"),
    "secondary_descriptor": ColumnSpec(description="Second descriptor column from the attribute_list file when present"),
}

COLLECTIONS_SCHEMA = {
    "collection_slug": ColumnSpec(required=True, description="Harmonizome dataset slug"),
    "name":            ColumnSpec(description="Display name of the collection"),
    "page_url":        ColumnSpec(description="URL of the Harmonizome web page for the collection"),
    "description":     ColumnSpec(description="Short description from metadata.json"),
    "category":        ColumnSpec(description="Harmonizome category"),
    "measurement":     ColumnSpec(description="What is being measured (free text)"),
    "association":     ColumnSpec(description="What the edges represent (free text)"),
    "resource":        ColumnSpec(description="Original upstream resource name"),
    "last_updated":    ColumnSpec(description="Original 'last_updated' string from metadata.json"),
    "source":          ColumnSpec(description="Where the edges came from ('kg' or 'gene_attributes')",
                                  allowed_values={"kg", "gene_attributes"}),
    "n_edges":         ColumnSpec(description="Number of edges emitted"),
    "n_genes_bg":      ColumnSpec(description="Rows in the background gene_list partition"),
    "n_attributes_bg": ColumnSpec(description="Rows in the background attribute_list partition"),
}

# Hard skips with explicit reasons (over and above the structural classifier).
_HARD_SKIPS: dict[str, str] = {}


def _read_metadata(path: Path) -> dict:
    try:
        with open(path, "rb") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _discover_slugs(raw_dir: Path) -> list[str]:
    slugs = {p.name.split("__", 1)[0] for p in raw_dir.iterdir() if "__" in p.name}
    return sorted(slugs)


def _save_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), path, compression="snappy")


def _coalesce_threshold(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").astype("Int8")


def _coalesce_float(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def _extract_kg(tgz: Path) -> tuple[str, pd.DataFrame | None]:
    """Return (status, edges_df). status is one of:
       'ok' (gene_to_attr -> df returned), 'attr_to_gene', 'gene_to_gene',
       'no_entrez', 'empty', 'ambiguous', or 'error: <msg>'.
    """
    try:
        nodes = kg_read_nodes(tgz)
        edges = kg_read_edges(tgz)
    except Exception as e:  # noqa: BLE001
        return f"error: {e}", None
    entrez_ids = set(nodes.loc[nodes["namespace"].fillna("") == "NCBI Entrez", "id"].dropna())
    status = classify_orientation(edges, entrez_ids)
    if status != "gene_to_attr":
        return status, None

    # Join target namespace + label from nodes via target == nodes.id.
    attr_nodes = nodes[nodes["namespace"].fillna("") != "NCBI Entrez"].copy()
    attr_nodes = attr_nodes.rename(columns={"id": "target", "label": "target_label",
                                            "namespace": "target_namespace"})
    merged = edges.merge(attr_nodes[["target", "target_label", "target_namespace"]],
                         on="target", how="left")
    merged = merged.rename(columns={"source": "entrez_id", "target": "target_id"})
    merged["target_namespace"] = merged["target_namespace"].fillna("")
    return "ok", merged[[
        "entrez_id", "relation", "target_id", "target_namespace",
        "target_label", "standardized_value", "threshold",
    ]]


def _extract_edges_file(path: Path) -> tuple[str, pd.DataFrame | None]:
    try:
        status = classify_edges_file(path)
    except Exception as e:  # noqa: BLE001
        return f"error: {e}", None
    if status != "gene_to_attr":
        return status, None
    try:
        df = read_edges_fallback(path)
    except Exception as e:  # noqa: BLE001
        return f"error: {e}", None
    df["relation"] = pd.NA
    df["target_namespace"] = ""
    df = df.rename(columns={"target_label": "target_label"})
    return "ok", df[[
        "entrez_id", "relation", "target_id", "target_namespace",
        "target_label", "standardized_value", "threshold",
    ]]


# Human-readable skip reasons indexed by classifier status.
_SKIP_REASON: dict[str, str] = {
    "attr_to_gene":   "KG orientation is attribute->gene (relation runs from non-gene to gene); needs manual semantics review before flipping",
    "gene_to_gene":   "both endpoints are genes (e.g. PPI, kinase-substrate, TF-target); direction depends on relation semantics",
    "no_entrez":      "no NCBI Entrez nodes detected in the KG; gene side cannot be identified structurally",
    "ambiguous":      "neither side is predominantly Entrez; orientation cannot be determined structurally",
    "empty":          "KG edges file is empty",
    "no_gene_id":     "edges file has no identifiable gene_id column",
    "unknown_format": "edges file header doesn't match any recognized Format-A or Format-B shape",
}


@register
class Harmonizome(DatasetPipeline):
    name = "harmonizome"
    depends_on = ["ncbi_gene_history"]
    _TABLES = [
        ("edges", EDGES_SCHEMA),
        ("genes", GENES_SCHEMA),
        ("attributes", ATTRIBUTES_SCHEMA),
        ("collections", COLLECTIONS_SCHEMA),
    ]

    def expected_schemas(self) -> dict:
        # processing_report.{parquet,yml,tsv} is emitted by write_report(), separately
        # from the four data tables in _TABLES; include it so its sidecar is tracked too.
        from lacuna_etl.datasets.harmonizome._report import REPORT_SCHEMA
        return {**dict(self._TABLES), "processing_report": dict(REPORT_SCHEMA)}

    # --- extract ---------------------------------------------------------

    def extract(self) -> None:
        raw = self.raw_path()
        slugs = _discover_slugs(raw)
        print(f"[harmonizome] discovered {len(slugs)} slugs")

        per_coll_root = self.intermediate_path() / "per_collection"
        collections_rows: list[dict] = []
        skipped_rows: list[dict] = []

        for slug in tqdm(slugs, desc="[harmonizome] extract"):
            if slug in _HARD_SKIPS:
                skipped_rows.append({"collection_slug": slug, "reason": _HARD_SKIPS[slug]})
                continue

            meta = _read_metadata(raw / f"{slug}__metadata.json")
            kg_tgz = raw / f"{slug}__kg_serializations.tar.gz"
            edges_path = raw / f"{slug}__gene_attribute_edges.txt.gz"

            edges_df: pd.DataFrame | None = None
            source = None
            kg_status = None
            ef_status = None

            if kg_tgz.exists():
                kg_status, edges_df = _extract_kg(kg_tgz)
                if edges_df is not None:
                    source = "kg"

            if edges_df is None and edges_path.exists():
                ef_status, edges_df = _extract_edges_file(edges_path)
                if edges_df is not None:
                    source = "gene_attributes"

            if edges_df is None:
                if kg_tgz.exists() and edges_path.exists():
                    reason = (f"KG path -> {_SKIP_REASON.get(kg_status, kg_status)}; "
                              f"gene_attributes path -> {_SKIP_REASON.get(ef_status, ef_status)}")
                elif kg_tgz.exists():
                    reason = f"KG path -> {_SKIP_REASON.get(kg_status, kg_status)}; no gene_attributes file"
                elif edges_path.exists():
                    reason = f"gene_attributes path -> {_SKIP_REASON.get(ef_status, ef_status)}; no KG file"
                else:
                    reason = "no edges file and no KG file"
                skipped_rows.append({"collection_slug": slug, "reason": reason})
                continue

            out_dir = per_coll_root / slug
            out_dir.mkdir(parents=True, exist_ok=True)
            _save_parquet(edges_df, out_dir / "edges.parquet")

            gene_list_path = raw / f"{slug}__gene_list_terms.txt.gz"
            genes_df = pd.DataFrame()
            if gene_list_path.exists():
                try:
                    genes_df = read_gene_list(gene_list_path)
                    _save_parquet(genes_df, out_dir / "genes.parquet")
                except Exception as e:  # noqa: BLE001
                    print(f"[harmonizome] WARN: gene_list parse failed for {slug}: {e}")

            attr_list_path = raw / f"{slug}__attribute_list_entries.txt.gz"
            attrs_df = pd.DataFrame()
            if attr_list_path.exists():
                try:
                    attrs_df = read_attribute_list(attr_list_path)
                    _save_parquet(attrs_df, out_dir / "attributes.parquet")
                except Exception as e:  # noqa: BLE001
                    print(f"[harmonizome] WARN: attribute_list parse failed for {slug}: {e}")

            collections_rows.append({
                "collection_slug": slug,
                "name": meta.get("name"),
                "page_url": meta.get("page_url"),
                "description": meta.get("description"),
                "category": meta.get("category"),
                "measurement": meta.get("measurement"),
                "association": meta.get("association"),
                "resource": meta.get("resource"),
                "last_updated": meta.get("last_updated"),
                "source": source,
                "n_edges": int(len(edges_df)),
                "n_genes_bg": int(len(genes_df)),
                "n_attributes_bg": int(len(attrs_df)),
                "stats": meta.get("stats") or [],
                "citations": meta.get("citations") or [],
            })

        coll_df = pd.DataFrame(collections_rows)
        skip_df = pd.DataFrame(skipped_rows)
        _save_parquet(coll_df, self.intermediate_path() / "collections.parquet")
        _save_parquet(skip_df, self.intermediate_path() / "skipped.parquet")
        print(f"[harmonizome] processed {len(coll_df)} | skipped {len(skip_df)}")

    # --- transform -------------------------------------------------------

    def transform(self) -> None:
        per_coll_root = self.intermediate_path() / "per_collection"
        if not per_coll_root.exists():
            return
        slugs = sorted(p.name for p in per_coll_root.iterdir() if p.is_dir())

        total_dropped = 0
        for slug in tqdm(slugs, desc="[harmonizome] transform"):
            d = per_coll_root / slug
            edges_path = d / "edges.parquet"
            if not edges_path.exists():
                continue
            edges = pd.read_parquet(edges_path)

            edges = edges.dropna(subset=["entrez_id"])
            edges["entrez_id"] = pd.to_numeric(edges["entrez_id"], errors="coerce").astype("Int64")
            edges = edges.dropna(subset=["entrez_id"])

            updated = update_entrez_ids(edges["entrez_id"], on_discontinued="drop")
            total_dropped += len(edges) - len(updated)
            edges = edges.loc[updated.index].copy()
            edges["entrez_id"] = updated.values

            ns_resolved = edges["target_namespace"].astype(str).map(resolve_namespace)
            edges["target_namespace"] = ns_resolved
            edges["target_id"] = [
                normalize_target_id(ns, tid) if isinstance(tid, str) else tid
                for ns, tid in zip(ns_resolved, edges["target_id"])
            ]

            edges["standardized_value"] = _coalesce_float(edges["standardized_value"])
            edges["threshold"] = _coalesce_threshold(edges["threshold"])

            edges = edges.rename(columns={"entrez_id": "source_entrez_id"})
            edges = edges[list(EDGES_SCHEMA.keys())]
            _save_parquet(edges, d / "edges_transformed.parquet")

            genes_path = d / "genes.parquet"
            if genes_path.exists():
                genes = pd.read_parquet(genes_path)
                if not genes.empty:
                    genes["entrez_id"] = pd.to_numeric(genes["entrez_id"], errors="coerce").astype("Int64")
                    genes = genes.dropna(subset=["entrez_id"])
                    if len(genes):
                        upd = update_entrez_ids(genes["entrez_id"], on_discontinued="drop")
                        genes = genes.loc[upd.index].copy()
                        genes["entrez_id"] = upd.values
                        genes = genes.drop_duplicates(subset=["entrez_id"]).reset_index(drop=True)
                    _save_parquet(genes[list(GENES_SCHEMA.keys())], d / "genes_transformed.parquet")

            attrs_path = d / "attributes.parquet"
            if attrs_path.exists():
                attrs = pd.read_parquet(attrs_path)
                if not attrs.empty:
                    attrs = attrs.drop_duplicates(subset=["attribute_id"]).reset_index(drop=True)
                    for k in ATTRIBUTES_SCHEMA:
                        if k not in attrs.columns:
                            attrs[k] = pd.NA
                    _save_parquet(attrs[list(ATTRIBUTES_SCHEMA.keys())], d / "attributes_transformed.parquet")

        if total_dropped:
            print(f"[harmonizome] dropped {total_dropped:,} rows with Entrez IDs discontinued without replacement")

    # --- load ------------------------------------------------------------

    def load(self) -> None:
        from lacuna_etl.datasets.harmonizome._report import write_report

        out = self.output_path()
        per_coll_root = self.intermediate_path() / "per_collection"
        slugs = sorted(p.name for p in per_coll_root.iterdir() if p.is_dir()) if per_coll_root.exists() else []

        for slug in tqdm(slugs, desc="[harmonizome] load"):
            d = per_coll_root / slug
            for kind, dest in [("edges_transformed.parquet", "edges"),
                               ("genes_transformed.parquet", "genes"),
                               ("attributes_transformed.parquet", "attributes")]:
                src = d / kind
                if not src.exists():
                    continue
                dst = out / dest / f"collection_slug={slug}" / "part.parquet"
                dst.parent.mkdir(parents=True, exist_ok=True)
                _save_parquet(pd.read_parquet(src), dst)

        coll_df = pd.read_parquet(self.intermediate_path() / "collections.parquet")
        _save_parquet(coll_df, out / "collections.parquet")

        for name, schema in self._TABLES:
            data = {col: spec.yaml_entry() for col, spec in schema.items()}
            (out / f"{name}.yml").write_text(yaml.dump(data, sort_keys=False, allow_unicode=True))

        skipped_df = pd.read_parquet(self.intermediate_path() / "skipped.parquet")
        report_path, tsv_path = write_report(self.raw_path(), out, coll_df, skipped_df)
        print(f"[harmonizome] wrote {report_path}")
        print(f"[harmonizome] wrote {tsv_path}")
