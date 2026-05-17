"""Build `processing_report.parquet` + `processing_report.tsv` summarizing what
happened to each Harmonizome dataset slug.

Inputs (already produced by the pipeline):
  - `collections_df`  : rows for slugs whose edges were emitted to output/
  - `skipped_df`      : rows of (collection_slug, reason) for slugs that were
                        skipped (orientation ambiguous / gene-gene / missing files / etc.)

The TSV mirror lists one slug per line:
    <slug>\t<status>
where <status> is 'kg', 'gene_attributes', or a free-text skip reason.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from lacuna_etl.core.schema import ColumnSpec

REPORT_SCHEMA = {
    "collection_slug":      ColumnSpec(required=True, description="Harmonizome dataset slug"),
    "processed":            ColumnSpec(description="True if edges were emitted to output/"),
    "source":               ColumnSpec(description="Where the edges came from",
                                       allowed_values={"kg", "gene_attributes", "skipped"}),
    "skip_reason":          ColumnSpec(description="Free-text reason when source='skipped'; null otherwise"),
    "n_edges":              ColumnSpec(description="Number of edges emitted (0 if skipped)"),
    "n_unique_genes":       ColumnSpec(description="Distinct source_entrez_id values in the output edges"),
    "n_unique_attributes":  ColumnSpec(description="Distinct target_id values in the output edges"),
    "n_genes_bg":           ColumnSpec(description="Rows in the background genes/<slug> partition (0 if absent)"),
    "n_attributes_bg":      ColumnSpec(description="Rows in the background attributes/<slug> partition (0 if absent)"),
    "target_namespaces":    ColumnSpec(description="Comma-joined sorted list of target_namespace values present in the edges"),
    "relations":            ColumnSpec(description="Comma-joined sorted list of relation values; null for KG-less sources"),
}


def _partition_rows(parquet_dir: Path) -> int:
    f = parquet_dir / "part.parquet"
    if not f.exists():
        return 0
    return pq.read_metadata(f).num_rows


def _summarize_edges(edges_dir: Path) -> dict:
    f = edges_dir / "part.parquet"
    if not f.exists():
        return {"n_edges": 0, "n_unique_genes": 0, "n_unique_attributes": 0,
                "target_namespaces": "", "relations": None}
    df = pq.read_table(f, columns=["source_entrez_id", "target_id",
                                   "target_namespace", "relation"]).to_pandas()
    rels = sorted(set(df["relation"].dropna().astype(str)))
    return {
        "n_edges": len(df),
        "n_unique_genes": int(df["source_entrez_id"].nunique(dropna=True)),
        "n_unique_attributes": int(df["target_id"].nunique(dropna=True)),
        "target_namespaces": ",".join(sorted(set(df["target_namespace"].dropna().astype(str)))),
        "relations": ",".join(rels) if rels else None,
    }


def build_report(
    raw_dir: Path,
    output_dir: Path,
    collections_df: pd.DataFrame,
    skipped_df: pd.DataFrame,
) -> pd.DataFrame:
    coll_by_slug = {r["collection_slug"]: r for r in collections_df.to_dict("records")}
    skip_by_slug = {r["collection_slug"]: r["reason"] for r in skipped_df.to_dict("records")}

    all_slugs = sorted(set(coll_by_slug) | set(skip_by_slug) |
                       {p.name.split("__", 1)[0] for p in raw_dir.iterdir() if "__" in p.name})

    rows: list[dict] = []
    for slug in all_slugs:
        if slug in coll_by_slug:
            edges_dir = output_dir / "edges" / f"collection_slug={slug}"
            stats = _summarize_edges(edges_dir)
            row = coll_by_slug[slug]
            rows.append({
                "collection_slug": slug,
                "processed": True,
                "source": row["source"],
                "skip_reason": None,
                "n_edges": stats["n_edges"],
                "n_unique_genes": stats["n_unique_genes"],
                "n_unique_attributes": stats["n_unique_attributes"],
                "n_genes_bg": _partition_rows(output_dir / "genes" / f"collection_slug={slug}"),
                "n_attributes_bg": _partition_rows(output_dir / "attributes" / f"collection_slug={slug}"),
                "target_namespaces": stats["target_namespaces"],
                "relations": stats["relations"],
            })
        else:
            rows.append({
                "collection_slug": slug,
                "processed": False,
                "source": "skipped",
                "skip_reason": skip_by_slug.get(slug, "unknown"),
                "n_edges": 0,
                "n_unique_genes": 0,
                "n_unique_attributes": 0,
                "n_genes_bg": 0,
                "n_attributes_bg": 0,
                "target_namespaces": "",
                "relations": None,
            })
    return pd.DataFrame(rows)[list(REPORT_SCHEMA.keys())]


def write_report(
    raw_dir: Path,
    output_dir: Path,
    collections_df: pd.DataFrame,
    skipped_df: pd.DataFrame,
) -> tuple[Path, Path]:
    df = build_report(raw_dir, output_dir, collections_df, skipped_df)

    parquet_path = output_dir / "processing_report.parquet"
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), parquet_path, compression="snappy")

    schema_yaml = {col: spec.yaml_entry() for col, spec in REPORT_SCHEMA.items()}
    (output_dir / "processing_report.yml").write_text(
        yaml.dump(schema_yaml, sort_keys=False, allow_unicode=True)
    )

    # TSV mirror: one slug per line, <slug>\t<status>.
    tsv_path = output_dir / "processing_report.tsv"
    lines = []
    for r in df.to_dict("records"):
        if r["processed"]:
            status = r["source"]  # 'kg' or 'gene_attributes'
        else:
            status = r["skip_reason"] or "skipped"
        lines.append(f"{r['collection_slug']}\t{status}")
    tsv_path.write_text("\n".join(lines) + "\n")

    return parquet_path, tsv_path
