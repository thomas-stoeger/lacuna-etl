"""Read `<slug>__gene_attribute_edges.txt.gz` for collections without a usable KG.

Two source formats coexist:

  Format A (~111 collections): header row + a row of column-types, then data:
      source  source_desc  source_id  target  target_desc  target_id  weight
      GeneSym NA           GeneID     <attr>  <descriptor> NA          weight
      ...
    The 'target_id' is often the literal string '-666' meaning NA.

  Format B (~59 collections): one header row, leading unnamed index column,
      friendlier column names (Gene/gene, Gene ID/geneid, attr, attr_id, weight-likes).

Classification (`classify_edges_file`) returns one of:
    'gene_to_attr'  -- safe: source = Entrez gene, target = non-gene attribute
    'gene_to_gene' -- both sides are gene IDs (e.g. cheappi PPI, kinaselib);
                       direction is semantically determined by the relation
                       (e.g. 'phosphorylates') which we don't interpret here.
    'no_gene_id'    -- gene-side column not identifiable (e.g. knocktf,
                       sangerdepmap with model/symbol layout).
    'unknown_format'-- header doesn't match Format A or any Format B shape we
                       recognize.

Only 'gene_to_attr' rows are admitted into the canonical output.
"""

from __future__ import annotations

import gzip
from pathlib import Path

import pandas as pd

_FORMAT_A_HEADER = ("source", "source_desc", "source_id", "target", "target_desc", "target_id", "weight")

# Format A meta-row token meaning "the target side is itself a gene"
_FORMAT_A_GENE_TARGET_META = {"GeneID", "GeneSym"}

# Format B header tokens that indicate the attribute side is also genes
_FORMAT_B_GENE_ATTR_LABELS = {"gene", "gene symbol", "kinase", "tf",
                              "gene ko", "gene perturbation"}


def _peek_header_and_meta(path: Path) -> tuple[tuple[str, ...], tuple[str, ...] | None]:
    """Return (header, meta_row) where meta_row is the 2nd row for Format A
    (a row of column-type tokens), or None for Format B."""
    with gzip.open(path, "rt", encoding="utf-8", errors="replace") as g:
        head = tuple(next(g, "").rstrip("\n").split("\t"))
        if head == _FORMAT_A_HEADER:
            meta = tuple(next(g, "").rstrip("\n").split("\t"))
            return head, meta
        return head, None


def classify_edges_file(path: Path) -> str:
    head, meta = _peek_header_and_meta(path)
    if head == _FORMAT_A_HEADER:
        # meta is ('GeneSym', 'NA/UniProt/...', 'GeneID', <target_label_type>,
        #          <target_desc_type>, <target_id_type>, 'weight')
        if meta is None or len(meta) < 6:
            return "unknown_format"
        target_label_meta = meta[3]
        target_id_meta = meta[5]
        if target_label_meta in _FORMAT_A_GENE_TARGET_META or target_id_meta in _FORMAT_A_GENE_TARGET_META:
            return "gene_to_gene"
        return "gene_to_attr"

    # Format B: locate gene_col and gene_id_col, then inspect the attr-label col.
    lower = [c.strip().lower() for c in head]
    gene_idx = None
    gene_id_idx = None
    for i, c in enumerate(lower):
        if c in {"gene", "gene symbol", "symbol"} and gene_idx is None:
            gene_idx = i
        if c in {"gene id", "geneid", "gene_id"} and gene_id_idx is None:
            gene_id_idx = i
    if gene_idx is None or gene_id_idx is None:
        return "no_gene_id"
    # The attribute label column is the first non-gene column to the right of gene_id.
    after = [(i, c) for i, c in enumerate(lower) if i > gene_id_idx]
    if not after:
        return "unknown_format"
    attr_label_idx, attr_label_name = after[0]
    if attr_label_name in _FORMAT_B_GENE_ATTR_LABELS:
        return "gene_to_gene"
    return "gene_to_attr"


def read_edges_fallback(path: Path) -> pd.DataFrame:
    head, _ = _peek_header_and_meta(path)
    if head == _FORMAT_A_HEADER:
        return _read_format_a(path)
    return _read_format_b(path, head)


def _read_format_a(path: Path) -> pd.DataFrame:
    df = pd.read_csv(
        path, sep="\t", dtype=str, skiprows=[1],
        keep_default_na=False, na_values=["", "na", "NA", "-666"],
        encoding_errors="replace", compression="gzip",
    )
    out = pd.DataFrame({
        "entrez_id": df["source_id"],
        "gene_symbol": df["source"],
        "target_label": df["target"],
        "target_id": df["target_desc"].where(df["target_desc"].notna(), df["target"]),
        "standardized_value": df["weight"],
        "threshold": pd.Series([pd.NA] * len(df), dtype="object"),
    })
    return out


def _read_format_b(path: Path, head: tuple[str, ...]) -> pd.DataFrame:
    df = pd.read_csv(
        path, sep="\t", dtype=str,
        keep_default_na=False, na_values=["", "na", "NA", "-666"],
        encoding_errors="replace", compression="gzip",
    )
    df = df.rename(columns={c: c.strip() for c in df.columns})
    lower = {c.lower(): c for c in df.columns}

    gene_col = lower.get("gene") or lower.get("gene symbol") or lower.get("symbol")
    gene_id_col = lower.get("gene id") or lower.get("geneid") or lower.get("gene_id")
    if gene_col is None or gene_id_col is None:
        raise ValueError(f"no gene columns in {path.name}: {list(df.columns)}")

    value_aliases = {
        "standardized value", "standardized_value", "standardized",
        "threshold value", "threshold_value", "threshold",
        "z-score", "z_score", "zscore", "z",
        "score", "percentile", "fc", "log2fc",
        "count", "protein_intensity", "cd",
    }
    skip = {gene_col.lower(), gene_id_col.lower()} | value_aliases
    attr_cols = [c for c in df.columns if c.lower() not in skip and c.strip() != ""]
    attr_label_col = attr_cols[0] if attr_cols else None
    attr_id_col = attr_cols[1] if len(attr_cols) > 1 else None

    std_col = next((lower[c] for c in ("standardized value", "standardized_value", "standardized",
                                       "z-score", "zscore", "z", "score", "percentile", "fc",
                                       "log2fc", "count", "protein_intensity", "cd") if c in lower), None)
    thresh_col = next((lower[c] for c in ("threshold value", "threshold_value", "threshold") if c in lower), None)

    out = pd.DataFrame({
        "entrez_id": df[gene_id_col],
        "gene_symbol": df[gene_col],
        "target_label": df[attr_label_col] if attr_label_col else pd.NA,
        "target_id": df[attr_id_col] if attr_id_col else (df[attr_label_col] if attr_label_col else pd.NA),
        "standardized_value": df[std_col] if std_col else pd.Series([pd.NA] * len(df), dtype="object"),
        "threshold": df[thresh_col] if thresh_col else pd.Series([pd.NA] * len(df), dtype="object"),
    })
    return out
