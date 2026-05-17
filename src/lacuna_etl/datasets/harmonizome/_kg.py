"""Read Harmonizome knowledge-graph TSVs from `<slug>__kg_serializations.tar.gz`.

Python's `tarfile` module rejects some of these archives ("invalid header" on
pax-extended formats); GNU `tar` reads them all. We shell out and stream stdout.

Each KG tarball contains `<slug>_tsv/nodes.tsv` and `<slug>_tsv/edges.tsv`.

This module is intentionally minimal -- it does NOT swap source/target or
resolve symbols. Orientation interpretation lives in the pipeline and is
strictly opt-in (only `source_entrez -> non-entrez_attribute` is accepted).
"""

from __future__ import annotations

import io
import subprocess
from pathlib import Path

import pandas as pd


def _list_members(tgz: Path) -> list[str]:
    r = subprocess.run(["tar", "-tzf", str(tgz)], capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"tar -tzf failed for {tgz}: {r.stderr.strip()}")
    return [m.strip() for m in r.stdout.splitlines() if m.strip()]


def _extract_member_bytes(tgz: Path, member: str) -> bytes:
    r = subprocess.run(["tar", "-xzOf", str(tgz), member], capture_output=True)
    if r.returncode != 0:
        raise RuntimeError(f"tar -xzOf failed for {tgz}::{member}: {r.stderr.decode(errors='replace').strip()}")
    return r.stdout


def has_kg(tgz: Path) -> bool:
    return tgz.exists()


def read_nodes(tgz: Path) -> pd.DataFrame:
    """Return a DataFrame with columns ['namespace', 'id', 'label'].

    Two upstream-format quirks are corrected so that the Entrez gene side is
    identifiable downstream; nothing else is changed:
      * sangerdepmap uses a column named `type` (with value 'gene') instead of
        `namespace`; renamed and value mapped to 'NCBI Entrez'.
      * Some KGs (kinaselib, tyrkinaselib, cm4aiu2os) tag gene-side nodes with
        namespace 'gene' or empty while the id values ARE Entrez IDs. The
        numeric subset of those rows is retagged 'NCBI Entrez'. Non-numeric IDs
        in the same namespace are left untouched.
    """
    members = _list_members(tgz)
    cand = [m for m in members if m.endswith("nodes.tsv")]
    if not cand:
        raise FileNotFoundError(f"no nodes.tsv in {tgz}")
    raw = _extract_member_bytes(tgz, cand[0])
    df = pd.read_csv(
        io.BytesIO(raw),
        sep="\t",
        dtype=str,
        keep_default_na=False,
        na_values=[""],
        encoding_errors="replace",
    )
    if "namespace" not in df.columns and "type" in df.columns:
        df = df.rename(columns={"type": "namespace"})
        df["namespace"] = df["namespace"].replace({"gene": "NCBI Entrez"})

    if "namespace" in df.columns:
        ns_filled = df["namespace"].fillna("")
        for variant in ("gene", ""):
            ns_mask = ns_filled == variant
            if not ns_mask.any():
                continue
            numeric_mask = ns_mask & df["id"].astype(str).str.fullmatch(r"\d+").fillna(False)
            if numeric_mask.sum() >= 100 or (numeric_mask.sum() / max(ns_mask.sum(), 1) >= 0.5):
                df.loc[numeric_mask, "namespace"] = "NCBI Entrez"

    df = df[[c for c in df.columns if c in {"namespace", "id", "label"}]]
    return df


def read_edges(tgz: Path) -> pd.DataFrame:
    """Return edges with normalized columns:
       'source', 'relation', 'target', 'standardized_value', 'threshold'.

    Coalesces continuous-value columns (standardized/standardized_value/z-score/
    zscore/score/percentile/cleaned_value/count/protein_intensity) into
    `standardized_value`, and `threshold_value` -> `threshold`. NO orientation
    swap is performed -- callers must check orientation explicitly.
    """
    members = _list_members(tgz)
    cand = [m for m in members if m.endswith("edges.tsv")]
    if not cand:
        raise FileNotFoundError(f"no edges.tsv in {tgz}")
    raw = _extract_member_bytes(tgz, cand[0])
    df = pd.read_csv(
        io.BytesIO(raw),
        sep="\t",
        dtype=str,
        keep_default_na=False,
        na_values=[""],
        encoding_errors="replace",
    )

    cols = list(df.columns)
    if "source" not in cols or "target" not in cols:
        raise ValueError(f"unexpected edges header in {tgz}: {cols}")

    if "relation" not in df.columns:
        df["relation"] = pd.NA

    continuous_candidates = [
        "standardized_value", "standardized", "z-score", "zscore",
        "score", "percentile", "cleaned_value", "count",
        "protein_intensity",
    ]
    chosen = next((c for c in continuous_candidates if c in df.columns), None)
    if chosen is not None and chosen != "standardized_value":
        df = df.rename(columns={chosen: "standardized_value"})
    if "standardized_value" not in df.columns:
        df["standardized_value"] = pd.NA

    if "threshold" not in df.columns and "threshold_value" in df.columns:
        df = df.rename(columns={"threshold_value": "threshold"})
    if "threshold" not in df.columns:
        df["threshold"] = pd.NA

    return df[["source", "relation", "target", "standardized_value", "threshold"]]


def classify_orientation(edges: pd.DataFrame, entrez_ids: set[str], sample_size: int = 200) -> str:
    """Classify a KG by the structural relationship of source / target to the
    Entrez-id set. Returns one of:
        'gene_to_attr'   -- source is overwhelmingly Entrez, target is not
        'attr_to_gene'   -- target is overwhelmingly Entrez, source is not (REVERSED)
        'gene_to_gene'   -- both are overwhelmingly Entrez
        'no_entrez'      -- neither side is Entrez (e.g. KG uses symbols only)
        'empty'          -- no edge rows
    Only 'gene_to_attr' should be admitted into the canonical output.
    """
    if not len(entrez_ids):
        return "no_entrez"
    if edges.empty:
        return "empty"
    sample = edges.head(sample_size)
    src_frac = sample["source"].isin(entrez_ids).mean()
    tgt_frac = sample["target"].isin(entrez_ids).mean()
    if src_frac >= 0.8 and tgt_frac <= 0.2:
        return "gene_to_attr"
    if src_frac >= 0.5 and tgt_frac >= 0.5:
        return "gene_to_gene"
    if src_frac <= 0.2 and tgt_frac >= 0.8:
        return "attr_to_gene"
    return "ambiguous"
