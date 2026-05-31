"""Readers for `<slug>__gene_list_terms.txt.gz` and `<slug>__attribute_list_entries.txt.gz`.

Both files have heterogeneous headers across collections, but conceptually:

  gene_list_terms          : (gene_symbol, secondary_id, entrez_id)
  attribute_list_entries   : (attribute_label, secondary_descriptor, attribute_id)

The exact column names vary -- e.g. gene_list has 23 distinct header tuples
('GeneSym/NA/GeneID', '/Gene/Gene ID', 'GeneSym/UniProtACC/GeneID', etc.).
We map positionally: col 0 = primary label, col 1 = secondary, col 2 = id-ish.

These tables provide the "background universe" of nodes the collection considered
even when no edge mentions them.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


def read_gene_list(path: Path) -> pd.DataFrame:
    """Return ['entrez_id', 'symbol', 'secondary_id', 'secondary_id_type'].

    Header detection: if the first column header is empty/index-like ('', 'id'),
    the file is the Format-B indexed style and the gene/id columns shift right.
    """
    df = pd.read_csv(
        path, sep="\t", dtype=str,
        keep_default_na=False, na_values=["", "na", "NA"],
        encoding_errors="replace", compression="gzip",
    )
    cols = list(df.columns)
    header_is_indexed = cols and cols[0].strip() in {"", "id"}
    if header_is_indexed:
        cols = cols[1:]
    if len(cols) < 3:
        # Malformed; emit empty frame.
        return pd.DataFrame(columns=["entrez_id", "symbol", "secondary_id", "secondary_id_type"])

    sym_col, sec_col, id_col = cols[0], cols[1], cols[2]
    out = pd.DataFrame({
        "entrez_id": df[id_col],
        "symbol": df[sym_col],
        "secondary_id": df[sec_col],
        "secondary_id_type": sec_col if sec_col.lower() not in {"na", "nan"} else None,
    })
    return out


def read_attribute_list(path: Path) -> pd.DataFrame:
    """Return ['attribute_id', 'attribute_label', 'secondary_descriptor'].

    Header heuristic mirrors `read_gene_list`. The 3rd column (if present) is
    usually the id-ish field; if there is no 3rd column, the label IS the id.
    """
    df = pd.read_csv(
        path, sep="\t", dtype=str,
        keep_default_na=False, na_values=["", "na", "NA"],
        encoding_errors="replace", compression="gzip",
    )
    cols = list(df.columns)
    header_is_indexed = cols and cols[0].strip() in {"", "id"}
    if header_is_indexed:
        cols = cols[1:]
    if not cols:
        return pd.DataFrame(columns=["attribute_id", "attribute_label", "secondary_descriptor"])

    label_col = cols[0]
    sec_col = cols[1] if len(cols) > 1 else None
    id_col = cols[2] if len(cols) > 2 else None

    label = df[label_col]
    secondary = df[sec_col] if sec_col else pd.Series([pd.NA] * len(df))
    raw_id = df[id_col] if id_col else label  # fall back: label is the id when no id col

    out = pd.DataFrame({
        "attribute_id": raw_id,
        "attribute_label": label,
        "secondary_descriptor": secondary,
    })
    return out
