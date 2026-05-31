"""Polars helpers for exploding Open Targets' nested struct/list columns into flat tables.

Each helper keeps one or more parent key columns on every output row and drops the
null rows that `explode` produces from empty or absent source lists, so an empty
source list contributes nothing rather than a spurious null row. Key columns are
aliased to their output names *before* unnesting, so a struct subfield named like a
key (e.g. a `go` struct's own `id`) cannot collide with the parent key.
"""

from __future__ import annotations

import polars as pl

# An empty frame returned when a source column is absent; the worker skips empty
# frames, so the (otherwise unknown) column schema does not matter.
_EMPTY = pl.DataFrame()


def explode_struct_list(
    df: pl.DataFrame,
    keys: dict[str, str],
    list_col: str,
    fields: dict[str, str],
    casts: dict[str, pl.DataType] | None = None,
) -> pl.DataFrame:
    """Explode a List(Struct) column into one row per element.

    keys:    {source_key_col: output_name} parent columns kept on every row.
    list_col: the List(Struct) column to explode.
    fields:  {struct_subfield: output_name} subfields to keep, in output order.
    casts:   optional {output_name: polars dtype} applied to those outputs.
    """
    if list_col not in df.columns:
        return _EMPTY
    keyed = df.select(
        [pl.col(k).alias(v) for k, v in keys.items()] + [pl.col(list_col)]
    )
    sub = (
        keyed.explode(list_col)
        .filter(pl.col(list_col).is_not_null())
        .unnest(list_col)
    )
    out = [pl.col(v) for v in keys.values()]
    for src, name in fields.items():
        expr = pl.col(src)
        if casts and name in casts:
            expr = expr.cast(casts[name])
        out.append(expr.alias(name))
    return sub.select(out)


def explode_scalar_list(
    df: pl.DataFrame,
    keys: dict[str, str],
    list_col: str,
    out_name: str,
    cast: pl.DataType | None = None,
) -> pl.DataFrame:
    """Explode a List(scalar) column into one row per element under `out_name`."""
    if list_col not in df.columns:
        return _EMPTY
    keyed = df.select(
        [pl.col(k).alias(v) for k, v in keys.items()] + [pl.col(list_col).alias(out_name)]
    )
    sub = keyed.explode(out_name).filter(pl.col(out_name).is_not_null())
    if cast is not None:
        sub = sub.with_columns(pl.col(out_name).cast(cast))
    return sub


def concat_tagged(parts: list[pl.DataFrame]) -> pl.DataFrame:
    """Vertically concat the non-empty frames (each already carrying its tag column)."""
    parts = [p for p in parts if not p.is_empty()]
    if not parts:
        return _EMPTY
    return pl.concat(parts, how="vertical")
