"""Shared parsing for OBO-format ontologies (Disease Ontology, GO basic, ...).

An OBO file is a header of ``key: value`` lines followed by a flat sequence of
bracketed stanzas (``[Term]``, ``[Typedef]``, ...), each itself a set of ``key: value``
tag lines where a key may repeat. Walking the file into per-stanza tag dicts is
identical across the in-memory ontology pipelines; only the per-term projection into
output rows and the schemas differ. Those pipelines share the three helpers here and
keep their own ``_flush_term`` / ``SCHEMA`` definitions.
"""
from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path


def find_obo_file(directory: Path) -> Path:
    """Return the single ``*.obo`` file under ``directory`` (error if not exactly one)."""
    matches = sorted(directory.glob("*.obo"))
    if len(matches) != 1:
        raise FileNotFoundError(
            f"expected exactly one *.obo file under {directory}, found {len(matches)}"
        )
    return matches[0]


def ref(value: str) -> str:
    """Strip a trailing OBO ``' ! label'`` comment from an ID-reference value."""
    return value.split(" ! ", 1)[0].strip()


def iter_stanzas(path: Path) -> Iterator[tuple[str, dict[str, list[str]]]]:
    """Yield ``(stanza_type, tags)`` for the header and then each bracketed stanza.

    The header (lines before the first ``[...]``) is yielded first with an empty
    ``stanza_type``; each subsequent stanza yields its type (``Term``, ``Typedef``, ...)
    and a dict mapping every tag key to the list of its values in file order. Blank
    lines and lines without ``:`` are skipped, and the key/value split is on the first
    ``:`` (so CURIE values like ``MESH:D006394`` are preserved intact).
    """
    stanza_type = ""
    tags: dict[str, list[str]] = {}
    with open(path, encoding="utf-8") as fh:
        for raw in fh:
            line = raw.rstrip("\n")
            if line.startswith("["):
                yield stanza_type, tags
                stanza_type = line[1:-1] if line.endswith("]") else line[1:]
                tags = {}
                continue
            if not line or ":" not in line:
                continue
            key, _, value = line.partition(":")
            tags.setdefault(key.strip(), []).append(value.strip())
        yield stanza_type, tags
