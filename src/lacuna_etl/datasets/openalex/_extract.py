"""
Streaming reader for OpenAlex gzipped JSONL snapshots.

Each entity type lives at:
  <raw_root>/<entity>/updated_date=YYYY-MM-DD/part_NNNN.gz

Records are newline-delimited JSON, one per line.
"""

import gzip
from pathlib import Path
from typing import Iterator

import orjson


def iter_records(gz_path: Path) -> Iterator[dict]:
    """Yield parsed JSON records from one gz file, one at a time."""
    with gzip.open(gz_path, "rb") as fh:
        for raw_line in fh:
            line = raw_line.strip()
            if line:
                yield orjson.loads(line)


def iter_entity_files(entity_dir: Path) -> Iterator[Path]:
    """
    Yield all .gz files for an entity, sorted for reproducibility.
    Skips manifest files.
    """
    for gz_path in sorted(entity_dir.rglob("part_*.gz")):
        yield gz_path


def iter_batches(gz_path: Path, batch_size: int) -> Iterator[list[dict]]:
    """
    Yield lists of up to batch_size records from a gz file.
    Keeps peak memory proportional to batch_size, not file size.
    """
    batch: list[dict] = []
    for record in iter_records(gz_path):
        batch.append(record)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch
