"""
Verification checks run after transforming each batch.

Hard failures  -> raise ValueError (crashes the worker, halts the pipeline).
Soft warnings  -> appended as JSON lines to issues.jsonl in the output root for later inspection.

issues.jsonl schema (one object per line):
  {
    "ts":         "2026-03-14T12:00:00",   # UTC ISO timestamp
    "source":     "openalex/works",
    "gz_path":    "/path/to/part_0000.gz",
    "batch_idx":  0,
    "check":      "null_publication_year",
    "message":    "412/50000 records missing publication_year",
    "count":      412,
    "total":      50000
  }
"""

import json
import threading
from datetime import datetime, timezone

import polars as pl

from lacuna_etl.config import get_output_root

_log_lock = threading.Lock()


def _issues_log_path():
    return get_output_root() / "openalex" / "issues.jsonl"


def log_warning(source: str, gz_path: str, batch_idx: int, check: str,
                message: str, count: int, total: int) -> None:
    entry = {
        "ts":        datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "source":    source,
        "gz_path":   gz_path,
        "batch_idx": batch_idx,
        "check":     check,
        "message":   message,
        "count":     count,
        "total":     total,
    }
    path = _issues_log_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with _log_lock:
        with open(path, "a") as fh:
            fh.write(json.dumps(entry) + "\n")


def verify_works_batch(
    tables: dict[str, pl.DataFrame],
    gz_path: str,
    batch_idx: int = 0,
) -> None:
    works = tables["works"]
    if works.is_empty():
        return

    n = len(works)
    errors = []

    null_ids = works["work_id"].null_count()
    if null_ids > 0:
        errors.append(f"{null_ids} records with null work_id")

    dup_count = works.filter(works["work_id"].is_duplicated()).height
    if dup_count > 0:
        errors.append(f"{dup_count} duplicate work_ids in batch")

    wrong_prefix = works.filter(~pl.col("work_id").str.starts_with("W")).height
    if wrong_prefix > 0:
        errors.append(f"{wrong_prefix} work_ids without 'W' prefix")

    auth = tables["works_authorships"]
    if not auth.is_empty():
        work_ids = set(works["work_id"].to_list())
        orphans = auth.filter(~pl.col("work_id").is_in(work_ids)).height
        if orphans > 0:
            errors.append(f"{orphans} authorships referencing unknown work_id")

    if errors:
        raise ValueError(
            f"[{gz_path}] batch {batch_idx} verification failed:\n  "
            + "\n  ".join(errors)
        )

    null_year = works["publication_year"].null_count()
    if null_year > n * 0.05:
        log_warning("openalex/works", gz_path, batch_idx, "null_publication_year",
                    f"{null_year}/{n} records missing publication_year",
                    null_year, n)

    null_title = works["title"].null_count()
    if null_title > n * 0.10:
        log_warning("openalex/works", gz_path, batch_idx, "null_title",
                    f"{null_title}/{n} records missing title",
                    null_title, n)
