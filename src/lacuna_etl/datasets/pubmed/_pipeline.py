"""ETL pipeline for the PubMed/MEDLINE annual baseline + daily update XML files.

Layout under `data_root / ncbi_pubmed`:
  YYYY/baseline/pubmed26nNNNN.xml.gz          (NLM annual baseline; numbered 0001..)
  YYYY/updatefiles/pubmed26nNNNN.xml.gz       (daily deltas, numbered above the baseline)

NLM's loading rule: baseline first, then updates in numerical order; revisions and
deletions in later files supersede earlier records. The integer NNNN in the
filename gives the global load order, so we tag every extracted row with
`file_seq = NNNN` and resolve "latest wins" during transform with a single
join against the per-PMID max(file_seq) from the articles shards.

Stages:
  extract()   parses each xml.gz once (parallel workers) into per-file parquet
              shards under `intermediate_path()/<table>/<stem>.parquet`. A
              marker `_done/<stem>.done` lets re-runs skip finished files.
  transform() lazily joins each table against the per-PMID latest-seq table
              and anti-joins against the union of DeleteCitation PMIDs, then
              streams the deduped result to `output_path()/<table>.parquet`.
  load()     writes a side-car schema yaml per table.
"""

from __future__ import annotations

import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import polars as pl
import yaml
from tqdm import tqdm

from lacuna_etl.core.pipeline import DatasetPipeline
from lacuna_etl.datasets.pubmed._parse import (
    iter_pubmed_records,
    parse_article,
    parse_delete_citation,
)
from lacuna_etl.datasets.pubmed._schemas import (
    CHILD_TABLES,
    POLARS_SCHEMAS,
    TABLES_DOC,
)
from lacuna_etl.datasets.registry import register

_DEFAULT_WORKERS = 4
_DONE_DIR = "_done"

# Anchored form of the Doi identifier pattern, applied to scrub the doi columns
# of source-data anomalies (stray whitespace, empty suffixes) before validation.
_DOI_OK = r"^10\.[^/\s]+/\S+$"


def _keep_if_doi(expr):  # pl.Expr -> pl.Expr
    import polars as pl
    return pl.when(expr.str.contains(_DOI_OK)).then(expr).otherwise(None)


def _file_seq(gz_path: Path) -> int:
    """Extract the integer NNNN from a `pubmed26nNNNN.xml.gz` filename."""
    stem = gz_path.name.removesuffix(".xml.gz")
    # stem looks like 'pubmed26n0001'
    digits = stem.split("n")[-1]
    return int(digits)


def _shard_stem(gz_path: Path) -> str:
    return gz_path.name.removesuffix(".xml.gz")


def _process_file(args: tuple[str, str]) -> tuple[str, int, int]:
    """Parse one xml.gz and write per-table parquet shards.

    Each shard gets a `file_seq` column so the transform stage can pick the
    latest version of every PMID without re-reading source XML.
    """
    gz_path_str, intermediate_dir_str = args
    gz_path = Path(gz_path_str)
    intermediate_dir = Path(intermediate_dir_str)

    file_seq = _file_seq(gz_path)
    stem = _shard_stem(gz_path)

    rows: dict[str, list[dict]] = {table: [] for table in POLARS_SCHEMAS}

    for kind, elem in iter_pubmed_records(gz_path):
        if kind == "article":
            parsed = parse_article(elem)
            for table, table_rows in parsed.items():
                if table_rows:
                    rows[table].extend(table_rows)
        elif kind == "delete":
            for pmid in parse_delete_citation(elem):
                rows["deleted_pmids"].append({"pmid": pmid})

    article_count = len(rows["articles"])
    delete_count = len(rows["deleted_pmids"])

    for table, table_rows in rows.items():
        out_dir = intermediate_dir / table
        out_dir.mkdir(parents=True, exist_ok=True)
        shard_path = out_dir / f"{stem}.parquet"
        if not table_rows:
            # Drop any stale shard from a previous run so resume stays consistent.
            if shard_path.exists():
                shard_path.unlink()
            continue
        df = pl.DataFrame(table_rows, schema=POLARS_SCHEMAS[table])
        df = df.with_columns(pl.lit(file_seq, dtype=pl.Int32).alias("file_seq"))
        df.write_parquet(shard_path, compression="snappy")

    done_dir = intermediate_dir / _DONE_DIR
    done_dir.mkdir(parents=True, exist_ok=True)
    (done_dir / f"{stem}.done").touch()

    return gz_path_str, article_count, delete_count


@register
class NcbiPubmed(DatasetPipeline):
    """ETL for NLM PubMed/MEDLINE baseline + update XML."""

    name = "ncbi_pubmed"

    # ---- extract ----------------------------------------------------------

    def _gz_files(self) -> list[Path]:
        """All input xml.gz under the latest version, baseline first then updates."""
        version = self.raw_path()
        baseline = sorted((version / "baseline").glob("pubmed*n[0-9]*.xml.gz"))
        updates = sorted((version / "updatefiles").glob("pubmed*n[0-9]*.xml.gz"))
        return baseline + updates

    def _is_done(self, gz_path: Path) -> bool:
        return (self.intermediate_path() / _DONE_DIR / f"{_shard_stem(gz_path)}.done").exists()

    def extract(self) -> None:
        all_files = self._gz_files()
        pending = [p for p in all_files if not self._is_done(p)]

        workers = int(os.environ.get("PUBMED_WORKERS", _DEFAULT_WORKERS))
        print(f"[{self.name}] {len(all_files)} xml.gz files, {len(pending)} to process")
        print(f"[{self.name}] workers={workers}")

        if not pending:
            return

        worker_args = [(str(gz), str(self.intermediate_path())) for gz in pending]
        total_articles = 0
        total_deletes = 0
        t0 = time.time()
        with ProcessPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(_process_file, a): a[0] for a in worker_args}
            with tqdm(total=len(pending), unit="file") as pbar:
                for future in as_completed(futures):
                    gz_str = futures[future]
                    try:
                        _, n_articles, n_deletes = future.result()
                        total_articles += n_articles
                        total_deletes += n_deletes
                    except Exception as e:
                        print(f"\nERROR [{gz_str}]: {e}", file=sys.stderr)
                        raise
                    finally:
                        pbar.update(1)

        elapsed = time.time() - t0
        rate = total_articles / elapsed if elapsed > 0 else 0
        print(
            f"[{self.name}] extracted {total_articles:,} articles, "
            f"{total_deletes:,} deletions in {elapsed:.1f}s ({rate:.0f} articles/s)"
        )

    # ---- transform --------------------------------------------------------

    def _shards(self, table: str) -> list[Path]:
        d = self.intermediate_path() / table
        return sorted(d.glob("*.parquet")) if d.exists() else []

    def _materialise_latest_seq(self) -> Path | None:
        """Write a `(pmid, file_seq)` table holding the latest version of each PMID.

        Returns the parquet path, or None if no article shards have been written.
        """
        shards = self._shards("articles")
        if not shards:
            return None
        out = self.intermediate_path() / "_latest_seq.parquet"
        (
            pl.scan_parquet(shards)
            .select(["pmid", "file_seq"])
            .group_by("pmid")
            .agg(pl.col("file_seq").max().alias("file_seq"))
            .sink_parquet(out, compression="snappy")
        )
        return out

    def _materialise_deleted(self) -> Path | None:
        shards = self._shards("deleted_pmids")
        if not shards:
            return None
        out = self.intermediate_path() / "_deleted.parquet"
        (
            pl.scan_parquet(shards)
            .select("pmid")
            .unique()
            .sink_parquet(out, compression="snappy")
        )
        return out

    def transform(self) -> None:
        latest_seq_path = self._materialise_latest_seq()
        if latest_seq_path is None:
            print(f"[{self.name}] no article shards; skipping transform")
            return
        deleted_path = self._materialise_deleted()

        deleted_lf = (
            pl.scan_parquet(deleted_path) if deleted_path is not None
            else pl.DataFrame({"pmid": []}, schema={"pmid": pl.Int64}).lazy()
        )
        latest_seq_lf = pl.scan_parquet(latest_seq_path)

        out_root = self.output_path()

        # articles: keep only the row at each PMID's latest file_seq, minus deletes.
        # Some source DOIs have stray internal whitespace or empty suffixes ('10.NNN/ '
        # collapses to '10.NNN/'); these can't pass the Doi validator, so we strip
        # whitespace and null out anything that no longer matches the canonical shape.
        articles_lf = (
            pl.scan_parquet(self._shards("articles"))
            .join(latest_seq_lf, on=["pmid", "file_seq"], how="inner")
            .join(deleted_lf, on="pmid", how="anti")
            .drop("file_seq")
            .with_columns(
                pl.col("doi")
                .str.replace_all(r"\s+", "")
                .pipe(_keep_if_doi)
                .alias("doi")
            )
        )
        articles_lf.sink_parquet(out_root / "articles.parquet", compression="snappy")

        # child tables: each row inherits its article's file_seq, so the same
        # (pmid, file_seq) join drops all rows from any superseded version.
        for table in CHILD_TABLES:
            shards = self._shards(table)
            if not shards:
                continue
            lf = (
                pl.scan_parquet(shards)
                .join(latest_seq_lf, on=["pmid", "file_seq"], how="inner")
                .join(deleted_lf, on="pmid", how="anti")
                .drop("file_seq")
            )
            if table == "article_ids":
                cleaned_doi = pl.col("value").str.replace_all(r"\s+", "")
                lf = (
                    lf.with_columns(
                        pl.when(pl.col("id_type") == "doi")
                        .then(cleaned_doi)
                        .otherwise(pl.col("value"))
                        .alias("value")
                    )
                    .filter(
                        (pl.col("id_type") != "doi")
                        | pl.col("value").str.contains(_DOI_OK)
                    )
                )
            lf.sink_parquet(out_root / f"{table}.parquet", compression="snappy")

        if deleted_path is not None:
            pl.scan_parquet(deleted_path).sink_parquet(
                out_root / "deleted_pmids.parquet", compression="snappy"
            )

        self._validate_outputs()

    def _validate_outputs(self) -> None:
        """Run each table's ColumnSpec checks against the consolidated output."""
        out_root = self.output_path()
        for table, doc in TABLES_DOC.items():
            path = out_root / f"{table}.parquet"
            if not path.exists():
                continue
            check_cols = [
                c for c, spec in doc.items()
                if (spec.identifier is not None or spec.allowed_values is not None or spec.required)
            ]
            if not check_cols:
                continue
            lf = pl.scan_parquet(path)
            available = set(lf.collect_schema().names())
            check_cols = [c for c in check_cols if c in available]
            if not check_cols:
                continue
            df = lf.select(check_cols).collect()
            for col in check_cols:
                spec = doc[col]
                spec.validate_polars(df[col])
            print(f"[{self.name}] validated {len(check_cols)} cols on {table} ({df.height:,} rows)")

    # ---- load -------------------------------------------------------------

    def load(self) -> None:
        out_root = self.output_path()
        for table, doc in TABLES_DOC.items():
            path = out_root / f"{table}.parquet"
            if not path.exists():
                continue
            data = {col: spec.yaml_entry() for col, spec in doc.items()}
            (out_root / f"{table}.yml").write_text(
                yaml.dump(data, sort_keys=False, allow_unicode=True)
            )
