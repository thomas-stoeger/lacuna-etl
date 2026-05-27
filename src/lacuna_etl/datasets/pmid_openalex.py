"""
Crosswalk from PubMed PMIDs to OpenAlex work_ids.

Per docs/DESIGN.md ("Filling in missing values"), cross-dataset heuristic
linkage lives in its own table so provenance is preserved and consumers can
opt into the fuzzier matches. Matching is layered, most precise first:

  1. pmid          - OpenAlex carries the PMID directly.
  2. doi           - exact match on normalized DOI.
  2b. doi_versioned- DOI agrees after stripping a trailing '.N' version suffix
                     (F1000Research / Wellcome Open Research style).
  3. pmcid         - bare-numeric PMCID match.
  5. title_year    - (title_norm, pub_year) fallback, only against OA records
                     that lack a PMID (anything with a PMID was already caught
                     by stage 1). (Stage 4 in the source notebook is the
                     exploratory inspection step.)

Both upstreams (`openalex_works`, `ncbi_pubmed`) are read from
`get_output_root()`, not from raw snapshots, so this pipeline has no
`raw_path()`. Run the upstreams first.
"""

from __future__ import annotations

import gc
import re

import pandas as pd

from lacuna_etl.config import get_output_root
from lacuna_etl.core.identifiers import OpenAlexWorkId, PubmedId
from lacuna_etl.core.pipeline import DatasetPipeline
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.registry import register

_MATCH_SOURCES = {"pmid", "doi", "doi_versioned", "pmcid", "title_year"}

SCHEMA = {
    "pmid":         ColumnSpec(identifier=PubmedId,       required=True, description="PubMed identifier"),
    "work_id":      ColumnSpec(identifier=OpenAlexWorkId, required=True, description="OpenAlex work identifier"),
    "match_source": ColumnSpec(
        allowed_values=_MATCH_SOURCES,
        required=True,
        description=(
            "Rule that produced this link: 'pmid' (OpenAlex carried the PMID), "
            "'doi' (exact normalized DOI), 'doi_versioned' (DOI agreed after "
            "stripping a trailing '.N' version suffix), 'pmcid', or "
            "'title_year' (heuristic; only against OA records lacking a PMID)"
        ),
    ),
}


# DOI normalisation matches the notebook: a URL/`doi:` prefix in any case.
_DOI_URL_PREFIX_RE = re.compile(r"^\s*(?:https?://(?:dx\.)?doi\.org/|doi:\s*)", re.IGNORECASE)
_DOI_VERSION_RE = re.compile(r"\.\d{1,3}$")
_PMCID_DIGITS_RE = re.compile(r"(\d+)")
_TITLE_NONWORD_RE = re.compile(r"[^a-z0-9]+")


def _normalize_doi(s: pd.Series) -> pd.Series:
    out = s.astype("string").str.strip()
    out = out.str.replace(_DOI_URL_PREFIX_RE, "", regex=True)
    out = out.str.lower()
    return out.mask(out.eq(""))


def _strip_doi_version(s: pd.Series) -> pd.Series:
    return s.astype("string").str.replace(_DOI_VERSION_RE, "", regex=True)


def _normalize_pmcid(s: pd.Series) -> pd.Series:
    out = s.astype("string").str.extract(_PMCID_DIGITS_RE, expand=False)
    return out.mask(out.eq(""))


def _normalize_title(s: pd.Series) -> pd.Series:
    """Aggressive collapse to [a-z0-9]; floors at 4 chars so near-empty
    titles like '!?' don't collide."""
    out = s.astype("string").str.lower().str.replace(_TITLE_NONWORD_RE, "", regex=True)
    return out.mask(out.str.len() < 4)


@register
class PmidOpenalex(DatasetPipeline):
    name = "pmid_openalex"
    depends_on = ["openalex_works", "ncbi_pubmed"]

    def _pubmed_articles_path(self):
        return get_output_root() / "ncbi_pubmed" / "articles.parquet"

    def _openalex_works_dir(self):
        return get_output_root() / "openalex_works" / "works"

    # ---- stages 1, 2, 2b, 3 (identifier matches) --------------------------

    def extract(self) -> None:
        """Load minimal id columns from both upstreams and run all ID-based
        match stages in memory. Writes matched rows and the remaining unmapped
        PMIDs to intermediate parquet so the title-year stage can resume."""
        works = pd.read_parquet(
            self._openalex_works_dir(),
            columns=["work_id", "pmid", "doi", "pmcid"],
        )
        works["pmid"] = works["pmid"].astype("Int64")
        print(f"[{self.name}] openalex works rows: {len(works):,}")

        pubmed = pd.read_parquet(
            self._pubmed_articles_path(),
            columns=["pmid", "pmid_version", "doi", "pmc_id"],
        )
        pubmed["pmid"] = pubmed["pmid"].astype("Int64")
        # articles.parquet's grain is (pmid, pmid_version): some journals
        # publish versioned revisions of a paper. The crosswalk's grain is one
        # row per pmid, so collapse to the latest version per pmid.
        n_pre = len(pubmed)
        pubmed = (
            pubmed.sort_values(["pmid", "pmid_version"], ascending=[True, False])
                  .drop_duplicates(subset="pmid", keep="first")
                  .drop(columns="pmid_version")
                  .reset_index(drop=True)
        )
        n_collapsed = n_pre - len(pubmed)
        pubmed["work_id"] = pd.Series(pd.NA, index=pubmed.index, dtype="string")
        pubmed["match_source"] = pd.Series(pd.NA, index=pubmed.index, dtype="string")
        print(f"[{self.name}] pubmed rows: {len(pubmed):,} (collapsed {n_collapsed:,} extra versions)")

        # Stage 1 - direct PMID match. Same PMID under different work_ids does
        # occur in OA; keep the lexicographically first work_id for determinism.
        oa_with_pmid = (
            works.loc[works["pmid"].notna(), ["pmid", "work_id"]]
                 .sort_values("work_id")
        )
        pmid_to_work = (
            oa_with_pmid.drop_duplicates(subset="pmid", keep="first")
                        .set_index("pmid")["work_id"]
        )
        del oa_with_pmid
        filled = pubmed["pmid"].map(pmid_to_work)
        hits = filled.dropna()
        pubmed.loc[hits.index, "work_id"] = hits.values
        pubmed.loc[hits.index, "match_source"] = "pmid"
        print(f"[{self.name}] stage 1 (pmid): {len(hits):,} hits")
        del pmid_to_work, filled, hits; gc.collect()

        # Stage 2 - exact DOI after normalisation on both sides.
        pubmed["_doi_norm"] = _normalize_doi(pubmed["doi"])
        works["_doi_norm"] = _normalize_doi(works["doi"])
        oa_with_doi = (
            works.loc[works["_doi_norm"].notna(), ["_doi_norm", "work_id"]]
                 .sort_values("work_id")
        )
        doi_to_work = (
            oa_with_doi.drop_duplicates(subset="_doi_norm", keep="first")
                       .set_index("_doi_norm")["work_id"]
        )
        del oa_with_doi
        unmapped = pubmed["work_id"].isna()
        filled = pubmed.loc[unmapped, "_doi_norm"].map(doi_to_work)
        hits = filled.dropna()
        pubmed.loc[hits.index, "work_id"] = hits.values
        pubmed.loc[hits.index, "match_source"] = "doi"
        print(f"[{self.name}] stage 2 (doi): {len(hits):,} hits")
        del doi_to_work, filled, hits; gc.collect()

        # Stage 2b - tolerate F1000-style trailing '.N' DOI version mismatches.
        # When several OA work_ids share a base, the highest-numbered version
        # wins (sort _doi_norm desc, keep first); stage 2 above has already
        # absorbed the cases where the full DOIs agree.
        pubmed["_doi_base"] = _strip_doi_version(pubmed["_doi_norm"])
        works["_doi_base"] = _strip_doi_version(works["_doi_norm"])
        oa_with_base = (
            works.loc[works["_doi_base"].notna(), ["_doi_base", "_doi_norm", "work_id"]]
                 .sort_values(["_doi_norm", "work_id"], ascending=[False, True])
        )
        doi_base_to_work = (
            oa_with_base.drop_duplicates(subset="_doi_base", keep="first")
                        .set_index("_doi_base")["work_id"]
        )
        del oa_with_base
        unmapped = pubmed["work_id"].isna() & pubmed["_doi_base"].notna()
        filled = pubmed.loc[unmapped, "_doi_base"].map(doi_base_to_work)
        hits = filled.dropna()
        pubmed.loc[hits.index, "work_id"] = hits.values
        pubmed.loc[hits.index, "match_source"] = "doi_versioned"
        print(f"[{self.name}] stage 2b (doi_versioned): {len(hits):,} hits")
        del doi_base_to_work, filled, hits
        pubmed.drop(columns=["_doi_norm", "_doi_base"], inplace=True)
        works.drop(columns=["_doi_norm", "_doi_base"], inplace=True)
        gc.collect()

        # Stage 3 - PMCID, normalised to bare digits on both sides.
        pubmed["_pmcid_norm"] = _normalize_pmcid(pubmed["pmc_id"])
        works["_pmcid_norm"] = _normalize_pmcid(works["pmcid"])
        oa_with_pmcid = (
            works.loc[works["_pmcid_norm"].notna(), ["_pmcid_norm", "work_id"]]
                 .sort_values("work_id")
        )
        pmcid_to_work = (
            oa_with_pmcid.drop_duplicates(subset="_pmcid_norm", keep="first")
                         .set_index("_pmcid_norm")["work_id"]
        )
        del oa_with_pmcid, works; gc.collect()
        unmapped = pubmed["work_id"].isna()
        filled = pubmed.loc[unmapped, "_pmcid_norm"].map(pmcid_to_work)
        hits = filled.dropna()
        pubmed.loc[hits.index, "work_id"] = hits.values
        pubmed.loc[hits.index, "match_source"] = "pmcid"
        print(f"[{self.name}] stage 3 (pmcid): {len(hits):,} hits")
        del pmcid_to_work, filled, hits; gc.collect()
        pubmed.drop(columns=["_pmcid_norm"], inplace=True)

        matched = pubmed.loc[
            pubmed["work_id"].notna(), ["pmid", "work_id", "match_source"]
        ].reset_index(drop=True)
        unmapped_pmids = pubmed.loc[
            pubmed["work_id"].isna(), ["pmid"]
        ].reset_index(drop=True)

        self.save_parquet(matched, self.intermediate_path() / "matched_id_stages.parquet")
        self.save_parquet(unmapped_pmids, self.intermediate_path() / "unmapped_pmids.parquet")
        print(
            f"[{self.name}] after id stages: {len(matched):,} matched, "
            f"{len(unmapped_pmids):,} unmapped"
        )

    # ---- stage 5 (title + year heuristic, streamed) -----------------------

    def transform(self) -> None:
        """Streams openalex_works shards one parquet at a time. Peak memory is
        bounded by one shard + the pubmed-side (title,year) lookup, not by the
        whole OA works table."""
        matched = self.load_parquet(self.intermediate_path() / "matched_id_stages.parquet")
        unmapped_pmids = self.load_parquet(
            self.intermediate_path() / "unmapped_pmids.parquet"
        )["pmid"].astype("Int64")

        if unmapped_pmids.empty:
            self._write_final(matched)
            return

        # Pubmed-side (title_norm, pub_year) -> pmid for the unmapped tail.
        # A key shared by >1 pmid on the pubmed side can't be resolved safely.
        pm = pd.read_parquet(
            self._pubmed_articles_path(),
            columns=["pmid", "pmid_version", "title", "pub_year"],
        )
        pm["pmid"] = pm["pmid"].astype("Int64")
        # Same collapse to latest pmid_version as in extract().
        pm = (
            pm.sort_values(["pmid", "pmid_version"], ascending=[True, False])
              .drop_duplicates(subset="pmid", keep="first")
              .drop(columns="pmid_version")
        )
        pm = pm[pm["pmid"].isin(unmapped_pmids)].copy()
        pm["_title_norm"] = _normalize_title(pm["title"])
        pm["pub_year"] = pm["pub_year"].astype("Int64")
        pm = pm.dropna(subset=["_title_norm", "pub_year"])

        key_counts = pm.groupby(["_title_norm", "pub_year"])["pmid"].nunique()
        ambiguous = key_counts[key_counts > 1].index
        pm = pm[~pm.set_index(["_title_norm", "pub_year"]).index.isin(ambiguous)]

        pm_key_to_pmid: dict[tuple[str, int], int] = dict(
            zip(
                zip(pm["_title_norm"].tolist(), pm["pub_year"].astype(int).tolist()),
                pm["pmid"].astype(int).tolist(),
            )
        )
        print(
            f"[{self.name}] stage 5 lookup keys: {len(pm_key_to_pmid):,} "
            f"(dropped {len(ambiguous):,} ambiguous on the pubmed side)"
        )
        del pm, key_counts, ambiguous, unmapped_pmids; gc.collect()

        if not pm_key_to_pmid:
            self._write_final(matched)
            return

        files = sorted(self._openalex_works_dir().glob("*.parquet"))
        print(f"[{self.name}] stage 5: scanning {len(files):,} openalex_works shards")
        hits_parts: list[pd.DataFrame] = []
        for i, f in enumerate(files):
            df = pd.read_parquet(
                f, columns=["work_id", "pmid", "title", "publication_year"]
            )
            df = df[df["pmid"].isna()]
            if df.empty:
                continue
            df["_title_norm"] = _normalize_title(df["title"])
            df["_pub_year"] = df["publication_year"].astype("Int64")
            df = df.dropna(subset=["_title_norm", "_pub_year"])
            if df.empty:
                continue
            keys = list(
                zip(df["_title_norm"].tolist(), df["_pub_year"].astype(int).tolist())
            )
            mask = [k in pm_key_to_pmid for k in keys]
            if any(mask):
                hits_parts.append(
                    df.loc[mask, ["_title_norm", "_pub_year", "work_id"]]
                )
            if (i + 1) % 500 == 0:
                n = sum(len(p) for p in hits_parts)
                print(
                    f"[{self.name}] stage 5: scanned {i+1:,}/{len(files):,} "
                    f"shards, candidates so far: {n:,}"
                )

        if hits_parts:
            oa_hits = pd.concat(hits_parts, ignore_index=True)
        else:
            oa_hits = pd.DataFrame(columns=["_title_norm", "_pub_year", "work_id"])
        del hits_parts; gc.collect()

        # Drop OA-side ambiguity (same key under several work_ids).
        key_counts = oa_hits.groupby(["_title_norm", "_pub_year"])["work_id"].nunique()
        ambiguous = key_counts[key_counts > 1].index
        oa_hits = (
            oa_hits[~oa_hits.set_index(["_title_norm", "_pub_year"]).index.isin(ambiguous)]
            .drop_duplicates(subset=["_title_norm", "_pub_year"])
        )
        print(
            f"[{self.name}] stage 5 unambiguous (title,year)->work_id: "
            f"{len(oa_hits):,} (dropped {len(ambiguous):,} ambiguous on the OA side)"
        )

        oa_hits["pmid"] = [
            pm_key_to_pmid.get((t, int(y)))
            for t, y in zip(oa_hits["_title_norm"], oa_hits["_pub_year"])
        ]
        oa_hits = oa_hits.dropna(subset=["pmid"])
        title_year_matched = pd.DataFrame({
            "pmid":         pd.Series(oa_hits["pmid"].values, dtype="Int64"),
            "work_id":      pd.Series(oa_hits["work_id"].values, dtype="string"),
            "match_source": "title_year",
        })
        print(f"[{self.name}] stage 5 (title_year): {len(title_year_matched):,} hits")
        del oa_hits, pm_key_to_pmid, key_counts, ambiguous; gc.collect()

        combined = pd.concat([matched, title_year_matched], ignore_index=True)
        self._write_final(combined)

    def _write_final(self, df: pd.DataFrame) -> None:
        df = df[list(SCHEMA.keys())]
        # No PMID should appear twice: stages are mutually exclusive by construction.
        dups = df["pmid"].duplicated()
        if dups.any():
            raise ValueError(
                f"[{self.name}] duplicate pmids in crosswalk: "
                f"{df.loc[dups, 'pmid'].head(10).tolist()}"
            )
        df = self.apply_schema(df, SCHEMA)
        self.save_parquet(df, self.intermediate_path() / "pmid_openalex.parquet")

    def load(self) -> None:
        df = self.load_parquet(self.intermediate_path() / "pmid_openalex.parquet")
        self.save_parquet(df, self.output_path() / "pmid_openalex.parquet")
        self.save_schema_yaml(SCHEMA, "pmid_openalex")
