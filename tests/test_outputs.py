"""Output-invariant tests — guard against a future dataset snapshot breaking logic
*silently*. Run against the cleaned outputs after a run (``python -m unittest
tests.test_outputs``); skipped when no ``ETL_OUTPUT_ROOT`` outputs are present.

The pipelines already validate identifier / required / allowed-values columns at run
time and abort on a violation, and ``etl check`` guards sidecar drift. These tests cover
the invariants that are *not* otherwise enforced and would otherwise pass silently:

  * non-emptiness — a renamed/changed source column or an over-eager filter can yield a
    table with zero rows without raising;
  * grain uniqueness — a source change can introduce duplicate keys, which no schema
    check catches but which silently breaks every downstream join.

Grain coverage is a representative set of the main parent tables (verified unique on the
current outputs); extend GRAINS as new keyed tables are added.
"""
import unittest

import polars as pl

from lacuna_etl.config import get_output_root
from lacuna_etl.datasets import REGISTRY

OUTPUT_ROOT = get_output_root()


def _table_paths(dataset: str, table: str) -> list:
    """Parquet file(s) for a table: a single file, or the shards under a directory."""
    single = OUTPUT_ROOT / dataset / f"{table}.parquet"
    if single.exists():
        return [single]
    shard_dir = OUTPUT_ROOT / dataset / table
    return sorted(shard_dir.rglob("*.parquet")) if shard_dir.exists() else []


def _outputs_available() -> bool:
    return OUTPUT_ROOT.exists() and any((OUTPUT_ROOT / name).exists() for name in REGISTRY)


OUTPUTS_AVAILABLE = _outputs_available()

# Representative parent-table grains (dataset, table, key columns), each verified unique
# on the current outputs. A duplicate here means a source update silently broke the grain.
GRAINS = [
    ("icite", "icite", ["pmid"]),
    ("ncbi_gene_info", "gene_info", ["entrez_id"]),
    ("ncbi_pubmed", "articles", ["pmid", "pmid_version"]),
    ("ncbi_gene_history", "gene_history", ["discontinued_gene_id"]),
    ("ncbi_taxdump", "taxonomy_nodes", ["tax_id"]),
    ("hgnc", "genes", ["hgnc_id"]),
    ("proteinatlas", "genes", ["ensembl_gene_id"]),
    ("omim", "mim2gene", ["mim_number"]),
    ("disease_ontology", "terms", ["doid"]),
    ("geneontology_basic", "terms", ["go_id"]),
    ("mesh", "descriptors", ["descriptor_ui"]),
    ("reactome", "pathways", ["pathway_id"]),
    ("opentargets_target", "targets", ["target_id"]),
    ("opentargets_disease", "diseases", ["disease_id"]),
    ("ror", "organizations", ["ror_id"]),
    ("nih_exporter", "projects", ["application_id"]),
    ("nsf_awards", "awards", ["award_id"]),
    ("openalex_awards", "awards", ["award_id"]),
]


@unittest.skipUnless(OUTPUTS_AVAILABLE, f"no ETL outputs under {OUTPUT_ROOT}")
class TestOutputsNonEmpty(unittest.TestCase):
    def test_every_present_table_is_non_empty(self):
        checked = 0
        for name, cls in sorted(REGISTRY.items()):
            for table in cls().expected_schemas():
                paths = _table_paths(name, table)
                if not paths:
                    continue  # not produced in this environment; etl check covers "missing"
                with self.subTest(dataset=name, table=table):
                    rows = pl.scan_parquet(paths).select(pl.len()).collect().item()
                    self.assertGreater(rows, 0, f"{name}/{table} has 0 rows")
                checked += 1
        self.assertGreater(checked, 0, "no output tables found to check")


@unittest.skipUnless(OUTPUTS_AVAILABLE, f"no ETL outputs under {OUTPUT_ROOT}")
class TestGrainUniqueness(unittest.TestCase):
    def test_documented_grains_are_unique(self):
        for dataset, table, keys in GRAINS:
            paths = _table_paths(dataset, table)
            if not paths:
                continue
            with self.subTest(dataset=dataset, table=table, keys=keys):
                key_df = pl.scan_parquet(paths).select(keys).collect()
                dups = key_df.height - key_df.unique().height
                self.assertEqual(dups, 0, f"{dataset}/{table}: {dups} duplicate {keys} rows")


if __name__ == "__main__":
    unittest.main()
