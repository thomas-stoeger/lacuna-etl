"""Contract-machinery tests — guard the validation/identifier logic against silent
regressions in the *code*. No data or downloaded snapshots required, so these run
anywhere and fast (``python -m unittest tests.test_contract``).

The downloaded datasets change over time; the pipelines defend against that by casting
and validating every identifier/required/allowed-values column at run time and aborting
loudly. These tests guard the machinery that defence relies on — patterns, the nullable/
required rules, the DOI version split, checksum normalisation, and that every dataset's
schema is well-formed — so a refactor cannot quietly weaken it.
"""
import re
import unittest

import pandas as pd
import polars as pl

from lacuna_etl.core import identifiers as ids
from lacuna_etl.core.identifiers import (
    ChemblId, CrossrefFunderDoi, Doi, EnsemblGeneId, EnsemblTranscriptId, GoId, Identifier, IssnL,
    MeshDescriptorId, MeshQualifierId, NlmUniqueId, NumericIdentifier, Orcid,
    PubmedId, ReactomePathwayId, UniprotAccession,
)
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets import REGISTRY


def _all_identifier_types():
    seen, stack = {}, [Identifier]
    while stack:
        for sub in stack.pop().__subclasses__():
            if sub.__name__ not in seen:
                seen[sub.__name__] = sub
                stack.append(sub)
    return list(seen.values())


class TestIdentifierPatterns(unittest.TestCase):
    def test_all_patterns_compile_and_are_anchorable(self):
        for idt in _all_identifier_types():
            if idt.pattern is not None:
                with self.subTest(identifier=idt.__name__):
                    re.compile(rf"^(?:{idt.pattern})$")

    # Format-level good/bad examples (validate_polars enforces the pattern; checksums
    # are a separate concern tested via normalize()). The point is to fail loudly if a
    # pattern is accidentally loosened or broken.
    FORMAT_EXAMPLES = {
        Doi: (["10.1038/nature12373", "10.1101/2023.03.28.534472"], ["nature12373", "10.1038", "doi:10.1/x"]),
        Orcid: (["0000-0002-1825-0097", "0000-0002-1825-009X"], ["0000-0002-1825", "0000000218250097"]),
        IssnL: (["2049-3630"], ["20493630", "2049-363"]),
        GoId: (["GO:0008150"], ["GO:8150", "0008150", "GO:00081500"]),
        EnsemblGeneId: (["ENSG00000139618", "ENSMUSG00000017167"], ["ENST00000380152", "ENSG"]),
        EnsemblTranscriptId: (["ENST00000380152"], ["ENSG00000139618"]),
        ReactomePathwayId: (["R-HSA-109582"], ["HSA-109582", "R-109582"]),
        ChemblId: (["CHEMBL25"], ["CHEM25", "25"]),
        NlmUniqueId: (["7501160", "2984730R"], ["ABC", "12-34"]),
        MeshDescriptorId: (["D000445"], ["Q000378", "000445"]),
        MeshQualifierId: (["Q000378"], ["D000445"]),
        UniprotAccession: (["P48347", "A0A0B4J2F2"], ["P48347-2", "p48347", "PETER"]),
        CrossrefFunderDoi: (["10.13039/100000002"], ["10.1038/nature12373", "100000002",
                                                     "https://doi.org/10.13039/100000002"]),
    }

    def test_format_examples(self):
        for idt, (good, bad) in self.FORMAT_EXAMPLES.items():
            with self.subTest(identifier=idt.__name__, kind="good"):
                idt.validate_polars(pl.Series("col", good))  # must not raise
            for value in bad:
                with self.subTest(identifier=idt.__name__, bad=value):
                    with self.assertRaises(ValueError):
                        idt.validate_polars(pl.Series("col", [value]))


class TestNumericIdentifier(unittest.TestCase):
    def test_positive_ints_ok_nulls_tolerated(self):
        PubmedId.validate_polars(pl.Series("pmid", [1, 2, None], dtype=pl.Int64))

    def test_nonpositive_rejected(self):
        for bad in ([0], [-3]):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    PubmedId.validate_polars(pl.Series("pmid", bad, dtype=pl.Int64))

    def test_required_rejects_nulls(self):
        with self.assertRaises(ValueError):
            PubmedId.validate_polars(pl.Series("pmid", [1, None], dtype=pl.Int64), required=True)


class TestColumnSpecValidation(unittest.TestCase):
    """Locks in the pandas validation contract (nullable numerics, uniform `required`)."""

    def test_nullable_numeric_passes(self):
        ColumnSpec(identifier=PubmedId).validate(pd.Series([1, 2, pd.NA], dtype="Int64"))

    def test_numeric_positivity_on_non_null(self):
        with self.assertRaises(ValueError):
            ColumnSpec(identifier=PubmedId).validate(pd.Series([1, -2, pd.NA], dtype="Int64"))

    def test_required_rejects_nulls_numeric_and_string(self):
        with self.assertRaises(ValueError):
            ColumnSpec(identifier=PubmedId, required=True).validate(pd.Series([1, pd.NA], dtype="Int64"))
        with self.assertRaises(ValueError):
            ColumnSpec(required=True).validate(pd.Series(["a", None], dtype="string"))

    def test_non_required_string_null_ok(self):
        ColumnSpec().validate(pd.Series(["a", None], dtype="string"))

    def test_allowed_values_rejects_unknown(self):
        spec = ColumnSpec(allowed_values={"A", "B"})
        spec.validate(pd.Series(["A", "B", None], dtype="string"))
        with self.assertRaises(ValueError):
            spec.validate(pd.Series(["A", "C"], dtype="string"))


class TestDoiVersionSplit(unittest.TestCase):
    """The article-vs-version split is intricate and registrant-anchored; a source DOI
    quirk could silently break it, so pin the behaviour on known cases."""

    VERSIONED = {
        "10.12688/f1000research.13457.2": "10.12688/f1000research.13457",   # F1000 .N
        "10.1101/2023.03.28.534472v2": "10.1101/2023.03.28.534472",         # bioRxiv vN (no separator)
        "10.21203/rs.3.rs-12345/v1": "10.21203/rs.3.rs-12345",              # Research Square /vN
        "10.20944/preprints202001.0001.v1": "10.20944/preprints202001.0001",  # Preprints.org .vN
    }
    # Must NOT be treated as versioned (these end in numbers / vN-looking tokens but are not):
    NOT_VERSIONED = [
        "10.1038/nature12373",
        "10.1016/j.cell.2020.01.001",       # Elsevier page id ending in digits
        "10.3390/v1010072",                  # 'Viruses' journal — 'v' then digits, not a version
        "10.1101/gad.123456",                # CSH journal (letter-led), shares bioRxiv registrant
        "10.1101/2023.03.28.534472",         # the bioRxiv base itself
    ]

    def test_versioned_split(self):
        for doi, base in self.VERSIONED.items():
            with self.subTest(doi=doi):
                self.assertTrue(Doi.is_versioned(doi))
                self.assertEqual(Doi.split_version(doi), (base, doi))

    def test_not_versioned_pass_through(self):
        for doi in self.NOT_VERSIONED:
            with self.subTest(doi=doi):
                self.assertFalse(Doi.is_versioned(doi))
                self.assertEqual(Doi.split_version(doi), (doi, None))

    def test_validate_rejects_surviving_version(self):
        with self.assertRaises(ValueError):
            Doi.validate(pd.Series(["10.1101/2023.03.28.534472v2"], dtype="string"))


class TestChecksumNormalisation(unittest.TestCase):
    def test_orcid_checksum(self):
        self.assertEqual(Orcid.normalize("0000-0002-1825-0097"), "0000-0002-1825-0097")
        self.assertEqual(Orcid.normalize("https://orcid.org/0000-0002-1825-0097"), "0000-0002-1825-0097")
        self.assertIsNone(Orcid.normalize("0000-0002-1825-0096"))  # bad check digit

    def test_issnl_checksum(self):
        self.assertEqual(IssnL.normalize("2049-3630"), "2049-3630")
        self.assertIsNone(IssnL.normalize("2049-3631"))  # bad check digit


class TestSchemaDeclarations(unittest.TestCase):
    """Every registered dataset must expose well-formed schemas (drives sidecars,
    validation, and ``etl check``)."""

    def test_all_datasets_have_wellformed_schemas(self):
        self.assertTrue(REGISTRY, "no datasets registered")
        for name, cls in sorted(REGISTRY.items()):
            with self.subTest(dataset=name):
                schemas = cls().expected_schemas()
                self.assertIsInstance(schemas, dict)
                self.assertTrue(schemas, f"{name}: no tables")
                for table, cols in schemas.items():
                    self.assertTrue(cols, f"{name}/{table}: empty schema")
                    for col, spec in cols.items():
                        self.assertIsInstance(spec, ColumnSpec)
                        if spec.identifier is not None:
                            self.assertTrue(issubclass(spec.identifier, Identifier))
                        if spec.allowed_values is not None:
                            self.assertIsInstance(spec.allowed_values, set)


if __name__ == "__main__":
    unittest.main()
