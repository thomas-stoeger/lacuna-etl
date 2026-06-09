from __future__ import annotations

import re
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    import polars as pl

_DOI_URL_PREFIX = re.compile(r"^https?://(?:doi\.org/)?")
_WIKIDATA_URL_PREFIX = re.compile(r"^https?://(?:www\.)?wikidata\.org/(?:wiki|entity)/")

# Versioned-DOI detection. A handful of publishers append an *article version* to
# the article DOI; two records of the same article then differ only by version,
# which breaks DOI joins. We split those into an article-level base DOI plus the
# original versioned form. Detection is a curated, registrant-anchored allowlist:
# a blanket "trailing .N/vN" rule is unsafe because most DOIs legitimately end in
# numbers (Elsevier page ids, journal 'Viruses' 10.3390/v1010072, arXiv/SSRN ids).
# Each entry is (article-base pattern, version-suffix pattern), validated against
# real snapshot data. Add a registrant here to recognise its versions.
_DOI_VERSION_RULES: list[tuple[str, str]] = [
    # F1000 platform (F1000Research, Wellcome/Gates/... Open Research): structured
    # as <journal>.<article>.<version>; the version is a bare .N (or legacy .vN).
    # The base is anchored to <journal>.<article> (each dotless) so the article
    # base 10.12688/f1000research.13457 is NOT itself mistaken for a version.
    (r"10\.12688/[^/\s.]+\.[^/\s.]+", r"\.v?\d+"),
    (r"10\.21203/\S+", r"/v\d+"),            # Research Square: /vN
    (r"10\.20944/\S+", r"\.v\d+"),           # Preprints.org: .vN
    (r"10\.26434/\S+", r"(?:\.v\d+|/v\d+)"), # ChemRxiv: .vN or /vN
    (r"10\.36227/\S+", r"(?:\.v\d+|/v\d+)"), # TechRxiv: .vN or /vN
    (r"10\.22541/\S+", r"/v\d+"),            # Authorea / ESSOAr: /vN
    (r"10\.32388/\S+", r"\.\d{1,2}"),        # Qeios: .N (1-2 digits; a 4-digit year in a slug, e.g. '.../...2025.2', is not a version)
    # bioRxiv / medRxiv (Cold Spring Harbor): the version is 'vN' appended directly
    # to the article number with NO separator (10.1101/2023.03.28.534472v2). The
    # base must be digit-led to exclude the CSH Press journals sharing this
    # registrant (10.1101/gad..., gr..., sqb..., which are letter-led). A bare
    # trailing 'vN' is far too common in unrelated DOIs (random ids, GBIF/ICPSR
    # deposits) to recognise outside this specific registrant.
    (r"10\.1101/\d[^/\s]*", r"v\d+"),
]
# Registrant-anchored GATE: is this a versioned DOI? (string form for polars too.)
_DOI_VERSIONED_PAT = "(?:" + "|".join(f"(?:{p}){s}" for p, s in _DOI_VERSION_RULES) + ")"
_DOI_VERSIONED = re.compile(rf"^{_DOI_VERSIONED_PAT}$")
# Generic trailing version-token strip. Broad, but only ever applied to values the
# GATE has already accepted, so it safely removes just the final version token.
# The bare 'vN' alternative is last so separator forms (/vN, .vN, .N) win when
# present; it only fires for the separator-less bioRxiv/medRxiv form.
_DOI_VERSION_SUFFIX_PAT = r"(?:/v\d+|\.v\d+|\.\d+|v\d+)$"
_DOI_VERSION_SUFFIX = re.compile(_DOI_VERSION_SUFFIX_PAT)


class Identifier:
    """Base class for column identifier types.

    Subclasses declare:
      dtype       - pandas dtype used by `cast` (for pandas-backed pipelines)
      pattern     - regex (without anchors) describing valid string values; used
                    by polars validation. None = no pattern check.
    """

    dtype: object = None
    pattern: str | None = None

    @classmethod
    def cast(cls, s: pd.Series) -> pd.Series:
        return s.astype(cls.dtype)

    @classmethod
    def validate(cls, s: pd.Series) -> None:  # noqa: ARG003
        pass

    @classmethod
    def validate_polars(cls, s: "pl.Series", *, required: bool = False) -> None:
        """Validate a polars Series against this identifier's contract.

        Empty strings are treated as null (a common OpenAlex convention).
        - if `required` and there are nulls or empties -> ValueError
        - if `pattern` is set, every non-null value must fully match -> ValueError
        """
        import polars as pl

        if s.dtype == pl.String:
            s = s.str.strip_chars()
            s = s.set(s == "", None)

        nulls = s.null_count()
        if required and nulls > 0:
            raise ValueError(f"{cls.__name__}: column {s.name!r} has {nulls} nulls (required)")

        if cls.pattern is None:
            return

        non_null = s.drop_nulls()
        if non_null.len() == 0:
            return

        bad_mask = ~non_null.str.contains(rf"^(?:{cls.pattern})$")
        if bad_mask.any():
            bad = non_null.filter(bad_mask)
            sample = bad.head(5).to_list()
            raise ValueError(
                f"{cls.__name__}: column {s.name!r} has {bad.len()} values "
                f"not matching r'{cls.pattern}', e.g. {sample}"
            )


class NumericIdentifier(Identifier):
    dtype = pd.Int64Dtype()

    @classmethod
    def parse(cls, value: object) -> int | None:
        """Parse a single source value into a positive Python int, or None.

        Accepts None, ints, and strings. Empty/whitespace-only strings become None.
        Non-numeric strings, floats, booleans, and non-positive values raise
        ValueError — there are no silent drops.
        """
        if value is None:
            return None
        if isinstance(value, bool):
            raise ValueError(f"{cls.__name__}: refusing to coerce bool: {value!r}")
        if isinstance(value, int):
            n = value
        elif isinstance(value, str):
            s = value.strip()
            if not s:
                return None
            try:
                n = int(s)
            except ValueError as e:
                raise ValueError(f"{cls.__name__}: not a numeric string: {value!r}") from e
        else:
            raise ValueError(f"{cls.__name__}: unsupported type {type(value).__name__}: {value!r}")
        if n <= 0:
            raise ValueError(f"{cls.__name__}: IDs must be positive, got {n!r}")
        return n

    @classmethod
    def validate(cls, s: pd.Series) -> None:
        if not pd.api.types.is_integer_dtype(s):
            raise TypeError(f"{cls.__name__}: expected integer dtype, got {s.dtype}")
        if s.isna().any():
            raise ValueError(f"{cls.__name__}: unexpected null values")
        if (s <= 0).any():
            raise ValueError(f"{cls.__name__}: IDs must be positive")

    @classmethod
    def validate_polars(cls, s: "pl.Series", *, required: bool = False) -> None:
        """Validate an integer polars Series: nulls (if required) and positivity."""
        import polars as pl

        if not s.dtype.is_integer():
            raise TypeError(f"{cls.__name__}: expected integer dtype, got {s.dtype}")
        nulls = s.null_count()
        if required and nulls > 0:
            raise ValueError(f"{cls.__name__}: column {s.name!r} has {nulls} nulls (required)")
        non_null = s.drop_nulls()
        if non_null.len() == 0:
            return
        bad_mask = non_null <= 0
        n_bad = int(bad_mask.sum())
        if n_bad:
            sample = non_null.filter(bad_mask).head(5).to_list()
            raise ValueError(
                f"{cls.__name__}: column {s.name!r} has {n_bad} non-positive values, e.g. {sample}"
            )


class NcbiTaxId(NumericIdentifier):
    pass


class NcbiGeneId(NumericIdentifier):
    pass


class PubmedId(NumericIdentifier):
    pass


class RetractionWatchId(NumericIdentifier):
    """Retraction Watch internal record identifier (positive Int64).

    Dataset-internal key for the Retraction Watch database; not cross-referenced
    by other datasets, but validated to the same positive-integer contract.
    """
    pass


class NlmUniqueId(Identifier):
    """NLM Catalog unique identifier (``NlmUniqueID``), e.g. ``'101549428'`` or
    ``'9919253715206676'``.

    Almost always all digits; some older records carry a trailing check letter
    (``'2984730R'``). Stored as a string rather than an integer because of that
    letter and because it is an opaque NLM-internal catalog key, not a numeric
    quantity cross-referenced by other datasets. ``normalize`` strips stray
    control / whitespace characters (some related-record IDs carry a trailing
    bidi mark) and returns the value only if it matches the canonical shape,
    else None.
    """
    dtype = pd.StringDtype()
    pattern = r"\d+[A-Z]?"

    @classmethod
    def normalize(cls, value: str | None) -> str | None:
        if value is None or not isinstance(value, str):
            return None
        import unicodedata

        cleaned = "".join(ch for ch in value if unicodedata.category(ch)[0] not in ("C", "Z"))
        cleaned = cleaned.strip()
        if not cleaned:
            return None
        return cleaned if re.fullmatch(r"\d+[A-Z]?", cleaned) else None

    @classmethod
    def validate(cls, s: pd.Series) -> None:
        non_null = s.dropna()
        bad = non_null[~non_null.str.fullmatch(r"\d+[A-Z]?")]
        if not bad.empty:
            raise ValueError(f"NlmUniqueId: malformed values {bad.head(5).tolist()}")


class GoId(Identifier):
    """Gene Ontology term ID, e.g. 'GO:0008150'.

    Keys the `geneontology_basic` term graph and every edge that references a term.
    Enforced in pandas as well as polars (the `geneontology_basic` pipeline is
    pandas-backed and the GO id is its grain key), unlike most string identifiers.
    """
    dtype = pd.StringDtype()
    pattern = r"GO:\d{7}"

    @classmethod
    def validate(cls, s: pd.Series) -> None:
        non_null = s.dropna()
        bad = non_null[~non_null.str.match(rf"^(?:{cls.pattern})$")]
        if not bad.empty:
            raise ValueError(
                f"GoId: {len(bad)} values are not a canonical GO id "
                f"(r'{cls.pattern}'), e.g. {bad.head(5).tolist()}"
            )


class DiseaseOntologyId(Identifier):
    """Human Disease Ontology term ID, e.g. 'DOID:0001816'.

    The canonical DOID curie: the literal prefix ``DOID:`` followed by digits. It
    keys the ``disease_ontology`` term graph and every edge that references a term
    (parents, alternate/merged IDs, disjoint-from, obsolete replacements). The
    ontology's external cross-references (MESH, UMLS_CUI, ORDO, …) are heterogeneous
    CURIEs with no single canonical form and stay plain strings (the Open Targets
    disease-ID precedent); only the native DOID gets this type.

    Unlike most string identifiers (whose pattern is enforced only in polars), this
    type also enforces its pattern in pandas, since ``disease_ontology`` is
    pandas-backed and the DOID is the grain key of every table.
    """
    dtype = pd.StringDtype()
    pattern = r"DOID:\d+"

    @classmethod
    def validate(cls, s: pd.Series) -> None:
        non_null = s.dropna()
        bad = non_null[~non_null.str.match(rf"^(?:{cls.pattern})$")]
        if not bad.empty:
            raise ValueError(
                f"DiseaseOntologyId: {len(bad)} values are not a canonical DOID curie "
                f"(r'{cls.pattern}'), e.g. {bad.head(5).tolist()}"
            )


class AllianceGeneId(Identifier):
    """Alliance of Genome Resources canonical gene curie.

    The Alliance keys every gene by the contributing model-organism database's own
    curie: one of ``HGNC`` (human), ``MGI`` (mouse), ``RGD`` (rat), ``ZFIN``
    (zebrafish), ``FB`` (FlyBase), ``WB`` (WormBase), ``SGD`` (yeast), or
    ``Xenbase`` (frog), followed by that database's accession (``HGNC:5``,
    ``WB:WBGene00022277``, ``ZFIN:ZDB-GENE-020812-2``, …). No single accession
    shape spans the eight databases, so the prefix set is the canonical anchor.
    NCBI cross-references these in ``gene_info`` as ``AllianceGenome:<curie>``,
    which is how ``ncbi_gene2_alliance`` links Entrez genes to Alliance genes.

    Unlike most string identifiers (whose pattern is enforced only by
    ``validate_polars``), this type also enforces the pattern in pandas, since the
    Alliance pipelines are pandas-backed and the crosswalk join depends on the form.
    """
    dtype = pd.StringDtype()
    pattern = r"(?:HGNC|MGI|RGD|ZFIN|SGD|FB|WB|Xenbase):\S+"

    @classmethod
    def validate(cls, s: pd.Series) -> None:
        non_null = s.dropna()
        bad = non_null[~non_null.str.match(rf"^(?:{cls.pattern})$")]
        if not bad.empty:
            raise ValueError(
                f"AllianceGeneId: {len(bad)} values are not a recognised Alliance gene "
                f"curie (prefix in HGNC/MGI/RGD/ZFIN/SGD/FB/WB/Xenbase), "
                f"e.g. {bad.head(5).tolist()}"
            )


class HgncId(Identifier):
    """HGNC gene identifier, e.g. 'HGNC:5'.

    The HUGO Gene Nomenclature Committee's stable accession for a human gene: the
    literal prefix ``HGNC:`` followed by digits. It is the grain key of the ``hgnc``
    tables and the human prefix of the ``AllianceGeneId`` curie (the Alliance keys
    human genes by their HGNC id), so the two forms coincide for human genes; NCBI's
    ``gene_info`` also cross-references it as ``HGNC:<id>`` in ``db_xrefs``.

    Unlike most string identifiers (whose pattern is checked only in polars), this
    type also enforces its pattern in pandas, since the ``hgnc`` pipeline is
    pandas-backed and the HGNC id is the key of every table.
    """
    dtype = pd.StringDtype()
    pattern = r"HGNC:\d+"

    @classmethod
    def validate(cls, s: pd.Series) -> None:
        non_null = s.dropna()
        bad = non_null[~non_null.str.match(rf"^(?:{cls.pattern})$")]
        if not bad.empty:
            raise ValueError(
                f"HgncId: {len(bad)} values are not a canonical HGNC id "
                f"(r'{cls.pattern}'), e.g. {bad.head(5).tolist()}"
            )


class UniprotAccession(Identifier):
    """UniProtKB accession number, e.g. 'P12345', 'Q70XZ5', 'A0A804MTU9'.

    The canonical 6- or 10-character UniProtKB accession, matched by UniProt's own
    official accession regex. Enforced in pandas (the ``unknome`` pipeline that uses
    it is pandas-backed), unlike most string identifiers whose pattern is checked
    only in polars. Isoform suffixes (``-2``) are not part of the base accession and
    do not match.
    """
    dtype = pd.StringDtype()
    pattern = r"[OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z][0-9](?:[A-Z][A-Z0-9]{2}[0-9]){1,2}"

    @classmethod
    def validate(cls, s: pd.Series) -> None:
        non_null = s.dropna()
        bad = non_null[~non_null.str.match(rf"^(?:{cls.pattern})$")]
        if not bad.empty:
            raise ValueError(
                f"UniprotAccession: {len(bad)} values are not a valid UniProt accession, "
                f"e.g. {bad.head(5).tolist()}"
            )


class _MeshUI(Identifier):
    """Base for MeSH unique identifiers (NLM Medical Subject Headings).

    Each MeSH record type has a single-letter-prefixed UI: descriptors ``D``,
    qualifiers ``Q``, supplementary concept records ``C``, concepts ``M``, terms
    ``T``. The pattern is enforced in pandas (the ``mesh`` pipeline is pandas-backed),
    unlike most string identifiers whose pattern is checked only in polars.
    """
    dtype = pd.StringDtype()

    @classmethod
    def validate(cls, s: pd.Series) -> None:
        non_null = s.dropna()
        bad = non_null[~non_null.str.match(rf"^(?:{cls.pattern})$")]
        if not bad.empty:
            raise ValueError(
                f"{cls.__name__}: {len(bad)} values not matching r'{cls.pattern}', "
                f"e.g. {bad.head(5).tolist()}"
            )


class MeshDescriptorId(_MeshUI):
    """MeSH descriptor (main heading) UI, e.g. 'D000001'."""
    pattern = r"D\d+"


class MeshQualifierId(_MeshUI):
    """MeSH qualifier (subheading) UI, e.g. 'Q000008'."""
    pattern = r"Q\d+"


class MeshSupplementalId(_MeshUI):
    """MeSH Supplementary Concept Record UI, e.g. 'C000002'."""
    pattern = r"C\d+"


class MeshConceptId(_MeshUI):
    """MeSH concept UI, e.g. 'M0000001'."""
    pattern = r"M\d+"


class MeshTermId(_MeshUI):
    """MeSH term UI, e.g. 'T000002'."""
    pattern = r"T\d+"


class Doi(Identifier):
    """Article-level DOI: URL prefix stripped, and no publisher version suffix.

    A versioned DOI (F1000 ``.N``/``.vN``, Research Square ``/vN``, etc.) is split
    into this article-level base plus a sibling ``DoiVersioned`` column; see
    ``split_version`` and ``_DOI_VERSION_RULES``. ``validate`` enforces that no
    version suffix survives, so a ``Doi`` column always refers to the article.
    """
    dtype = pd.StringDtype()
    pattern = r"10\.[^/\s]+/\S+"

    @classmethod
    def shorten(cls, value: str | None) -> str | None:
        """Strip URL prefix and surrounding whitespace from a single DOI string. Empty input -> None."""
        if value is None or not isinstance(value, str):
            return value
        stripped = _DOI_URL_PREFIX.sub("", value).strip()
        return stripped or None

    @classmethod
    def is_versioned(cls, value: str | None) -> bool:
        """True if `value` is a recognised versioned DOI (curated registrant + suffix)."""
        return isinstance(value, str) and _DOI_VERSIONED.match(value) is not None

    @classmethod
    def split_version(cls, value: str | None) -> tuple[str | None, str | None]:
        """Split a single DOI into (article_base, versioned_or_None).

        For a recognised versioned DOI, the article base has the version token
        stripped and the second element keeps the full versioned form. For any
        other value (including None / non-versioned DOIs) the value passes through
        unchanged and the second element is None.
        """
        if not cls.is_versioned(value):
            return value, None
        base = _DOI_VERSION_SUFFIX.sub("", value)
        return base, value

    @classmethod
    def cast(cls, s: pd.Series) -> pd.Series:
        s = s.astype(cls.dtype)
        s = s.str.replace(_DOI_URL_PREFIX, "", regex=True)
        return s

    @classmethod
    def validate(cls, s: pd.Series) -> None:
        non_null = s.dropna()
        bad = non_null[non_null.str.startswith(("http://", "https://"))]
        if not bad.empty:
            raise ValueError(f"Doi: URL prefix not stripped from {bad.head(5).tolist()}")
        versioned = non_null[non_null.str.match(_DOI_VERSIONED)]
        if not versioned.empty:
            raise ValueError(
                f"Doi: article DOI carries a version suffix, e.g. {versioned.head(5).tolist()} "
                f"(split into a sibling DoiVersioned column instead)"
            )

    @classmethod
    def validate_polars(cls, s: "pl.Series", *, required: bool = False) -> None:
        super().validate_polars(s, required=required)
        import polars as pl

        if s.dtype != pl.String:
            return
        non_null = s.drop_nulls()
        if non_null.len() == 0:
            return
        bad = non_null.filter(non_null.str.contains(_DOI_VERSIONED.pattern))
        if bad.len():
            raise ValueError(
                f"Doi: column {s.name!r} has {bad.len()} article DOIs with a version "
                f"suffix, e.g. {bad.head(5).to_list()} (split into a DoiVersioned column)"
            )


class DoiVersioned(Doi):
    """Article DOI that MAY carry a publisher version suffix (supports but does not
    require versioning).

    Same canonical ``10.x/...`` shape and URL stripping as ``Doi``, but the version
    suffix is allowed: this is the sibling column that preserves the original
    versioned DOI when ``Doi`` holds the version-stripped article base.
    """

    @classmethod
    def validate(cls, s: pd.Series) -> None:
        non_null = s.dropna()
        bad = non_null[non_null.str.startswith(("http://", "https://"))]
        if not bad.empty:
            raise ValueError(f"DoiVersioned: URL prefix not stripped from {bad.head(5).tolist()}")

    @classmethod
    def validate_polars(cls, s: "pl.Series", *, required: bool = False) -> None:
        # versions are allowed here, so use the base pattern check only (skip Doi's
        # version-reject by going straight to Identifier).
        super(Doi, cls).validate_polars(s, required=required)


def doi_base_expr(col: str) -> "pl.Expr":
    """Polars expr: article-level DOI for `col` (version token stripped on
    recognised versioned DOIs, all other values passed through unchanged)."""
    import polars as pl

    c = pl.col(col)
    return (
        pl.when(c.str.contains(_DOI_VERSIONED.pattern))
        .then(c.str.replace(_DOI_VERSION_SUFFIX_PAT, ""))
        .otherwise(c)
    )


def doi_versioned_expr(col: str) -> "pl.Expr":
    """Polars expr: the full versioned DOI for `col` on recognised versioned DOIs,
    null otherwise (the sparse sibling column)."""
    import polars as pl

    c = pl.col(col)
    return pl.when(c.str.contains(_DOI_VERSIONED.pattern)).then(c).otherwise(None)


def split_doi_version_pandas(s: pd.Series) -> tuple[pd.Series, pd.Series]:
    """Vectorised pandas split: returns (article_base, versioned_or_null) for a
    DOI Series. `base` strips the version token on recognised versioned DOIs and
    passes everything else through; `versioned` is the full DOI where recognised,
    else null."""
    s = s.astype("string")
    is_ver = s.str.match(_DOI_VERSIONED).fillna(False)
    versioned = s.where(is_ver)
    base = s.mask(is_ver, s.str.replace(_DOI_VERSION_SUFFIX, "", regex=True))
    return base, versioned


class WikidataId(Identifier):
    """Wikidata Q-identifier, e.g. 'Q42'. URL forms are stripped via `shorten`."""
    dtype = pd.StringDtype()
    pattern = r"Q\d+"

    @classmethod
    def shorten(cls, value: str | None) -> str | None:
        if value is None or not isinstance(value, str):
            return value
        return _WIKIDATA_URL_PREFIX.sub("", value)


class Orcid(Identifier):
    """ORCID iD in canonical hyphenated form, e.g. '0000-0002-1825-0097'.

    `normalize` accepts any common variant (URL-prefixed, dashless, missing check digit)
    and returns the canonical form, or None if the value is unrecoverable / fails its
    ISO 7064 MOD 11-2 check digit.
    """
    dtype = pd.StringDtype()
    pattern = r"\d{4}-\d{4}-\d{4}-\d{3}[\dX]"

    @staticmethod
    def _checksum(fifteen_digits: str) -> str:
        """Compute the ORCID check digit (ISO 7064 MOD 11-2) for the leading 15 digits."""
        total = 0
        for ch in fifteen_digits:
            total = (total + int(ch)) * 2
        result = (12 - total % 11) % 11
        return "X" if result == 10 else str(result)

    @classmethod
    def normalize(cls, value: str | None) -> str | None:
        """Normalize an ORCID-like value to canonical 'XXXX-XXXX-XXXX-XXXX' form.

        Strips URL prefix and hyphens; if 15 digits, recovers the check digit; if 16
        chars (15 digits + check digit/X), validates it. Returns None for malformed or
        bad-checksum input.
        """
        if value is None or not isinstance(value, str):
            return None
        s = value.strip().removeprefix("https://orcid.org/").removeprefix("http://orcid.org/")
        bare = s.replace("-", "")
        if not bare:
            return None
        if len(bare) == 15 and bare.isdigit():
            check = cls._checksum(bare)
            full = bare + check
        elif len(bare) == 16 and bare[:15].isdigit() and bare[15] in "0123456789X":
            if cls._checksum(bare[:15]) != bare[15]:
                return None
            full = bare
        else:
            return None
        return f"{full[0:4]}-{full[4:8]}-{full[8:12]}-{full[12:16]}"


class RorId(Identifier):
    """Research Organization Registry ID, canonical URL form, e.g. 'https://ror.org/01an7q238'.

    Keys the `ror` organization tables (and every relationship edge). Enforced in
    pandas as well as polars (the `ror` pipeline is pandas-backed and keys on it),
    unlike most string identifiers.
    """
    dtype = pd.StringDtype()
    pattern = r"https://ror\.org/[a-z0-9]+"

    @classmethod
    def validate(cls, s: pd.Series) -> None:
        non_null = s.dropna()
        bad = non_null[~non_null.str.match(rf"^(?:{cls.pattern})$")]
        if not bad.empty:
            raise ValueError(
                f"RorId: {len(bad)} values are not a canonical ROR URL "
                f"(r'{cls.pattern}'), e.g. {bad.head(5).tolist()}"
            )


class IssnL(Identifier):
    """Linking ISSN in canonical hyphenated form, e.g. '1234-567X'.

    `normalize` accepts both hyphenated and hyphenless inputs, validates the ISO 7064
    MOD 11-2 check digit, recovers it when missing (7-digit input), and returns the
    canonical form or None if the value is unrecoverable / fails its checksum.
    """
    dtype = pd.StringDtype()
    pattern = r"\d{4}-\d{3}[\dX]"

    @staticmethod
    def _checksum(seven_digits: str) -> str:
        """Compute the ISSN check digit (ISO 7064 MOD 11-2, weights 8,7,6,5,4,3,2)."""
        total = sum(int(d) * w for d, w in zip(seven_digits, (8, 7, 6, 5, 4, 3, 2)))
        rem = total % 11
        check = (11 - rem) % 11
        return "X" if check == 10 else str(check)

    @classmethod
    def normalize(cls, value: str | None) -> str | None:
        """Normalize an ISSN-like value to canonical 'XXXX-XXXC' form."""
        if value is None or not isinstance(value, str):
            return None
        bare = value.strip().replace("-", "")
        if not bare:
            return None
        if len(bare) == 7 and bare.isdigit():
            check = cls._checksum(bare)
            full = bare + check
        elif len(bare) == 8 and bare[:7].isdigit() and bare[7] in "0123456789X":
            if cls._checksum(bare[:7]) != bare[7]:
                return None
            full = bare
        else:
            return None
        return f"{full[0:4]}-{full[4:8]}"


class CountryCode(Identifier):
    """ISO 3166-1 alpha-2 country code, e.g. 'US'."""
    dtype = pd.StringDtype()
    pattern = r"[A-Z]{2}"


class CountryCodeAlpha3(Identifier):
    """ISO 3166-1 alpha-3 country code, e.g. 'USA'."""
    dtype = pd.StringDtype()
    pattern = r"[A-Z]{3}"


# --- Open Targets identifier types -----------------------------------------

class EnsemblGeneId(Identifier):
    """Ensembl gene ID, e.g. 'ENSG00000157764' (human) or 'ENSMUSG00000002111' (mouse).

    Open Targets keys targets on the unversioned Ensembl gene ID. The species infix
    varies ('' for human, 'MUSG' for mouse, etc.), so the pattern is permissive across
    species. Cross-species homologue gene IDs are NOT all Ensembl (worm/fly/etc. use
    WBGene/FBgn), so those columns stay plain strings rather than using this type.

    The pattern is enforced in pandas too (not only in polars), so the pandas-backed
    ``proteinatlas`` pipeline validates its gene key; the polars Open Targets
    pipelines are unaffected (they call ``validate_polars``).
    """
    dtype = pd.StringDtype()
    pattern = r"ENS[A-Z]*G\d+"

    @classmethod
    def validate(cls, s: pd.Series) -> None:
        non_null = s.dropna()
        bad = non_null[~non_null.str.match(rf"^(?:{cls.pattern})$")]
        if not bad.empty:
            raise ValueError(
                f"EnsemblGeneId: {len(bad)} values are not an Ensembl gene ID "
                f"(r'{cls.pattern}'), e.g. {bad.head(5).tolist()}"
            )


class ChemblId(Identifier):
    """ChEMBL molecule ID, e.g. 'CHEMBL25'. Open Targets' canonical drug identifier."""
    dtype = pd.StringDtype()
    pattern = r"CHEMBL\d+"


# --- NCBI sequence accession types -----------------------------------------

class RefSeqAccession(Identifier):
    """RefSeq molecule accession, e.g. 'NM_000546' or 'NP_000537.3'.

    Two-letter molecule-type prefix + '_' + digits, with an optional
    '.<version>' suffix (some sources, e.g. Ensembl's TSV dumps, drop the
    version). Covers the curated/predicted transcript and protein accessions
    NM/NR/XM/XR/NP/XP/YP. NOTE: WGS *genomic* RefSeq accessions interleave letters
    after the prefix (e.g. 'NZ_MCBT01000001.1') and do NOT match this pattern, so
    columns that can hold those stay plain strings rather than using this type.
    """
    dtype = pd.StringDtype()
    pattern = r"[A-Z]{2}_\d+(\.\d+)?"


# --- OpenAlex identifier types ---------------------------------------------

class OpenAlexId(Identifier):
    """Base for OpenAlex internal IDs. Subclasses set `pattern`."""
    dtype = pd.StringDtype()


class OpenAlexWorkId(OpenAlexId):
    pattern = r"W\d+"


class OpenAlexAuthorId(OpenAlexId):
    pattern = r"A\d+"


class OpenAlexInstitutionId(OpenAlexId):
    pattern = r"I\d+"


class OpenAlexSourceId(OpenAlexId):
    pattern = r"S\d+"


class OpenAlexFunderId(OpenAlexId):
    pattern = r"F\d+"


class OpenAlexPublisherId(OpenAlexId):
    pattern = r"P\d+"


class OpenAlexConceptId(OpenAlexId):
    pattern = r"C\d+"


class OpenAlexTopicId(OpenAlexId):
    pattern = r"T\d+"


class OpenAlexAwardId(OpenAlexId):
    pattern = r"G\d+"


class OpenAlexDomainId(OpenAlexId):
    pattern = r"domains/\d+"


class OpenAlexFieldId(OpenAlexId):
    pattern = r"fields/\d+"


class OpenAlexSubfieldId(OpenAlexId):
    pattern = r"subfields/\d+"


class OpenAlexSdgId(OpenAlexId):
    pattern = r"sdgs/\d+"


class OpenAlexKeywordId(OpenAlexId):
    # OpenAlex keyword slugs include unicode letters, dots, and unicode hyphens, so
    # accept any non-whitespace tail.
    pattern = r"keywords/\S+"


class OpenAlexLanguageId(OpenAlexId):
    pattern = r"languages/[a-z]{2,3}"


class OpenAlexLicenseId(OpenAlexId):
    pattern = r"licenses/[a-z0-9\-]+"


class OpenAlexSourceTypeId(OpenAlexId):
    pattern = r"source-types/[A-Za-z0-9 \-]+"


class OpenAlexWorkTypeId(OpenAlexId):
    pattern = r"types/[a-z0-9\-]+"


class OpenAlexInstitutionTypeId(OpenAlexId):
    pattern = r"institution-types/[a-z0-9\-]+"


class OpenAlexContinentId(OpenAlexId):
    pattern = r"continents/Q\d+"


class OpenAlexCountryId(OpenAlexId):
    pattern = r"countries/[A-Z]{2}"


# --- Identifier types for the additional source datasets -------------------
# These key the omim / geneontology / reactome / gwas_catalog / interpro / ror /
# nih_exporter / nsf_awards / biogrid_interactions pipelines. The string-typed
# ones below subclass `_PandasPatternId`, which (like `_MeshUI`) enforces the
# pattern in pandas as well as polars, because each is the grain key of a
# pandas-backed table; the polars-backed tables that reuse them are unaffected
# (they call `validate_polars`).

class _PandasPatternId(Identifier):
    """Base for string identifiers whose `pattern` is enforced in pandas too.

    Subclasses set `pattern`. Mirrors `_MeshUI` but is dataset-agnostic.
    """
    dtype = pd.StringDtype()

    @classmethod
    def validate(cls, s: pd.Series) -> None:
        if cls.pattern is None:
            return
        non_null = s.dropna()
        bad = non_null[~non_null.str.match(rf"^(?:{cls.pattern})$")]
        if not bad.empty:
            raise ValueError(
                f"{cls.__name__}: {len(bad)} values not matching r'{cls.pattern}', "
                f"e.g. {bad.head(5).tolist()}"
            )


class MimNumber(_PandasPatternId):
    """OMIM MIM number, a 6-digit catalog id, e.g. '100050'.

    Stored as a string (a fixed-width catalog key, not a numeric quantity). Keys
    the `omim` mapping table.
    """
    pattern = r"\d{6}"


class ReactomePathwayId(_PandasPatternId):
    """Reactome stable pathway identifier, e.g. 'R-HSA-109582'.

    Form is `R-<species>-<number>` where the 3-letter species code spans Reactome's
    model organisms (HSA, MMU, DME, CEL, …). Keys the `reactome` pathway tables.
    """
    pattern = r"R-[A-Z]{3}-\d+"


class InterProId(_PandasPatternId):
    """InterPro entry accession, e.g. 'IPR000126'.

    Keys the `interpro` entry tables and the protein-to-entry mapping (the latter
    is polars-backed and validated via `validate_polars`).
    """
    pattern = r"IPR\d{6}"


class GwasStudyAccession(_PandasPatternId):
    """GWAS Catalog study accession, e.g. 'GCST000001'.

    Keys the `gwas_catalog` study / ancestry / association tables.
    """
    pattern = r"GCST\d+"


class EfoId(_PandasPatternId):
    """Experimental Factor Ontology term id in CURIE form, e.g. 'EFO:0000270'.

    The GWAS Catalog supplies EFO terms as URIs; only confirmed `EFO_` terms are
    normalized to this CURIE and typed. The association table's `MAPPED_TRAIT_URI`
    mixes EFO with Orphanet / HP / MONDO and so stays a plain string.
    """
    pattern = r"EFO:\d+"


class NsfAwardId(_PandasPatternId):
    """NSF award identifier, e.g. '2142912'.

    A numeric string (historical award ids vary in width), kept as a string because
    it is an opaque agency key, not a quantity. Keys the `nsf_awards` tables.
    """
    pattern = r"\d+"


class NihCoreProjectNum(_PandasPatternId):
    """NIH RePORTER core project number, e.g. 'R01GM123456'.

    The grant's stable activity+IC+serial identifier (the full project number adds
    a support-year/suffix). It is an opaque key whose exact shape varies across
    decades — most are plain alphanumeric, but historical/special records carry
    spaces ('CIT S&SF'), asterisk/slash subproject markers ('N01DA57746*6'), and
    underscores ('NOV190003877625_YCA3') — with no enforceable canonical character
    set, so no pattern is imposed (presence-validated only where required). Links
    `nih_exporter` projects, publications, patents, and clinical studies.
    """
    pattern = None


class NihApplicationId(NumericIdentifier):
    """NIH RePORTER APPLICATION_ID — the unique key of one project-year row
    (positive `Int64`). Dataset-internal; not cross-referenced by other datasets.
    """


class BiogridId(NumericIdentifier):
    """BioGRID internal identifier (positive `Int64`).

    Used for both the BioGRID interaction id and the BioGRID interactor ids in
    `biogrid_interactions`. Dataset-internal; not cross-referenced elsewhere.
    """
