from __future__ import annotations

import re
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    import polars as pl

_DOI_URL_PREFIX = re.compile(r"^https?://(?:doi\.org/)?")
_WIKIDATA_URL_PREFIX = re.compile(r"^https?://(?:www\.)?wikidata\.org/(?:wiki|entity)/")


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


class GoId(Identifier):
    """Gene Ontology term ID, e.g. 'GO:0008150'."""
    dtype = pd.StringDtype()
    pattern = r"GO:\d{7}"


class Doi(Identifier):
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
    """Research Organization Registry ID, canonical URL form, e.g. 'https://ror.org/01an7q238'."""
    dtype = pd.StringDtype()
    pattern = r"https://ror\.org/[a-z0-9]+"


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
    """
    dtype = pd.StringDtype()
    pattern = r"ENS[A-Z]*G\d+"


class ChemblId(Identifier):
    """ChEMBL molecule ID, e.g. 'CHEMBL25'. Open Targets' canonical drug identifier."""
    dtype = pd.StringDtype()
    pattern = r"CHEMBL\d+"


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
