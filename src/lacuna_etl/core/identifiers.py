import re

import pandas as pd

_DOI_URL_PREFIX = re.compile(r"^https?://(?:doi\.org/)?")


class Identifier:
    dtype: object = None

    @classmethod
    def cast(cls, s: pd.Series) -> pd.Series:
        return s.astype(cls.dtype)

    @classmethod
    def validate(cls, s: pd.Series) -> None:  # noqa: ARG003
        pass


class NumericIdentifier(Identifier):
    dtype = pd.Int64Dtype()

    @classmethod
    def validate(cls, s: pd.Series) -> None:
        if not pd.api.types.is_integer_dtype(s):
            raise TypeError(f"{cls.__name__}: expected integer dtype, got {s.dtype}")
        if s.isna().any():
            raise ValueError(f"{cls.__name__}: unexpected null values")
        if (s <= 0).any():
            raise ValueError(f"{cls.__name__}: IDs must be positive")


class NcbiTaxId(NumericIdentifier):
    pass


class NcbiGeneId(NumericIdentifier):
    pass


class PubmedId(NumericIdentifier):
    pass


class Doi(Identifier):
    dtype = pd.StringDtype()

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
