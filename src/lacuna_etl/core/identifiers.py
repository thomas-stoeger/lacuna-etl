import pandas as pd


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
