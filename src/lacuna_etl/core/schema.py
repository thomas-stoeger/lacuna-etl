from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pandas as pd

if TYPE_CHECKING:
    from lacuna_etl.core.identifiers import Identifier


@dataclass
class ColumnSpec:
    identifier: type[Identifier] | None = None
    description: str = ""
    allowed_values: set[str] | None = None
    required: bool = False

    def cast(self, s: pd.Series) -> pd.Series:
        if self.identifier is not None:
            return self.identifier.cast(s)
        return s

    def validate(self, s: pd.Series) -> None:
        if self.identifier is not None:
            self.identifier.validate(s)
        if self.required:
            nulls = int(s.isna().sum())
            if nulls:
                raise ValueError(f"{s.name}: {nulls} nulls in required column")
        if self.allowed_values is not None:
            unexpected = set(s.dropna().unique()) - self.allowed_values
            if unexpected:
                raise ValueError(f"{s.name}: unexpected values {unexpected}, allowed: {self.allowed_values}")

    def validate_polars(self, s: Any) -> None:
        if self.identifier is not None:
            self.identifier.validate_polars(s, required=self.required)
        elif self.required:
            nulls = s.null_count()
            if nulls > 0:
                raise ValueError(f"{s.name}: {nulls} nulls (required)")
        if self.allowed_values is not None:
            non_null = s.drop_nulls()
            unexpected = set(non_null.unique().to_list()) - self.allowed_values
            if unexpected:
                raise ValueError(f"{s.name}: unexpected values {unexpected}, allowed: {self.allowed_values}")

    def yaml_entry(self) -> dict[str, str | bool]:
        entry: dict[str, str | bool] = {}
        if self.identifier is not None:
            entry["identifier_type"] = self.identifier.__name__
        if self.description:
            entry["description"] = self.description
        if self.allowed_values is not None:
            entry["allowed_values"] = ", ".join(sorted(self.allowed_values))
        if self.required:
            entry["required"] = True
        return entry
