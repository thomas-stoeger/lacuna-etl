from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from lacuna_etl.core.identifiers import Identifier


@dataclass
class ColumnSpec:
    identifier: type[Identifier] | None = None
    description: str = ""
    allowed_values: set[str] | None = None

    def cast(self, s: pd.Series) -> pd.Series:
        if self.identifier is not None:
            return self.identifier.cast(s)
        return s

    def validate(self, s: pd.Series) -> None:
        if self.identifier is not None:
            self.identifier.validate(s)
        if self.allowed_values is not None:
            unexpected = set(s.dropna().unique()) - self.allowed_values
            if unexpected:
                raise ValueError(f"{s.name}: unexpected values {unexpected}, allowed: {self.allowed_values}")

    def yaml_entry(self) -> dict[str, str]:
        entry: dict[str, str] = {}
        if self.identifier is not None:
            entry["identifier_type"] = self.identifier.__name__
        if self.description:
            entry["description"] = self.description
        if self.allowed_values is not None:
            entry["allowed_values"] = ", ".join(sorted(self.allowed_values))
        return entry
