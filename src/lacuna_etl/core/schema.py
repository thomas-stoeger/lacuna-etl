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

    def cast(self, s: pd.Series) -> pd.Series:
        if self.identifier is not None:
            return self.identifier.cast(s)
        return s

    def validate(self, s: pd.Series) -> None:
        if self.identifier is not None:
            self.identifier.validate(s)

    def parquet_field_metadata(self) -> dict[bytes, bytes]:
        meta: dict[bytes, bytes] = {}
        if self.identifier is not None:
            meta[b"identifier_type"] = self.identifier.__name__.encode()
        if self.description:
            meta[b"description"] = self.description.encode()
        return meta
