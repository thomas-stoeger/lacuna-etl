from abc import ABC, abstractmethod
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from lacuna_etl.config import get_data_root, get_intermediate_root, get_output_root
from lacuna_etl.core.schema import ColumnSpec


class DatasetPipeline(ABC):
    name: str
    depends_on: list[str] = []

    def raw_path(self) -> Path:
        """Latest versioned directory produced by data_downloader."""
        root = get_data_root() / self.name
        if not root.exists():
            raise FileNotFoundError(f"Raw data not found: {root}")
        versions = sorted(p for p in root.iterdir() if p.is_dir())
        if not versions:
            raise FileNotFoundError(f"No versions found under: {root}")
        return versions[-1]

    def output_path(self) -> Path:
        path = get_output_root() / self.name
        path.mkdir(parents=True, exist_ok=True)
        return path

    def intermediate_path(self) -> Path:
        path = get_intermediate_root() / self.name
        path.mkdir(parents=True, exist_ok=True)
        return path

    def apply_schema(self, df: pd.DataFrame, schema: dict[str, ColumnSpec]) -> pd.DataFrame:
        for col, spec in schema.items():
            df[col] = spec.cast(df[col])
            spec.validate(df[col])
        return df

    def save_parquet(self, df: pd.DataFrame, path: Path) -> None:
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(table, path)

    def save_schema_yaml(self, schema: dict[str, ColumnSpec], stem: str) -> None:
        data = {}
        for col, spec in schema.items():
            entry: dict[str, str] = {}
            if spec.identifier is not None:
                entry["identifier_type"] = spec.identifier.__name__
            if spec.description:
                entry["description"] = spec.description
            data[col] = entry
        path = self.output_path() / f"{stem}.yml"
        path.write_text(yaml.dump(data, sort_keys=False, allow_unicode=True))

    @abstractmethod
    def extract(self) -> None:
        """Read from raw_path(), write intermediates to intermediate_path()."""

    @abstractmethod
    def transform(self) -> None:
        """Read from intermediate_path(), produce transformed outputs."""

    @abstractmethod
    def load(self) -> None:
        """Load transformed outputs into final destination."""

    def run(self) -> None:
        print(f"[{self.name}] extract")
        self.extract()
        print(f"[{self.name}] transform")
        self.transform()
        print(f"[{self.name}] load")
        self.load()
