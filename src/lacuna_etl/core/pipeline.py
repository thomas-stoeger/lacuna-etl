import sys
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

    def load_parquet(self, path: Path) -> pd.DataFrame:
        return pq.read_table(path).to_pandas()

    def save_parquet(self, df: pd.DataFrame, path: Path) -> None:
        pq.write_table(pa.Table.from_pandas(df, preserve_index=False), path, compression="snappy")

    def save_schema_yaml(self, schema: dict[str, ColumnSpec], stem: str) -> None:
        data = {col: spec.yaml_entry() for col, spec in schema.items()}
        path = self.output_path() / f"{stem}.yml"
        path.write_text(yaml.dump(data, sort_keys=False, allow_unicode=True))

    def expected_schemas(self) -> dict[str, dict[str, ColumnSpec]]:
        """Return ``{table_name: {column: ColumnSpec}}`` for every sidecar this dataset emits.

        This is the single introspectable source of truth used by ``etl check`` (sidecar
        drift detection) and the cross-dataset identifier audit, derived without running a
        pipeline. The default auto-detects the repo's schema-declaration conventions:

          * a class ``tables_doc`` mapping (Open Targets product pipelines);
          * a module-level ``TABLES_DOC`` mapping (OpenAlex entities; PubMed/ORCID/PubTator3
            import it into their pipeline module's namespace);
          * a module-level or class ``_TABLES`` (a mapping, or a sequence of
            ``(table, schema)`` pairs).

        Datasets that build tables dynamically, hard-code a stem, or expose nothing
        introspectable override this method.
        """
        td = getattr(type(self), "tables_doc", None)
        if td:
            return {table: dict(cols) for table, cols in td.items()}
        mod = sys.modules.get(type(self).__module__)
        for source in (getattr(mod, "TABLES_DOC", None), getattr(mod, "_TABLES", None),
                       getattr(type(self), "_TABLES", None)):
            if not source:
                continue
            pairs = source.items() if hasattr(source, "items") else source
            return {table: dict(schema) for table, schema in pairs}
        raise NotImplementedError(
            f"{self.name}: cannot introspect output schemas; override expected_schemas() "
            f"or declare a _TABLES / TABLES_DOC mapping"
        )

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
