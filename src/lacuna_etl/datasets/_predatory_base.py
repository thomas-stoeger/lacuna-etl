"""Shared base for the predatory-journal and predatory-publisher name lists.

Both datasets are Beall's-list-style exports: a headerless two-column CSV whose
first column is a running row number and whose second column is the entity name
(``1,Abstract and Applied Analysis``). The cleaned output is a single column of
distinct names, one row per listed journal/publisher.
"""
from __future__ import annotations

import pandas as pd

from lacuna_etl.core.pipeline import DatasetPipeline
from lacuna_etl.core.schema import ColumnSpec


class PredatoryListPipeline(DatasetPipeline):
    """One row per listed entity (journal or publisher), keyed by name."""

    csv_name: str
    table_name: str
    entity: str  # "journal" or "publisher", woven into the column description

    def _schema(self) -> dict[str, ColumnSpec]:
        return {
            "title": ColumnSpec(
                description=f"Name of the predatory {self.entity} as listed by the source",
                required=True,
            ),
        }

    def expected_schemas(self) -> dict[str, dict[str, ColumnSpec]]:
        return {self.table_name: self._schema()}

    def extract(self) -> None:
        # Headerless: column 0 is a row counter, column 1 is the name. Names that
        # contain commas are quoted in the source, so the parser yields exactly
        # two columns; assert that before assigning names, so a changed layout
        # (e.g. added columns) fails loudly rather than silently mislabeling.
        df = pd.read_csv(self.raw_path() / self.csv_name, header=None, dtype=str)
        if df.shape[1] != 2:
            raise ValueError(f"{self.name}: expected 2 source columns, got {df.shape[1]}")
        df.columns = ["row_number", "title"]
        self.save_parquet(df[["title"]], self.intermediate_path() / "raw.parquet")

    def transform(self) -> None:
        df = self.load_parquet(self.intermediate_path() / "raw.parquet")
        title = df["title"].astype("string").str.strip()
        out = pd.DataFrame({"title": title})
        out = out[out["title"].notna() & (out["title"] != "")]
        out = out.drop_duplicates(subset="title").sort_values("title").reset_index(drop=True)
        self.save_parquet(out, self.intermediate_path() / "transformed.parquet")

    def load(self) -> None:
        df = self.load_parquet(self.intermediate_path() / "transformed.parquet")
        schema = self._schema()
        df = df[list(schema)]
        self.save_parquet(df, self.output_path() / f"{self.table_name}.parquet")
        self.save_schema_yaml(schema, self.table_name)
