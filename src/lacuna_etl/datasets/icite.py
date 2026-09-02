import shutil
import zipfile

import pandas as pd
import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.parquet as pq

from lacuna_etl.core.identifiers import Doi, DoiVersioned, PubmedId, split_doi_version_pandas
from lacuna_etl.core.pipeline import DatasetPipeline
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.registry import register

_DROP_COLS = ["authors", "cited_by_clin", "cited_by", "references"]
_INT_COLS = ["year", "citation_count"]
_FLOAT_COLS = [
    "field_citation_rate",
    "expected_citations_per_year",
    "citations_per_year",
    "relative_citation_ratio",
    "nih_percentile",
    "human",
    "animal",
    "molecular_cellular",
    "x_coord",
    "y_coord",
    "apt",
]
_BOOL_COLS = ["is_research_article", "is_clinical", "provisional"]
_BOOL_MAP = {"True": True, "False": False}

SCHEMA = {
    "pmid": ColumnSpec(identifier=PubmedId, description="PubMed Identifier assigned by the National Library of Medicine"),
    "doi": ColumnSpec(identifier=Doi, description="Digital Object Identifier (article-level; any publisher version suffix is split into doi_versioned), if available"),
    "doi_versioned": ColumnSpec(identifier=DoiVersioned, description="Original versioned DOI when the publisher appends an article version (e.g. F1000 '.N', Research Square '/vN'); null otherwise"),
    "title": ColumnSpec(description="Title of the article"),
    "year": ColumnSpec(description="Year the article was published"),
    "journal": ColumnSpec(description="Journal name (NLM abbreviation, without periods)"),
    "is_research_article": ColumnSpec(description="Whether the article's Publication Type tags are consistent with a primary research article"),
    "citation_count": ColumnSpec(description="Number of unique articles that have cited this one"),
    "citations_per_year": ColumnSpec(description="Citations per year since publication; numerator for the RCR"),
    "expected_citations_per_year": ColumnSpec(description="Citations per year received by NIH-funded articles in the same field and year; denominator for the RCR"),
    "relative_citation_ratio": ColumnSpec(description="Relative Citation Ratio (RCR): field- and time-adjusted citation impact benchmarked against NIH-funded papers; median NIH paper = 1.0"),
    "nih_percentile": ColumnSpec(description="Percentile rank of this paper's RCR among all NIH publications; e.g. 95 means higher than 95% of NIH-funded papers"),
    "field_citation_rate": ColumnSpec(description="Intrinsic citation rate of this paper's field, estimated from its co-citation network"),
    "human": ColumnSpec(description="Fraction of MeSH terms in the Human category, out of Human/Animal/Molecular-Cellular MeSH terms"),
    "animal": ColumnSpec(description="Fraction of MeSH terms in the Animal category, out of Human/Animal/Molecular-Cellular MeSH terms"),
    "molecular_cellular": ColumnSpec(description="Fraction of MeSH terms in the Molecular/Cellular Biology category, out of Human/Animal/Molecular-Cellular MeSH terms"),
    "x_coord": ColumnSpec(description="x-coordinate on the Triangle of Biomedicine"),
    "y_coord": ColumnSpec(description="y-coordinate on the Triangle of Biomedicine"),
    "apt": ColumnSpec(description="Approximate Potential to Translate (APT): ML-based estimate of the likelihood of citation in clinical trials or guidelines"),
    "is_clinical": ColumnSpec(description="Whether this paper meets the definition of a clinical article"),
    "provisional": ColumnSpec(description="Whether the RCR is provisional, flagged for papers published in the previous two years where citation metrics are less stable"),
    "last_modified": ColumnSpec(description="Date citation metrics were last updated"),
}

OPEN_CITATION_SCHEMA = {
    "citing": ColumnSpec(identifier=PubmedId, description="PubMed ID of the citing article"),
    "referenced": ColumnSpec(identifier=PubmedId, description="PubMed ID of the referenced article"),
}


@register
class ICite(DatasetPipeline):
    name = "icite"
    _TABLES = [("icite", SCHEMA), ("open_citation_collection", OPEN_CITATION_SCHEMA)]

    def extract(self) -> None:
        self._extract_metadata()
        self._extract_open_citation_collection()

    def transform(self) -> None:
        self._transform_metadata()
        self._transform_open_citation_collection()

    def load(self) -> None:
        self._load_metadata()
        self._load_open_citation_collection()

    def _extract_metadata(self) -> None:
        src = self.raw_path() / "icite_metadata.zip"
        with zipfile.ZipFile(src) as z:
            with z.open("icite_metadata.csv") as f:
                df = pd.read_csv(f, dtype=str, keep_default_na=True)

        df.drop(columns=_DROP_COLS, inplace=True)

        df["pmid"] = PubmedId.cast(df["pmid"])
        PubmedId.validate(df["pmid"])

        df["doi"] = Doi.cast(df["doi"])
        # Split publisher-versioned DOIs (F1000 '.N', Research Square '/vN', ...):
        # doi keeps the article-level base, doi_versioned keeps the full versioned form.
        df["doi"], df["doi_versioned"] = split_doi_version_pandas(df["doi"])
        Doi.validate(df["doi"])
        DoiVersioned.validate(df["doi_versioned"])

        for col in _INT_COLS:
            df[col] = pd.to_numeric(df[col], errors="raise").astype(pd.Int64Dtype())

        for col in _FLOAT_COLS:
            df[col] = pd.to_numeric(df[col], errors="raise")

        for col in _BOOL_COLS:
            unexpected = ~df[col].isin(_BOOL_MAP) & df[col].notna()
            if unexpected.any():
                raise ValueError(f"{col}: unexpected values {df.loc[unexpected, col].unique()}")
            df[col] = df[col].map(_BOOL_MAP).astype(pd.BooleanDtype())

        df["last_modified"] = pd.to_datetime(
            df["last_modified"].str.replace(r"\[.*\]$", "", regex=True),
            format="ISO8601",
            utc=True,
        )

        # Distinct from the output's icite.parquet so the two never collide
        # when intermediate_root == output_root.
        self.save_parquet(df, self.intermediate_path() / "icite_metadata.parquet")

    def _extract_open_citation_collection(self) -> None:
        src = self.raw_path() / "open_citation_collection.zip"
        out = self.intermediate_path() / "open_citation_collection.parquet"
        convert_options = pa_csv.ConvertOptions(
            column_types={"citing": pa.int64(), "referenced": pa.int64()},
            strings_can_be_null=False,
        )
        with zipfile.ZipFile(src) as z, z.open("open_citation_collection.csv") as f:
            reader = pa_csv.open_csv(f, convert_options=convert_options)
            with pq.ParquetWriter(out, reader.schema, compression="snappy") as writer:
                for batch in reader:
                    writer.write_batch(batch)

    def _transform_metadata(self) -> None:
        df = self.load_parquet(self.intermediate_path() / "icite_metadata.parquet")

        unexpected_cols = set(df.columns) - set(SCHEMA)
        if unexpected_cols:
            raise ValueError(f"Unexpected columns in iCite data: {unexpected_cols}")

        dups = df["pmid"].duplicated()
        if dups.any():
            raise ValueError(f"Duplicate PMIDs: {df.loc[dups, 'pmid'].tolist()[:10]}")

        df = df[list(SCHEMA.keys())]
        self.save_parquet(df, self.intermediate_path() / "icite_transformed.parquet")

    def _transform_open_citation_collection(self) -> None:
        src = self.intermediate_path() / "open_citation_collection.parquet"
        meta = pq.ParquetFile(src).metadata
        col_indices = {meta.schema.column(i).name: i for i in range(meta.num_columns)}
        for col in ("citing", "referenced"):
            idx = col_indices[col]
            for rg in range(meta.num_row_groups):
                stats = meta.row_group(rg).column(idx).statistics
                if stats is None or not stats.has_min_max:
                    raise ValueError(f"open_citation_collection.{col}: missing parquet stats in row group {rg}")
                if stats.null_count:
                    raise ValueError(f"open_citation_collection.{col}: {stats.null_count} null PMIDs in row group {rg}")
                if stats.min <= 0:
                    raise ValueError(f"open_citation_collection.{col}: non-positive PMID {stats.min} in row group {rg}")

    def _load_metadata(self) -> None:
        df = self.load_parquet(self.intermediate_path() / "icite_transformed.parquet")
        self.save_parquet(df, self.output_path() / "icite.parquet")
        self.save_schema_yaml(SCHEMA, "icite")

    def _load_open_citation_collection(self) -> None:
        src = self.intermediate_path() / "open_citation_collection.parquet"
        dst = self.output_path() / "open_citation_collection.parquet"
        # No-op when intermediate_root == output_root (src and dst are the same
        # file); shutil.copyfile would otherwise raise SameFileError.
        if src.resolve() != dst.resolve():
            shutil.copyfile(src, dst)
        self.save_schema_yaml(OPEN_CITATION_SCHEMA, "open_citation_collection")
