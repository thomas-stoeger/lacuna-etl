import zipfile

import pandas as pd

from lacuna_etl.core.identifiers import Doi, PubmedId
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
    "doi": ColumnSpec(identifier=Doi, description="Digital Object Identifier, if available"),
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


@register
class ICite(DatasetPipeline):
    name = "icite"

    def extract(self) -> None:
        src = self.raw_path() / "icite_metadata.zip"
        with zipfile.ZipFile(src) as z:
            with z.open("icite_metadata.csv") as f:
                df = pd.read_csv(f, dtype=str, keep_default_na=True)

        df.drop(columns=_DROP_COLS, inplace=True)

        df["pmid"] = PubmedId.cast(df["pmid"])
        PubmedId.validate(df["pmid"])

        df["doi"] = Doi.cast(df["doi"])
        Doi.validate(df["doi"])

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

        self.save_parquet(df, self.intermediate_path() / "icite.parquet")

    def transform(self) -> None:
        df = self.load_parquet(self.intermediate_path() / "icite.parquet")

        unexpected_cols = set(df.columns) - set(SCHEMA)
        if unexpected_cols:
            raise ValueError(f"Unexpected columns in iCite data: {unexpected_cols}")

        dups = df["pmid"].duplicated()
        if dups.any():
            raise ValueError(f"Duplicate PMIDs: {df.loc[dups, 'pmid'].tolist()[:10]}")

        df = df[list(SCHEMA.keys())]
        self.save_parquet(df, self.intermediate_path() / "icite_transformed.parquet")

    def load(self) -> None:
        df = self.load_parquet(self.intermediate_path() / "icite_transformed.parquet")
        self.save_parquet(df, self.output_path() / "icite.parquet")
        self.save_schema_yaml(SCHEMA, "icite")
