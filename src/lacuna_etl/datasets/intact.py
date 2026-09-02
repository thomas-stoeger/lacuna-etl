"""IntAct — the EBI molecular-interaction database, in PSI-MITAB 2.8 format.

IntAct ships its full interaction set as one large tab-delimited MITAB file
(``intact.txt``, ~1.79M rows × 42 columns, ~11 GB) plus a small negative-results
file (``intact_negative.txt``), both inside a single zip. The rows are very wide
(pipe-delimited multi-value fields), so this is a polars streaming pipeline rather
than an in-memory one: ``extract`` decompresses both members to the intermediate
directory (restartable via done-markers), ``transform`` scans them lazily, derives a
few typed columns, and streams the result to a single ``interactions`` parquet via
``sink_parquet`` (bounded memory), and ``load`` writes the schema sidecar.

The output is a faithful, snake_cased projection of the 42 MITAB columns, kept as
documented plain strings (the multi-value fields stay verbatim, the
alliancegenome-MITAB precedent). A handful of convenience columns are derived and
typed: ``pubmed_id`` (``PubmedId``, the first ``pubmed:`` token of the publication
identifiers) and ``taxid_a``/``taxid_b`` (the numeric interactor taxids — kept plain
``Int64`` rather than ``NcbiTaxId`` because MITAB uses negative special codes
``-1``/``-2`` for in-vitro/chemical synthesis). ``negative`` is the MITAB negative
flag; ``is_negative_dataset`` distinguishes rows from ``intact_negative.txt``. MITAB's
``-`` empty marker is normalised to null. See docs/DESIGN.md for the table inventory.
"""
from __future__ import annotations

import zipfile
from pathlib import Path

import polars as pl
import yaml

from lacuna_etl.core.identifiers import PubmedId
from lacuna_etl.core.pipeline import DatasetPipeline
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.registry import register

# The 42 MITAB columns in order, snake_cased (the header is dropped and replaced).
_MITAB_COLUMNS = [
    "interactor_a_id", "interactor_b_id", "alt_ids_a", "alt_ids_b", "aliases_a", "aliases_b",
    "detection_methods", "publication_first_authors", "publication_identifiers",
    "taxid_a_raw", "taxid_b_raw", "interaction_types", "source_databases",
    "interaction_identifiers", "confidence_values", "expansion_methods",
    "biological_role_a", "biological_role_b", "experimental_role_a", "experimental_role_b",
    "type_a", "type_b", "xrefs_a", "xrefs_b", "interaction_xrefs",
    "annotations_a", "annotations_b", "interaction_annotations", "host_organisms",
    "interaction_parameters", "creation_date", "update_date", "checksum_a", "checksum_b",
    "interaction_checksum", "negative_raw", "features_a", "features_b",
    "stoichiometry_a", "stoichiometry_b", "identification_method_a", "identification_method_b",
]

# Output schema (also used for validation + sidecar). Final column order.
TABLES_DOC = {
    "interactions": {
        "interactor_a_id": ColumnSpec(required=True, description="Primary identifier of interactor A (e.g. uniprotkb:O43426, intact:EBI-…)"),
        "interactor_b_id": ColumnSpec(description="Primary identifier of interactor B (null for intramolecular / single-participant interactions, where the source gives '-')"),
        "alt_ids_a": ColumnSpec(description="Alternative identifier(s) of interactor A (pipe-delimited)"),
        "alt_ids_b": ColumnSpec(description="Alternative identifier(s) of interactor B (pipe-delimited)"),
        "aliases_a": ColumnSpec(description="Alias(es) of interactor A (pipe-delimited)"),
        "aliases_b": ColumnSpec(description="Alias(es) of interactor B (pipe-delimited)"),
        "detection_methods": ColumnSpec(description="Interaction detection method(s) as PSI-MI terms (pipe-delimited)"),
        "publication_first_authors": ColumnSpec(description="Publication first author(s)"),
        "publication_identifiers": ColumnSpec(description="Publication identifier(s) (pubmed:/doi:/imex:…, pipe-delimited)"),
        "pubmed_id": ColumnSpec(identifier=PubmedId, description="PubMed id parsed from the first 'pubmed:' token of publication_identifiers (nullable)"),
        "taxid_a_raw": ColumnSpec(description="Taxid of interactor A as given (e.g. 'taxid:9606(human)')"),
        "taxid_b_raw": ColumnSpec(description="Taxid of interactor B as given"),
        "taxid_a": ColumnSpec(description="Numeric NCBI taxid of interactor A (Int64; MITAB negatives -1/-2 = in-vitro/chemical synthesis, so not typed NcbiTaxId)"),
        "taxid_b": ColumnSpec(description="Numeric NCBI taxid of interactor B (Int64)"),
        "interaction_types": ColumnSpec(description="Interaction type(s) as PSI-MI terms (pipe-delimited)"),
        "source_databases": ColumnSpec(description="Source database(s) (pipe-delimited)"),
        "interaction_identifiers": ColumnSpec(description="Interaction identifier(s) (pipe-delimited)"),
        "confidence_values": ColumnSpec(description="Confidence value(s) (pipe-delimited)"),
        "expansion_methods": ColumnSpec(description="Complex expansion method(s)"),
        "biological_role_a": ColumnSpec(description="Biological role of interactor A (PSI-MI term)"),
        "biological_role_b": ColumnSpec(description="Biological role of interactor B"),
        "experimental_role_a": ColumnSpec(description="Experimental role of interactor A (PSI-MI term)"),
        "experimental_role_b": ColumnSpec(description="Experimental role of interactor B"),
        "type_a": ColumnSpec(description="Molecular type of interactor A (PSI-MI term)"),
        "type_b": ColumnSpec(description="Molecular type of interactor B"),
        "xrefs_a": ColumnSpec(description="Cross-reference(s) for interactor A (pipe-delimited)"),
        "xrefs_b": ColumnSpec(description="Cross-reference(s) for interactor B (pipe-delimited)"),
        "interaction_xrefs": ColumnSpec(description="Interaction cross-reference(s) (pipe-delimited)"),
        "annotations_a": ColumnSpec(description="Annotation(s) for interactor A"),
        "annotations_b": ColumnSpec(description="Annotation(s) for interactor B"),
        "interaction_annotations": ColumnSpec(description="Interaction annotation(s)"),
        "host_organisms": ColumnSpec(description="Host organism(s) of the experiment (may carry MITAB negative taxids)"),
        "interaction_parameters": ColumnSpec(description="Interaction parameter(s) (e.g. kd)"),
        "creation_date": ColumnSpec(description="Record creation date"),
        "update_date": ColumnSpec(description="Record last-update date"),
        "checksum_a": ColumnSpec(description="Checksum of interactor A (e.g. ROGID)"),
        "checksum_b": ColumnSpec(description="Checksum of interactor B"),
        "interaction_checksum": ColumnSpec(description="Interaction checksum (RIGID)"),
        "negative": ColumnSpec(description="MITAB 'Negative' flag: True if the interaction is a negative result"),
        "features_a": ColumnSpec(description="Feature(s) of interactor A (pipe-delimited)"),
        "features_b": ColumnSpec(description="Feature(s) of interactor B (pipe-delimited)"),
        "stoichiometry_a": ColumnSpec(description="Stoichiometry of interactor A"),
        "stoichiometry_b": ColumnSpec(description="Stoichiometry of interactor B"),
        "identification_method_a": ColumnSpec(description="Participant identification method for A (PSI-MI term)"),
        "identification_method_b": ColumnSpec(description="Participant identification method for B"),
        "is_negative_dataset": ColumnSpec(required=True, description="True for rows sourced from intact_negative.txt (the curated negative-interactions file)"),
    }
}

_SOURCE_FILES = {"intact.txt": False, "intact_negative.txt": True}


@register
class Intact(DatasetPipeline):
    name = "intact"

    def _zip(self) -> Path:
        zips = sorted(self.raw_path().glob("*.zip"))
        if len(zips) != 1:
            raise FileNotFoundError(f"intact: expected exactly one *.zip under {self.raw_path()}, found {len(zips)}")
        return zips[0]

    def extract(self) -> None:
        done_dir = self.intermediate_path() / "_done"
        done_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(self._zip()) as zf:
            for member in _SOURCE_FILES:
                done = done_dir / f"{member}.done"
                if done.exists() and (self.intermediate_path() / member).exists():
                    continue
                with zf.open(member) as src, open(self.intermediate_path() / member, "wb") as dst:
                    while chunk := src.read(1 << 24):  # 16 MiB chunks
                        dst.write(chunk)
                done.touch()
        print(f"[{self.name}] decompressed {', '.join(_SOURCE_FILES)}")

    def _scan(self, member: str, is_negative: bool) -> pl.LazyFrame:
        lf = pl.scan_csv(
            self.intermediate_path() / member,
            separator="\t", has_header=False, skip_rows=1, new_columns=_MITAB_COLUMNS,
            quote_char=None, infer_schema_length=0, null_values=["-"],
        )
        return lf.with_columns(
            pl.col("publication_identifiers").str.extract(r"pubmed:(\d+)", 1).cast(pl.Int64).alias("pubmed_id"),
            pl.col("taxid_a_raw").str.extract(r"taxid:(-?\d+)", 1).cast(pl.Int64).alias("taxid_a"),
            pl.col("taxid_b_raw").str.extract(r"taxid:(-?\d+)", 1).cast(pl.Int64).alias("taxid_b"),
            (pl.col("negative_raw") == "true").alias("negative"),
            pl.lit(is_negative).alias("is_negative_dataset"),
        )

    def transform(self) -> None:
        out_path = self.output_path() / "interactions.parquet"
        columns = list(TABLES_DOC["interactions"])
        lf = pl.concat(
            [self._scan(m, neg).select(columns) for m, neg in _SOURCE_FILES.items()],
            how="vertical",
        )
        lf.sink_parquet(out_path, compression="snappy")
        n = pl.scan_parquet(out_path).select(pl.len()).collect().item()
        print(f"[{self.name}] interactions: {n:,} rows")
        self._validate_outputs()

    def _validate_outputs(self) -> None:
        out_path = self.output_path() / "interactions.parquet"
        doc = TABLES_DOC["interactions"]
        check_cols = [c for c, spec in doc.items() if spec.identifier is not None or spec.allowed_values is not None or spec.required]
        df = pl.scan_parquet(out_path).select(check_cols).collect()
        for col in check_cols:
            doc[col].validate_polars(df[col])
        print(f"[{self.name}] validated {len(check_cols)} cols ({df.height:,} rows)")

    def load(self) -> None:
        doc = TABLES_DOC["interactions"]
        data = {col: spec.yaml_entry() for col, spec in doc.items()}
        (self.output_path() / "interactions.yml").write_text(yaml.dump(data, sort_keys=False, allow_unicode=True))
