"""Open Targets molecular interactions (aggregated; IntAct, Reactome, Signor, string).

  interactions - one row per (interactor A, interactor B, source database) pair with
                 its interaction count and aggregated score
"""

import polars as pl

from lacuna_etl.core.identifiers import NcbiTaxId
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.opentargets._base import OpenTargetsProductPipeline
from lacuna_etl.datasets.registry import register


def _species(col: str, prefix: str) -> list[pl.Expr]:
    """Unnest a {taxonId, scientificName, mnemonic} species struct into prefixed columns."""
    f = lambda name: pl.col(col).struct.field(name)  # noqa: E731
    return [
        f("taxonId").cast(pl.Int64).alias(f"{prefix}_taxon_id"),
        f("scientificName").alias(f"{prefix}_scientific_name"),
        f("mnemonic").alias(f"{prefix}_mnemonic"),
    ]


def transform_file(df: pl.DataFrame) -> dict[str, pl.DataFrame]:
    interactions = df.select(
        pl.col("sourceDatabase").alias("source_database"),
        pl.col("targetA").alias("target_a"),
        pl.col("intA").alias("interactor_a"),
        pl.col("intABiologicalRole").alias("interactor_a_biological_role"),
        pl.col("targetB").alias("target_b"),
        pl.col("intB").alias("interactor_b"),
        pl.col("intBBiologicalRole").alias("interactor_b_biological_role"),
        *_species("speciesA", "species_a"),
        *_species("speciesB", "species_b"),
        pl.col("count").cast(pl.Int64).alias("interaction_count"),
        pl.col("scoring").alias("score"),
    )
    return {"interactions": interactions}


TABLES_DOC = {
    "interactions": {
        "source_database":              ColumnSpec(description="Interaction source database"),
        "target_a":                     ColumnSpec(description="Open Targets target ID of interactor A (Ensembl gene ID when mapped)"),
        "interactor_a":                 ColumnSpec(description="Source molecule identifier of interactor A (e.g. UniProt)"),
        "interactor_a_biological_role": ColumnSpec(description="Biological role of interactor A"),
        "target_b":                     ColumnSpec(description="Open Targets target ID of interactor B (Ensembl gene ID when mapped)"),
        "interactor_b":                 ColumnSpec(description="Source molecule identifier of interactor B"),
        "interactor_b_biological_role": ColumnSpec(description="Biological role of interactor B"),
        "species_a_taxon_id":           ColumnSpec(identifier=NcbiTaxId, description="NCBI taxon ID of interactor A's species"),
        "species_a_scientific_name":    ColumnSpec(description="Scientific name of interactor A's species"),
        "species_a_mnemonic":           ColumnSpec(description="UniProt mnemonic of interactor A's species"),
        "species_b_taxon_id":           ColumnSpec(identifier=NcbiTaxId, description="NCBI taxon ID of interactor B's species"),
        "species_b_scientific_name":    ColumnSpec(description="Scientific name of interactor B's species"),
        "species_b_mnemonic":           ColumnSpec(description="UniProt mnemonic of interactor B's species"),
        "interaction_count":            ColumnSpec(description="Number of supporting interaction evidences"),
        "score":                        ColumnSpec(description="Aggregated interaction score"),
    },
}


@register
class OpenTargetsInteraction(OpenTargetsProductPipeline):
    name = "opentargets_interaction"
    raw_dirname = "interaction"
    transform_module = "lacuna_etl.datasets.opentargets.interaction"
    first_table = "interactions"
    tables_doc = TABLES_DOC
