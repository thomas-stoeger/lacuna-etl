"""Open Targets molecular-interaction evidence (one row per supporting experiment).

These records have no stable key, so the nested species / resource / tissue structs
are unnested into prefixed columns and the participant-detection-method struct lists
are flattened into parallel list columns rather than exploded into orphan child tables.

  interaction_evidences - one row per interaction evidence (experiment)
"""

import polars as pl

from lacuna_etl.core.identifiers import NcbiTaxId
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.opentargets._base import OpenTargetsProductPipeline
from lacuna_etl.datasets.registry import register


def _species(col: str, prefix: str) -> list[pl.Expr]:
    f = lambda name: pl.col(col).struct.field(name)  # noqa: E731
    return [
        f("taxonId").cast(pl.Int64).alias(f"{prefix}_taxon_id"),
        f("scientificName").alias(f"{prefix}_scientific_name"),
        f("mnemonic").alias(f"{prefix}_mnemonic"),
    ]


def _method_lists(col: str, prefix: str) -> list[pl.Expr]:
    """Flatten a List(Struct{miIdentifier, shortName}) into two parallel list columns."""
    return [
        pl.col(col).list.eval(pl.element().struct.field("miIdentifier")).alias(f"{prefix}_mi_identifiers"),
        pl.col(col).list.eval(pl.element().struct.field("shortName")).alias(f"{prefix}_short_names"),
    ]


def transform_file(df: pl.DataFrame) -> dict[str, pl.DataFrame]:
    evidences = df.select(
        pl.col("interactionIdentifier").alias("interaction_identifier"),
        pl.col("interactionResources").struct.field("sourceDatabase").alias("source_database"),
        pl.col("interactionResources").struct.field("databaseVersion").alias("source_database_version"),
        pl.col("interactionTypeShortName").alias("interaction_type_short_name"),
        pl.col("interactionTypeMiIdentifier").alias("interaction_type_mi_identifier"),
        pl.col("interactionDetectionMethodShortName").alias("interaction_detection_method_short_name"),
        pl.col("interactionDetectionMethodMiIdentifier").alias("interaction_detection_method_mi_identifier"),
        pl.col("expansionMethodShortName").alias("expansion_method_short_name"),
        pl.col("expansionMethodMiIdentifier").alias("expansion_method_mi_identifier"),
        pl.col("interactionScore").alias("interaction_score"),
        pl.col("evidenceScore").alias("evidence_score"),
        pl.col("pubmedId").alias("pubmed_id"),
        pl.col("targetA").alias("target_a"),
        pl.col("intA").alias("interactor_a"),
        pl.col("intASource").alias("interactor_a_source"),
        pl.col("intABiologicalRole").alias("interactor_a_biological_role"),
        pl.col("targetB").alias("target_b"),
        pl.col("intB").alias("interactor_b"),
        pl.col("intBSource").alias("interactor_b_source"),
        pl.col("intBBiologicalRole").alias("interactor_b_biological_role"),
        *_species("speciesA", "species_a"),
        *_species("speciesB", "species_b"),
        pl.col("hostOrganismTaxId").cast(pl.Int64).alias("host_organism_tax_id"),
        pl.col("hostOrganismScientificName").alias("host_organism_scientific_name"),
        pl.col("hostOrganismTissue").struct.field("fullName").alias("host_organism_tissue_full_name"),
        pl.col("hostOrganismTissue").struct.field("shortName").alias("host_organism_tissue_short_name"),
        pl.col("hostOrganismTissue").struct.field("xrefs").alias("host_organism_tissue_xrefs"),
        *_method_lists("participantDetectionMethodA", "participant_detection_method_a"),
        *_method_lists("participantDetectionMethodB", "participant_detection_method_b"),
    )
    return {"interaction_evidences": evidences}


TABLES_DOC = {
    "interaction_evidences": {
        "interaction_identifier":                    ColumnSpec(description="Source interaction identifier"),
        "source_database":                           ColumnSpec(description="Interaction source database"),
        "source_database_version":                   ColumnSpec(description="Source database version"),
        "interaction_type_short_name":               ColumnSpec(description="Interaction type (PSI-MI short name)"),
        "interaction_type_mi_identifier":            ColumnSpec(description="Interaction type PSI-MI identifier"),
        "interaction_detection_method_short_name":   ColumnSpec(description="Interaction detection method (PSI-MI short name)"),
        "interaction_detection_method_mi_identifier": ColumnSpec(description="Interaction detection method PSI-MI identifier"),
        "expansion_method_short_name":               ColumnSpec(description="Complex-expansion method (PSI-MI short name)"),
        "expansion_method_mi_identifier":            ColumnSpec(description="Complex-expansion method PSI-MI identifier"),
        "interaction_score":                         ColumnSpec(description="Interaction-level score"),
        "evidence_score":                            ColumnSpec(description="Evidence-level score"),
        "pubmed_id":                                 ColumnSpec(description="PubMed ID of the publication (may be an IMEx 'unassigned' ID)"),
        "target_a":                                  ColumnSpec(description="Open Targets target ID of interactor A (Ensembl gene ID when mapped)"),
        "interactor_a":                              ColumnSpec(description="Source molecule identifier of interactor A"),
        "interactor_a_source":                       ColumnSpec(description="Identifier source of interactor A"),
        "interactor_a_biological_role":              ColumnSpec(description="Biological role of interactor A"),
        "target_b":                                  ColumnSpec(description="Open Targets target ID of interactor B (Ensembl gene ID when mapped)"),
        "interactor_b":                              ColumnSpec(description="Source molecule identifier of interactor B"),
        "interactor_b_source":                       ColumnSpec(description="Identifier source of interactor B"),
        "interactor_b_biological_role":              ColumnSpec(description="Biological role of interactor B"),
        "species_a_taxon_id":                        ColumnSpec(identifier=NcbiTaxId, description="NCBI taxon ID of interactor A's species"),
        "species_a_scientific_name":                 ColumnSpec(description="Scientific name of interactor A's species"),
        "species_a_mnemonic":                        ColumnSpec(description="UniProt mnemonic of interactor A's species"),
        "species_b_taxon_id":                        ColumnSpec(identifier=NcbiTaxId, description="NCBI taxon ID of interactor B's species"),
        "species_b_scientific_name":                 ColumnSpec(description="Scientific name of interactor B's species"),
        "species_b_mnemonic":                        ColumnSpec(description="UniProt mnemonic of interactor B's species"),
        "host_organism_tax_id":                      ColumnSpec(identifier=NcbiTaxId, description="NCBI taxon ID of the experimental host organism"),
        "host_organism_scientific_name":             ColumnSpec(description="Scientific name of the host organism"),
        "host_organism_tissue_full_name":            ColumnSpec(description="Host organism tissue full name"),
        "host_organism_tissue_short_name":           ColumnSpec(description="Host organism tissue short name"),
        "host_organism_tissue_xrefs":                ColumnSpec(description="Host organism tissue cross-references (list)"),
        "participant_detection_method_a_mi_identifiers": ColumnSpec(description="Participant-detection-method PSI-MI identifiers for interactor A (list)"),
        "participant_detection_method_a_short_names":    ColumnSpec(description="Participant-detection-method short names for interactor A (list)"),
        "participant_detection_method_b_mi_identifiers": ColumnSpec(description="Participant-detection-method PSI-MI identifiers for interactor B (list)"),
        "participant_detection_method_b_short_names":    ColumnSpec(description="Participant-detection-method short names for interactor B (list)"),
    },
}


@register
class OpenTargetsInteractionEvidence(OpenTargetsProductPipeline):
    name = "opentargets_interaction_evidence"
    raw_dirname = "interaction_evidence"
    transform_module = "lacuna_etl.datasets.opentargets.interaction_evidence"
    first_table = "interaction_evidences"
    tables_doc = TABLES_DOC
