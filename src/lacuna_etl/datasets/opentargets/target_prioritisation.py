"""Open Targets target prioritisation factors (one row per gene).

Each column is a per-gene prioritisation metric. The 0/1 flags are kept as integer
indicators (1 = property holds, 0 = does not, -1 = counts against) as shipped by
the platform; the remaining columns are continuous scores.

  target_prioritisation - one row per Ensembl gene with its prioritisation metrics
"""

import polars as pl

from lacuna_etl.core.identifiers import EnsemblGeneId
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.opentargets._base import OpenTargetsProductPipeline
from lacuna_etl.datasets.registry import register


def transform_file(df: pl.DataFrame) -> dict[str, pl.DataFrame]:
    prioritisation = df.select(
        pl.col("targetId").alias("target_id"),
        pl.col("isInMembrane").cast(pl.Int64).alias("is_in_membrane"),
        pl.col("isSecreted").cast(pl.Int64).alias("is_secreted"),
        pl.col("hasSafetyEvent").cast(pl.Int64).alias("has_safety_event"),
        pl.col("hasPocket").cast(pl.Int64).alias("has_pocket"),
        pl.col("hasLigand").cast(pl.Int64).alias("has_ligand"),
        pl.col("hasSmallMoleculeBinder").cast(pl.Int64).alias("has_small_molecule_binder"),
        pl.col("geneticConstraint").alias("genetic_constraint"),
        pl.col("paralogMaxIdentityPercentage").alias("paralog_max_identity_percentage"),
        pl.col("mouseOrthologMaxIdentityPercentage").alias("mouse_ortholog_max_identity_percentage"),
        pl.col("isCancerDriverGene").cast(pl.Int64).alias("is_cancer_driver_gene"),
        pl.col("hasTEP").cast(pl.Int64).alias("has_tep"),
        pl.col("mouseKOScore").alias("mouse_ko_score"),
        pl.col("hasHighQualityChemicalProbes").cast(pl.Int64).alias("has_high_quality_chemical_probes"),
        pl.col("maxClinicalStage").alias("max_clinical_stage"),
        pl.col("tissueSpecificity").alias("tissue_specificity"),
        pl.col("tissueDistribution").alias("tissue_distribution"),
    )
    return {"target_prioritisation": prioritisation}


TABLES_DOC = {
    "target_prioritisation": {
        "target_id":                            ColumnSpec(identifier=EnsemblGeneId, required=True, description="Ensembl gene ID"),
        "is_in_membrane":                       ColumnSpec(description="Indicator: located in the membrane"),
        "is_secreted":                          ColumnSpec(description="Indicator: secreted protein"),
        "has_safety_event":                     ColumnSpec(description="Indicator: known safety event"),
        "has_pocket":                           ColumnSpec(description="Indicator: has a druggable pocket"),
        "has_ligand":                           ColumnSpec(description="Indicator: has a known ligand"),
        "has_small_molecule_binder":            ColumnSpec(description="Indicator: has a small-molecule binder"),
        "genetic_constraint":                   ColumnSpec(description="Genetic constraint score"),
        "paralog_max_identity_percentage":      ColumnSpec(description="Max sequence identity to a human paralog (%)"),
        "mouse_ortholog_max_identity_percentage": ColumnSpec(description="Max sequence identity to the mouse ortholog (%)"),
        "is_cancer_driver_gene":                ColumnSpec(description="Indicator: cancer driver gene"),
        "has_tep":                              ColumnSpec(description="Indicator: has a Target Enabling Package"),
        "mouse_ko_score":                       ColumnSpec(description="Mouse knockout phenotype score"),
        "has_high_quality_chemical_probes":     ColumnSpec(description="Indicator: has high-quality chemical probes"),
        "max_clinical_stage":                   ColumnSpec(description="Maximum clinical stage of drugs against the target"),
        "tissue_specificity":                   ColumnSpec(description="Tissue-specificity score"),
        "tissue_distribution":                  ColumnSpec(description="Tissue-distribution score"),
    },
}


@register
class OpenTargetsTargetPrioritisation(OpenTargetsProductPipeline):
    name = "opentargets_target_prioritisation"
    raw_dirname = "target_prioritisation"
    transform_module = "lacuna_etl.datasets.opentargets.target_prioritisation"
    first_table = "target_prioritisation"
    tables_doc = TABLES_DOC
