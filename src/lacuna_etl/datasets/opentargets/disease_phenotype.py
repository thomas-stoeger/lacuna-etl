"""Open Targets disease -> phenotype annotations (HPOA-derived).

  disease_phenotypes          - one row per (disease, phenotype) link
  disease_phenotype_evidence  - one row per supporting evidence record for the link
"""

import polars as pl

from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.opentargets._base import OpenTargetsProductPipeline
from lacuna_etl.datasets.opentargets._helpers import explode_struct_list
from lacuna_etl.datasets.registry import register

_KEYS = {"disease": "disease_id", "phenotype": "phenotype_id"}


def transform_file(df: pl.DataFrame) -> dict[str, pl.DataFrame]:
    disease_phenotypes = df.select(
        pl.col("disease").alias("disease_id"),
        pl.col("phenotype").alias("phenotype_id"),
    )
    evidence = explode_struct_list(
        df, _KEYS, "evidence",
        {
            "aspect": "aspect",
            "bioCuration": "bio_curation",
            "diseaseFromSourceId": "disease_from_source_id",
            "diseaseFromSource": "disease_from_source",
            "diseaseName": "disease_name",
            "evidenceType": "evidence_type",
            "frequency": "frequency",
            "qualifier": "qualifier",
            "qualifierNot": "qualifier_not",
            "sex": "sex",
            "resource": "resource",
            "modifiers": "modifiers",
            "onset": "onset",
            "references": "references",
        },
    )
    return {"disease_phenotypes": disease_phenotypes, "disease_phenotype_evidence": evidence}


TABLES_DOC = {
    "disease_phenotypes": {
        "disease_id":   ColumnSpec(required=True, description="Open Targets / EFO disease ontology ID"),
        "phenotype_id": ColumnSpec(required=True, description="Human Phenotype Ontology (HPO) term ID"),
    },
    "disease_phenotype_evidence": {
        "disease_id":             ColumnSpec(required=True, description="Open Targets / EFO disease ontology ID"),
        "phenotype_id":           ColumnSpec(required=True, description="Human Phenotype Ontology (HPO) term ID"),
        "aspect":                 ColumnSpec(description="HPO aspect (phenotypic abnormality, clinical modifier, ...)"),
        "bio_curation":           ColumnSpec(description="Curation provenance string"),
        "disease_from_source_id": ColumnSpec(description="Disease identifier as reported by the source"),
        "disease_from_source":    ColumnSpec(description="Disease identifier/code as reported by the source"),
        "disease_name":           ColumnSpec(description="Disease name as reported by the source"),
        "evidence_type":          ColumnSpec(description="HPO evidence code (PCS, TAS, IEA, ...)"),
        "frequency":              ColumnSpec(description="Phenotype frequency (HPO term or ratio)"),
        "qualifier":              ColumnSpec(description="Qualifier on the annotation"),
        "qualifier_not":          ColumnSpec(description="Whether the annotation is negated (NOT)"),
        "sex":                    ColumnSpec(description="Sex the annotation applies to"),
        "resource":               ColumnSpec(description="Source resource"),
        "modifiers":              ColumnSpec(description="Clinical modifier term IDs (list)"),
        "onset":                  ColumnSpec(description="Onset term IDs (list)"),
        "references":             ColumnSpec(description="Supporting references (list)"),
    },
}


@register
class OpenTargetsDiseasePhenotype(OpenTargetsProductPipeline):
    name = "opentargets_disease_phenotype"
    raw_dirname = "disease_phenotype"
    transform_module = "lacuna_etl.datasets.opentargets.disease_phenotype"
    first_table = "disease_phenotypes"
    tables_doc = TABLES_DOC
