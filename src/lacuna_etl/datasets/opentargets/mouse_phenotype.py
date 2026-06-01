"""Open Targets mouse phenotypes (IMPC / MGI).

  mouse_phenotypes        - one row per (human target, mouse model phenotype) record
  mouse_phenotype_classes - phenotype-class annotations for each record
  mouse_phenotype_models  - biological models exhibiting the phenotype
"""

import polars as pl

from lacuna_etl.core.identifiers import EnsemblGeneId
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.opentargets._base import OpenTargetsProductPipeline
from lacuna_etl.datasets.opentargets._helpers import explode_struct_list
from lacuna_etl.datasets.registry import register

_KEYS = {"targetFromSourceId": "target_from_source_id", "modelPhenotypeId": "model_phenotype_id"}


def transform_file(df: pl.DataFrame) -> dict[str, pl.DataFrame]:
    phenotypes = df.select(
        pl.col("targetFromSourceId").alias("target_from_source_id"),
        pl.col("targetInModel").alias("target_in_model"),
        pl.col("targetInModelMgiId").alias("target_in_model_mgi_id"),
        pl.col("targetInModelEnsemblId").alias("target_in_model_ensembl_id"),
        pl.col("modelPhenotypeId").alias("model_phenotype_id"),
        pl.col("modelPhenotypeLabel").alias("model_phenotype_label"),
    )
    classes = explode_struct_list(
        df, _KEYS, "modelPhenotypeClasses",
        {"id": "class_id", "label": "class_label"},
    )
    models = explode_struct_list(
        df, _KEYS, "biologicalModels",
        {
            "allelicComposition": "allelic_composition",
            "geneticBackground": "genetic_background",
            "id": "model_id",
            "literature": "literature",
        },
    )
    return {
        "mouse_phenotypes": phenotypes,
        "mouse_phenotype_classes": classes,
        "mouse_phenotype_models": models,
    }


TABLES_DOC = {
    "mouse_phenotypes": {
        "target_from_source_id":      ColumnSpec(identifier=EnsemblGeneId, required=True, description="Human Ensembl gene ID the model maps to"),
        "target_in_model":            ColumnSpec(description="Mouse gene symbol in the model"),
        "target_in_model_mgi_id":     ColumnSpec(description="MGI ID of the mouse gene"),
        "target_in_model_ensembl_id": ColumnSpec(identifier=EnsemblGeneId, description="Mouse Ensembl gene ID"),
        "model_phenotype_id":         ColumnSpec(required=True, description="Mammalian Phenotype (MP) term ID"),
        "model_phenotype_label":      ColumnSpec(description="Phenotype term label"),
    },
    "mouse_phenotype_classes": {
        "target_from_source_id": ColumnSpec(identifier=EnsemblGeneId, required=True, description="Human Ensembl gene ID"),
        "model_phenotype_id":    ColumnSpec(required=True, description="Mammalian Phenotype (MP) term ID"),
        "class_id":              ColumnSpec(description="Phenotype-class (top-level MP) term ID"),
        "class_label":           ColumnSpec(description="Phenotype-class label"),
    },
    "mouse_phenotype_models": {
        "target_from_source_id": ColumnSpec(identifier=EnsemblGeneId, required=True, description="Human Ensembl gene ID"),
        "model_phenotype_id":    ColumnSpec(required=True, description="Mammalian Phenotype (MP) term ID"),
        "allelic_composition":   ColumnSpec(description="Allelic composition of the model"),
        "genetic_background":    ColumnSpec(description="Genetic background of the model"),
        "model_id":              ColumnSpec(description="Source biological-model ID"),
        "literature":            ColumnSpec(description="Supporting literature references (list)"),
    },
}


@register
class OpenTargetsMousePhenotype(OpenTargetsProductPipeline):
    name = "opentargets_mouse_phenotype"
    raw_dirname = "mouse_phenotype"
    transform_module = "lacuna_etl.datasets.opentargets.mouse_phenotype"
    first_table = "mouse_phenotypes"
    tables_doc = TABLES_DOC
