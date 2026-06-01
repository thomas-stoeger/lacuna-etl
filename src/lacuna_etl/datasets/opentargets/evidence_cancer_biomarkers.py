"""Open Targets Cancer Biomarkers evidence.

  evidence                              - one row per evidence record
  evidence_urls                         - reference links, exploded from `urls`
  evidence_biomarkers_gene_expression   - gene-expression biomarkers (biomarkers.geneExpression)
  evidence_biomarkers_genetic_variation - genetic-variation biomarkers (biomarkers.geneticVariation)
"""

import polars as pl

from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.opentargets import _evidence
from lacuna_etl.datasets.opentargets._base import OpenTargetsProductPipeline
from lacuna_etl.datasets.registry import register

_PARENT = [
    "evidence_id", "datasource_id", "datatype_id", "target_id", "target_from_source_id",
    "disease_id", "disease_from_source", "disease_from_source_mapped_id", "drug_id",
    "drug_from_source", "drug_response", "biomarker_name", "confidence", "literature",
    "quality_controls", "publication_date", "evidence_date", "score",
]
_CHILDREN = ["urls"]


def _biomarker_table(df: pl.DataFrame, subfield: str, fields: dict[str, str]) -> pl.DataFrame:
    """Explode one List(Struct) subfield of the `biomarkers` struct into a child table."""
    names = {f.name for f in df.schema["biomarkers"].fields}
    if subfield not in names:
        return pl.DataFrame()
    sub = (
        df.select(
            pl.col("id").alias("evidence_id"),
            pl.col("biomarkers").struct.field(subfield).alias("_list"),
        )
        .explode("_list")
        .filter(pl.col("_list").is_not_null())
        .unnest("_list")
    )
    return sub.select([pl.col("evidence_id")] + [pl.col(s).alias(o) for s, o in fields.items()])


def transform_file(df: pl.DataFrame) -> dict[str, pl.DataFrame]:
    tables = _evidence.build(df.drop("biomarkers"), _PARENT, _CHILDREN)
    if "biomarkers" in df.columns and isinstance(df.schema["biomarkers"], pl.Struct):
        tables["evidence_biomarkers_gene_expression"] = _biomarker_table(
            df, "geneExpression", {"id": "marker_id", "name": "marker_name"}
        )
        tables["evidence_biomarkers_genetic_variation"] = _biomarker_table(
            df, "geneticVariation",
            {"functionalConsequenceId": "functional_consequence_id", "id": "marker_id", "name": "marker_name"},
        )
    return tables


_EVIDENCE_ID = _evidence.FIELDS["evidence_id"][1]

TABLES_DOC = _evidence.build_doc(_PARENT, _CHILDREN)
TABLES_DOC["evidence_biomarkers_gene_expression"] = {
    "evidence_id": _EVIDENCE_ID,
    "marker_id":   ColumnSpec(description="Gene-expression biomarker ID"),
    "marker_name": ColumnSpec(description="Gene-expression biomarker name"),
}
TABLES_DOC["evidence_biomarkers_genetic_variation"] = {
    "evidence_id":              _EVIDENCE_ID,
    "functional_consequence_id": ColumnSpec(description="Sequence Ontology functional-consequence ID"),
    "marker_id":                ColumnSpec(description="Genetic-variation biomarker ID"),
    "marker_name":              ColumnSpec(description="Genetic-variation biomarker name"),
}


@register
class OpenTargetsCancerBiomarkers(OpenTargetsProductPipeline):
    name = "opentargets_evidence_cancer_biomarkers"
    raw_dirname = "evidence_cancer_biomarkers"
    transform_module = "lacuna_etl.datasets.opentargets.evidence_cancer_biomarkers"
    first_table = "evidence"
    tables_doc = TABLES_DOC
