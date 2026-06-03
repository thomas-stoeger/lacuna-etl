"""Open Targets IMPC mouse-knockout phenotype evidence.

  evidence            - one row per evidence record
  evidence_model_phenotypes      - exploded from `diseaseModelAssociatedModelPhenotypes`
  evidence_human_phenotypes      - exploded from `diseaseModelAssociatedHumanPhenotypes`
"""

from lacuna_etl.datasets.opentargets import _evidence
from lacuna_etl.datasets.opentargets._base import OpenTargetsProductPipeline
from lacuna_etl.datasets.registry import register

_PARENT = [
    'evidence_id',
    'datasource_id',
    'datatype_id',
    'target_id',
    'target_from_source_id',
    'disease_id',
    'disease_from_source',
    'disease_from_source_id',
    'disease_from_source_mapped_id',
    'target_in_model',
    'target_in_model_mgi_id',
    'target_in_model_ensembl_id',
    'biological_model_id',
    'biological_model_genetic_background',
    'biological_model_allelic_composition',
    'resource_score',
    'direction_on_trait',
    'direction_on_target',
    'literature',
    'quality_controls',
    'publication_date',
    'evidence_date',
    'score',
]
_CHILDREN = ['diseaseModelAssociatedModelPhenotypes', 'diseaseModelAssociatedHumanPhenotypes']


def transform_file(df):
    return _evidence.build(df, _PARENT, _CHILDREN)


TABLES_DOC = _evidence.build_doc(_PARENT, _CHILDREN)


@register
class OpenTargetsImpc(OpenTargetsProductPipeline):
    name = "opentargets_evidence_impc"
    raw_dirname = "evidence_impc"
    transform_module = "lacuna_etl.datasets.opentargets.evidence_impc"
    first_table = "evidence"
    tables_doc = TABLES_DOC
