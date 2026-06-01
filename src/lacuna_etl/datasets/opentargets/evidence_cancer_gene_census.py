"""Open Targets COSMIC Cancer Gene Census somatic-mutation evidence.

  evidence            - one row per evidence record
  evidence_mutated_samples       - exploded from `mutatedSamples`
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
    'disease_from_source_id',
    'disease_from_source_mapped_id',
    'study_id',
    'resource_score',
    'direction_on_trait',
    'direction_on_target',
    'literature',
    'quality_controls',
    'publication_date',
    'evidence_date',
    'score',
]
_CHILDREN = ['mutatedSamples']


def transform_file(df):
    return _evidence.build(df, _PARENT, _CHILDREN)


TABLES_DOC = _evidence.build_doc(_PARENT, _CHILDREN)


@register
class OpenTargetsCancerGeneCensus(OpenTargetsProductPipeline):
    name = "opentargets_evidence_cancer_gene_census"
    raw_dirname = "evidence_cancer_gene_census"
    transform_module = "lacuna_etl.datasets.opentargets.evidence_cancer_gene_census"
    first_table = "evidence"
    tables_doc = TABLES_DOC
