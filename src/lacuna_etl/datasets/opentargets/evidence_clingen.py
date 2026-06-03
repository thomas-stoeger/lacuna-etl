"""Open Targets ClinGen gene-disease validity evidence.

  evidence            - one row per evidence record
  evidence_urls                  - exploded from `urls`
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
    'confidence',
    'study_id',
    'release_date',
    'allelic_requirements',
    'quality_controls',
    'evidence_date',
    'score',
]
_CHILDREN = ['urls']


def transform_file(df):
    return _evidence.build(df, _PARENT, _CHILDREN)


TABLES_DOC = _evidence.build_doc(_PARENT, _CHILDREN)


@register
class OpenTargetsClingen(OpenTargetsProductPipeline):
    name = "opentargets_evidence_clingen"
    raw_dirname = "evidence_clingen"
    transform_module = "lacuna_etl.datasets.opentargets.evidence_clingen"
    first_table = "evidence"
    tables_doc = TABLES_DOC
