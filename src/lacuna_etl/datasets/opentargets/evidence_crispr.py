"""Open Targets Project Score (CRISPR) cancer-dependency evidence.

  evidence            - one row per evidence record
  evidence_disease_cell_lines    - exploded from `diseaseCellLines`
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
    'target_from_source',
    'disease_id',
    'disease_from_source',
    'disease_from_source_mapped_id',
    'resource_score',
    'literature',
    'quality_controls',
    'publication_date',
    'evidence_date',
    'score',
]
_CHILDREN = ['diseaseCellLines']


def transform_file(df):
    return _evidence.build(df, _PARENT, _CHILDREN)


TABLES_DOC = _evidence.build_doc(_PARENT, _CHILDREN)


@register
class OpenTargetsCrispr(OpenTargetsProductPipeline):
    name = "opentargets_evidence_crispr"
    raw_dirname = "evidence_crispr"
    transform_module = "lacuna_etl.datasets.opentargets.evidence_crispr"
    first_table = "evidence"
    tables_doc = TABLES_DOC
