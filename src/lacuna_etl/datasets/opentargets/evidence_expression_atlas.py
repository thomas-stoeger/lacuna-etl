"""Open Targets Expression Atlas differential-expression evidence.

  evidence            - one row per evidence record linking a target to a disease
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
    'disease_from_source_mapped_id',
    'confidence',
    'study_id',
    'study_overview',
    'contrast',
    'log2_fold_change_value',
    'log2_fold_change_percentile_rank',
    'resource_score',
    'literature',
    'biosamples_from_source',
    'quality_controls',
    'publication_date',
    'evidence_date',
    'score',
]
_CHILDREN = []


def transform_file(df):
    return _evidence.build(df, _PARENT, _CHILDREN)


TABLES_DOC = _evidence.build_doc(_PARENT, _CHILDREN)


@register
class OpenTargetsExpressionAtlas(OpenTargetsProductPipeline):
    name = "opentargets_evidence_expression_atlas"
    raw_dirname = "evidence_expression_atlas"
    transform_module = "lacuna_etl.datasets.opentargets.evidence_expression_atlas"
    first_table = "evidence"
    tables_doc = TABLES_DOC
