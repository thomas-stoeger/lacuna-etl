"""Open Targets CRISPR screen evidence.

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
    'disease_from_source',
    'disease_from_source_mapped_id',
    'study_id',
    'study_overview',
    'project_id',
    'contrast',
    'cell_type',
    'genetic_background',
    'crispr_screen_library',
    'statistical_test_tail',
    'log2_fold_change_value',
    'resource_score',
    'literature',
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
class OpenTargetsCrisprScreen(OpenTargetsProductPipeline):
    name = "opentargets_evidence_crispr_screen"
    raw_dirname = "evidence_crispr_screen"
    transform_module = "lacuna_etl.datasets.opentargets.evidence_crispr_screen"
    first_table = "evidence"
    tables_doc = TABLES_DOC
