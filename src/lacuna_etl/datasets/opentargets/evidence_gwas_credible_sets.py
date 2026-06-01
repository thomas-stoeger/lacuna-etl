"""Open Targets GWAS credible-set evidence.

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
    'study_locus_id',
    'resource_score',
    'curation_date',
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
class OpenTargetsGwasCredibleSets(OpenTargetsProductPipeline):
    name = "opentargets_evidence_gwas_credible_sets"
    raw_dirname = "evidence_gwas_credible_sets"
    transform_module = "lacuna_etl.datasets.opentargets.evidence_gwas_credible_sets"
    first_table = "evidence"
    tables_doc = TABLES_DOC
