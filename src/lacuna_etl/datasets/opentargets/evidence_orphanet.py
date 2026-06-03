"""Open Targets Orphanet gene-disease evidence.

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
    'target_from_source',
    'disease_id',
    'disease_from_source',
    'disease_from_source_id',
    'disease_from_source_mapped_id',
    'confidence',
    'variant_functional_consequence_id',
    'direction_on_trait',
    'direction_on_target',
    'literature',
    'allele_origins',
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
class OpenTargetsOrphanet(OpenTargetsProductPipeline):
    name = "opentargets_evidence_orphanet"
    raw_dirname = "evidence_orphanet"
    transform_module = "lacuna_etl.datasets.opentargets.evidence_orphanet"
    first_table = "evidence"
    tables_doc = TABLES_DOC
