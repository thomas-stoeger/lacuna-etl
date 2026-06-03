"""Open Targets Europe PMC text-mined literature evidence.

  evidence            - one row per evidence record
  evidence_sentences             - exploded from `textMiningSentences`
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
    'publication_year',
    'resource_score',
    'literature',
    'pmc_ids',
    'quality_controls',
    'publication_date',
    'evidence_date',
    'score',
]
_CHILDREN = ['textMiningSentences']


def transform_file(df):
    return _evidence.build(df, _PARENT, _CHILDREN)


TABLES_DOC = _evidence.build_doc(_PARENT, _CHILDREN)


@register
class OpenTargetsEuropePmc(OpenTargetsProductPipeline):
    name = "opentargets_evidence_europepmc"
    raw_dirname = "evidence_europepmc"
    transform_module = "lacuna_etl.datasets.opentargets.evidence_europepmc"
    first_table = "evidence"
    tables_doc = TABLES_DOC
