"""Open Targets gene-burden evidence.

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
    'project_id',
    'cohort_id',
    'statistical_method',
    'statistical_method_overview',
    'p_value_mantissa',
    'p_value_exponent',
    'beta',
    'beta_confidence_interval_lower',
    'beta_confidence_interval_upper',
    'odds_ratio',
    'odds_ratio_confidence_interval_lower',
    'odds_ratio_confidence_interval_upper',
    'ancestry',
    'ancestry_id',
    'study_sample_size',
    'study_cases',
    'study_cases_with_qualifying_variants',
    'resource_score',
    'release_version',
    'direction_on_trait',
    'direction_on_target',
    'literature',
    'allelic_requirements',
    'sex',
    'quality_controls',
    'publication_date',
    'evidence_date',
    'score',
]
_CHILDREN = ['urls']


def transform_file(df):
    return _evidence.build(df, _PARENT, _CHILDREN)


TABLES_DOC = _evidence.build_doc(_PARENT, _CHILDREN)


@register
class OpenTargetsGeneBurden(OpenTargetsProductPipeline):
    name = "opentargets_evidence_gene_burden"
    raw_dirname = "evidence_gene_burden"
    transform_module = "lacuna_etl.datasets.opentargets.evidence_gene_burden"
    first_table = "evidence"
    tables_doc = TABLES_DOC
