"""Open Targets ClinVar (EVA) somatic evidence.

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
    'disease_from_source_id',
    'disease_from_source_mapped_id',
    'confidence',
    'study_id',
    'variant_id',
    'variant_rs_id',
    'variant_from_source_id',
    'variant_hgvs_id',
    'variant_functional_consequence_id',
    'release_date',
    'direction_on_trait',
    'direction_on_target',
    'literature',
    'allele_origins',
    'allelic_requirements',
    'clinical_significances',
    'cohort_phenotypes',
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
class OpenTargetsEvaSomatic(OpenTargetsProductPipeline):
    name = "opentargets_evidence_eva_somatic"
    raw_dirname = "evidence_eva_somatic"
    transform_module = "lacuna_etl.datasets.opentargets.evidence_eva_somatic"
    first_table = "evidence"
    tables_doc = TABLES_DOC
