"""Open Targets overall indirect target-disease associations.

Like `association_overall_direct`, but the score also propagates evidence from
descendant disease-ontology terms up to each disease.

  associations            - one row per (target, disease) with its overall indirect score
  associations_timeseries - one row per (target, disease, year) novelty/score point
"""

from lacuna_etl.datasets.opentargets._association import make_tables_doc, transform_file  # noqa: F401
from lacuna_etl.datasets.opentargets._base import OpenTargetsProductPipeline
from lacuna_etl.datasets.registry import register

TABLES_DOC = make_tables_doc(
    "Overall indirect association score in [0, 1] (includes evidence propagated from "
    "descendant disease terms)"
)


@register
class OpenTargetsAssociationOverallIndirect(OpenTargetsProductPipeline):
    name = "opentargets_association_overall_indirect"
    raw_dirname = "association_overall_indirect"
    transform_module = "lacuna_etl.datasets.opentargets.association_overall_indirect"
    first_table = "associations"
    tables_doc = TABLES_DOC
