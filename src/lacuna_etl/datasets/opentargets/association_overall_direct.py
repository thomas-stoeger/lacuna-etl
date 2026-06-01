"""Open Targets overall direct target-disease associations.

  associations            - one row per (target, disease) with its overall direct score
  associations_timeseries - one row per (target, disease, year) novelty/score point
"""

from lacuna_etl.datasets.opentargets._association import make_tables_doc, transform_file  # noqa: F401
from lacuna_etl.datasets.opentargets._base import OpenTargetsProductPipeline
from lacuna_etl.datasets.registry import register

TABLES_DOC = make_tables_doc("Overall direct association score in [0, 1]")


@register
class OpenTargetsAssociationOverallDirect(OpenTargetsProductPipeline):
    name = "opentargets_association_overall_direct"
    raw_dirname = "association_overall_direct"
    transform_module = "lacuna_etl.datasets.opentargets.association_overall_direct"
    first_table = "associations"
    tables_doc = TABLES_DOC
