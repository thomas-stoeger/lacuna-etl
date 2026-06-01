"""Open Targets target-disease associations broken down by datasource (direct).

One row per (target, disease, datasource); the datasource is carried in
`aggregation_value`.

  associations            - per-datasource direct association score
  associations_timeseries - one row per (target, disease, datasource, year) point
"""

from lacuna_etl.datasets.opentargets._association import make_tables_doc, transform_file  # noqa: F401
from lacuna_etl.datasets.opentargets._base import OpenTargetsProductPipeline
from lacuna_etl.datasets.registry import register

TABLES_DOC = make_tables_doc("Per-datasource direct association score in [0, 1]")


@register
class OpenTargetsAssociationByDatasourceDirect(OpenTargetsProductPipeline):
    name = "opentargets_association_by_datasource_direct"
    raw_dirname = "association_by_datasource_direct"
    transform_module = "lacuna_etl.datasets.opentargets.association_by_datasource_direct"
    first_table = "associations"
    tables_doc = TABLES_DOC
