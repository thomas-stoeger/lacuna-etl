"""Open Targets target-disease associations broken down by datatype (direct).

One row per (target, disease, datatype); the datatype is carried in
`aggregation_value`.

  associations            - per-datatype direct association score
  associations_timeseries - one row per (target, disease, datatype, year) point
"""

from lacuna_etl.datasets.opentargets._association import make_tables_doc, transform_file  # noqa: F401
from lacuna_etl.datasets.opentargets._base import OpenTargetsProductPipeline
from lacuna_etl.datasets.registry import register

TABLES_DOC = make_tables_doc("Per-datatype direct association score in [0, 1]")


@register
class OpenTargetsAssociationByDatatypeDirect(OpenTargetsProductPipeline):
    name = "opentargets_association_by_datatype_direct"
    raw_dirname = "association_by_datatype_direct"
    transform_module = "lacuna_etl.datasets.opentargets.association_by_datatype_direct"
    first_table = "associations"
    tables_doc = TABLES_DOC
