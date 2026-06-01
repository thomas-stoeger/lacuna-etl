"""Open Targets target-disease associations broken down by datatype (indirect).

Like `association_by_datatype_direct`, but the score also propagates evidence from
descendant disease-ontology terms. One row per (target, disease, datatype); the
datatype is carried in `aggregation_value`.

  associations            - per-datatype indirect association score
  associations_timeseries - one row per (target, disease, datatype, year) point
"""

from lacuna_etl.datasets.opentargets._association import make_tables_doc, transform_file  # noqa: F401
from lacuna_etl.datasets.opentargets._base import OpenTargetsProductPipeline
from lacuna_etl.datasets.registry import register

TABLES_DOC = make_tables_doc(
    "Per-datatype indirect association score in [0, 1] (includes evidence propagated "
    "from descendant disease terms)"
)


@register
class OpenTargetsAssociationByDatatypeIndirect(OpenTargetsProductPipeline):
    name = "opentargets_association_by_datatype_indirect"
    raw_dirname = "association_by_datatype_indirect"
    transform_module = "lacuna_etl.datasets.opentargets.association_by_datatype_indirect"
    first_table = "associations"
    tables_doc = TABLES_DOC
