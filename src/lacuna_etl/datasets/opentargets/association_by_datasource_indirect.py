"""Open Targets target-disease associations broken down by datasource (indirect).

Like `association_by_datasource_direct`, but the score also propagates evidence
from descendant disease-ontology terms. One row per (target, disease, datasource);
the datasource is carried in `aggregation_value`.

  associations            - per-datasource indirect association score
  associations_timeseries - one row per (target, disease, datasource, year) point
"""

from lacuna_etl.datasets.opentargets._association import make_tables_doc, transform_file  # noqa: F401
from lacuna_etl.datasets.opentargets._base import OpenTargetsProductPipeline
from lacuna_etl.datasets.registry import register

TABLES_DOC = make_tables_doc(
    "Per-datasource indirect association score in [0, 1] (includes evidence propagated "
    "from descendant disease terms)"
)


@register
class OpenTargetsAssociationByDatasourceIndirect(OpenTargetsProductPipeline):
    name = "opentargets_association_by_datasource_indirect"
    raw_dirname = "association_by_datasource_indirect"
    transform_module = "lacuna_etl.datasets.opentargets.association_by_datasource_indirect"
    first_table = "associations"
    tables_doc = TABLES_DOC
