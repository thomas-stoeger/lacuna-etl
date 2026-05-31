"""Open Targets per-product pipelines.

Each module under this package registers one DatasetPipeline subclass
(`opentargets_target`, `opentargets_disease`, ...) that transforms the
corresponding Open Targets Parquet product into per-table Parquet shards.
"""

from lacuna_etl.datasets.opentargets import association_overall_direct as _assoc  # noqa: F401
from lacuna_etl.datasets.opentargets import disease as _disease  # noqa: F401
from lacuna_etl.datasets.opentargets import drug_molecule as _drug  # noqa: F401
from lacuna_etl.datasets.opentargets import mouse_phenotype as _mouse  # noqa: F401
from lacuna_etl.datasets.opentargets import target as _target  # noqa: F401
from lacuna_etl.datasets.opentargets import target_essentiality as _essentiality  # noqa: F401
