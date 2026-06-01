"""OpenAlex entity pipelines.

Each module under this package registers one DatasetPipeline subclass
(`openalex_works`, `openalex_authors`, ...) that streams the corresponding
OpenAlex S3 snapshot entity into Parquet shards.
"""

from lacuna_etl.datasets.openalex import authors as _authors  # noqa: F401
from lacuna_etl.datasets.openalex import awards as _awards  # noqa: F401
from lacuna_etl.datasets.openalex import concepts as _concepts  # noqa: F401
from lacuna_etl.datasets.openalex import continents as _continents  # noqa: F401
from lacuna_etl.datasets.openalex import countries as _countries  # noqa: F401
from lacuna_etl.datasets.openalex import domains as _domains  # noqa: F401
from lacuna_etl.datasets.openalex import fields as _fields  # noqa: F401
from lacuna_etl.datasets.openalex import funders as _funders  # noqa: F401
from lacuna_etl.datasets.openalex import institution_types as _institution_types  # noqa: F401
from lacuna_etl.datasets.openalex import institutions as _institutions  # noqa: F401
from lacuna_etl.datasets.openalex import keywords as _keywords  # noqa: F401
from lacuna_etl.datasets.openalex import languages as _languages  # noqa: F401
from lacuna_etl.datasets.openalex import licenses as _licenses  # noqa: F401
from lacuna_etl.datasets.openalex import publishers as _publishers  # noqa: F401
from lacuna_etl.datasets.openalex import sdgs as _sdgs  # noqa: F401
from lacuna_etl.datasets.openalex import source_types as _source_types  # noqa: F401
from lacuna_etl.datasets.openalex import sources as _sources  # noqa: F401
from lacuna_etl.datasets.openalex import subfields as _subfields  # noqa: F401
from lacuna_etl.datasets.openalex import topics as _topics  # noqa: F401
from lacuna_etl.datasets.openalex import work_types as _work_types  # noqa: F401
from lacuna_etl.datasets.openalex import works as _works  # noqa: F401
