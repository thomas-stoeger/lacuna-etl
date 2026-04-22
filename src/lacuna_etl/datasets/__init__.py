from lacuna_etl.datasets.registry import REGISTRY, register
from lacuna_etl.datasets import icite as _  # noqa: F401
from lacuna_etl.datasets import ncbi_gene2pubmed as __  # noqa: F401
from lacuna_etl.datasets import ncbi_gene_info as ___  # noqa: F401
from lacuna_etl.datasets import ncbi_gene_rif as ____  # noqa: F401

__all__ = ["REGISTRY", "register"]
