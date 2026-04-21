from lacuna_etl.datasets.registry import REGISTRY, register
from lacuna_etl.datasets import ncbi_gene2pubmed as _  # noqa: F401

__all__ = ["REGISTRY", "register"]
