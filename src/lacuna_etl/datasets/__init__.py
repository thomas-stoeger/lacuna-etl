from lacuna_etl.datasets.registry import REGISTRY, register
from lacuna_etl.datasets import icite as _  # noqa: F401
from lacuna_etl.datasets import ncbi_gene2pubmed as __  # noqa: F401
from lacuna_etl.datasets import ncbi_gene_history as ___  # noqa: F401
from lacuna_etl.datasets import ncbi_gene_info as ____  # noqa: F401
from lacuna_etl.datasets import ncbi_gene_rif as _____  # noqa: F401
from lacuna_etl.datasets import openalex as ______  # noqa: F401

__all__ = ["REGISTRY", "register"]
