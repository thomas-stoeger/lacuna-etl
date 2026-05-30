from lacuna_etl.datasets.registry import REGISTRY, register
from lacuna_etl.datasets import icite as _  # noqa: F401
from lacuna_etl.datasets import ncbi_gene2go as __  # noqa: F401
from lacuna_etl.datasets import ncbi_gene2pubmed as ___  # noqa: F401
from lacuna_etl.datasets import ncbi_gene_history as ____  # noqa: F401
from lacuna_etl.datasets import ncbi_gene_info as _____  # noqa: F401
from lacuna_etl.datasets import ncbi_gene_rif as ______  # noqa: F401
from lacuna_etl.datasets import openalex as _______  # noqa: F401
from lacuna_etl.datasets import pubmed as ________  # noqa: F401
from lacuna_etl.datasets import pubtator3 as _________  # noqa: F401
from lacuna_etl.datasets import pmid_openalex as __________  # noqa: F401

__all__ = ["REGISTRY", "register"]
