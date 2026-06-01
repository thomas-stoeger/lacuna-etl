from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from lacuna_etl.core.pipeline import DatasetPipeline

REGISTRY: dict[str, type["DatasetPipeline"]] = {}


def register(cls: type["DatasetPipeline"]) -> type["DatasetPipeline"]:
    REGISTRY[cls.name] = cls
    return cls
