from abc import ABC, abstractmethod
from pathlib import Path

from lacuna_etl.config import get_data_root, get_intermediate_root


class DatasetPipeline(ABC):
    name: str
    depends_on: list[str] = []

    def raw_path(self) -> Path:
        """Latest versioned directory produced by data_downloader."""
        root = get_data_root() / self.name
        if not root.exists():
            raise FileNotFoundError(f"Raw data not found: {root}")
        versions = sorted(root.iterdir())
        if not versions:
            raise FileNotFoundError(f"No versions found under: {root}")
        return versions[-1]

    def intermediate_path(self) -> Path:
        path = get_intermediate_root() / self.name
        path.mkdir(parents=True, exist_ok=True)
        return path

    @abstractmethod
    def extract(self) -> None:
        """Read from raw_path(), write intermediates to intermediate_path()."""

    @abstractmethod
    def transform(self) -> None:
        """Read from intermediate_path(), produce transformed outputs."""

    @abstractmethod
    def load(self) -> None:
        """Load transformed outputs into final destination."""

    def run(self) -> None:
        print(f"[{self.name}] extract")
        self.extract()
        print(f"[{self.name}] transform")
        self.transform()
        print(f"[{self.name}] load")
        self.load()
