import os
import tomllib
from pathlib import Path

_CONFIG_PATH = Path.home() / ".config" / "lacuna_etl" / "config.toml"


def get_data_root() -> Path:
    if env := os.environ.get("DL_DATA_ROOT"):
        return Path(env)
    if _CONFIG_PATH.exists():
        with open(_CONFIG_PATH, "rb") as f:
            cfg = tomllib.load(f)
        if path := cfg.get("data_root"):
            return Path(path)
    raise RuntimeError(
        "Data root not configured. Set DL_DATA_ROOT or run: etl configure <path>"
    )


def get_intermediate_root() -> Path:
    if env := os.environ.get("ETL_INTERMEDIATE_ROOT"):
        return Path(env)
    if _CONFIG_PATH.exists():
        with open(_CONFIG_PATH, "rb") as f:
            cfg = tomllib.load(f)
        if path := cfg.get("intermediate_root"):
            return Path(path)
    return Path(__file__).parents[3] / "intermediate"


def save_config(data_root: Path, intermediate_root: Path | None = None) -> None:
    _CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    lines = [f'data_root = "{data_root}"']
    if intermediate_root:
        lines.append(f'intermediate_root = "{intermediate_root}"')
    _CONFIG_PATH.write_text("\n".join(lines) + "\n")
