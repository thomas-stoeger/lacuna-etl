import typer

from lacuna_etl.config import get_data_root, save_config
from lacuna_etl.datasets import REGISTRY

app = typer.Typer()


@app.command()
def configure(
    data_root: str = typer.Argument(..., help="Path to data_downloader data root"),
    output_root: str = typer.Option(None, help="Path for final output files"),
    intermediate_root: str = typer.Option(None, help="Path for intermediate files"),
) -> None:
    from pathlib import Path
    save_config(
        Path(data_root),
        Path(output_root) if output_root else None,
        Path(intermediate_root) if intermediate_root else None,
    )
    typer.echo(f"Configured data root: {data_root}")
    if output_root:
        typer.echo(f"Configured output root: {output_root}")


@app.command("list")
def list_datasets() -> None:
    if not REGISTRY:
        typer.echo("No datasets registered.")
        return
    for name, cls in sorted(REGISTRY.items()):
        deps = ", ".join(cls.depends_on) if cls.depends_on else "-"
        typer.echo(f"{name:30s}  depends_on: {deps}")


@app.command()
def run(
    dataset: str = typer.Argument(..., help="Dataset name to run"),
) -> None:
    if dataset not in REGISTRY:
        typer.echo(f"Unknown dataset: {dataset}", err=True)
        raise typer.Exit(1)
    REGISTRY[dataset]().run()


@app.command()
def check(
    dataset: str = typer.Argument(None, help="Only check this dataset (default: all)"),
) -> None:
    """Diff on-disk schema sidecars against the code's ColumnSpec definitions.

    The sidecar YAML is generated from the same ColumnSpec objects used for validation,
    so a freshly-written sidecar always matches the code. But once written, an output
    can fall behind a later code change. This re-derives each dataset's expected sidecar
    (via expected_schemas()) and compares it to the .yml on disk, reporting:

      DRIFT   - the .yml differs from the code (the output predates a code change; rerun)
      ORPHAN  - a .yml on disk has no schema in the code (a removed/renamed table)
      MISSING - the code declares a table with no .yml on disk (dataset not run yet)

    Exits non-zero if any DRIFT or ORPHAN is found (MISSING alone is informational).
    """
    from pathlib import Path

    import yaml

    from lacuna_etl.config import get_output_root

    if dataset is not None and dataset not in REGISTRY:
        typer.echo(f"Unknown dataset: {dataset}", err=True)
        raise typer.Exit(1)
    names = [dataset] if dataset else sorted(REGISTRY)
    out_root = get_output_root()

    n_ok = n_drift = n_orphan = n_missing = 0

    for name in names:
        try:
            schemas = REGISTRY[name]().expected_schemas()
        except Exception as e:  # noqa: BLE001
            typer.echo(f"[{name}] DRIFT: cannot introspect schemas: {e}")
            n_drift += 1
            continue

        ds_dir = out_root / name
        for table, schema in schemas.items():
            expected = {col: spec.yaml_entry() for col, spec in schema.items()}
            yml = ds_dir / f"{table}.yml"
            if not yml.exists():
                n_missing += 1
                typer.echo(f"[{name}/{table}] MISSING: no sidecar on disk")
                continue
            ondisk = yaml.safe_load(yml.read_text()) or {}
            if list(ondisk.items()) == list(expected.items()):
                n_ok += 1
                continue
            n_drift += 1
            added = [c for c in expected if c not in ondisk]
            removed = [c for c in ondisk if c not in expected]
            changed = [c for c in expected if c in ondisk and expected[c] != ondisk[c]]
            parts = []
            if added:
                parts.append(f"+{added}")
            if removed:
                parts.append(f"-{removed}")
            if changed:
                parts.append(f"changed {changed}")
            if not parts:
                parts.append("column order changed")
            typer.echo(f"[{name}/{table}] DRIFT: {'; '.join(parts)}")

        # Orphan sidecars: a .yml on disk that the code no longer declares.
        if ds_dir.exists():
            for yml in sorted(ds_dir.glob("*.yml")):
                if yml.stem not in schemas:
                    n_orphan += 1
                    typer.echo(f"[{name}/{yml.stem}] ORPHAN: sidecar on disk has no schema in code")

    typer.echo(
        f"\nchecked {len(names)} dataset(s): {n_ok} ok, {n_drift} drift, "
        f"{n_orphan} orphan, {n_missing} missing"
    )
    if n_drift or n_orphan:
        raise typer.Exit(1)
