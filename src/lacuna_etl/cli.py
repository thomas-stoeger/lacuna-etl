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
