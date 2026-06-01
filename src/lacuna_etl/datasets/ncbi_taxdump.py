import tarfile

import pandas as pd

from lacuna_etl.core.identifiers import NcbiTaxId
from lacuna_etl.core.pipeline import DatasetPipeline
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.registry import register

# The NCBI taxonomy dump ships as a single tar.gz of pipe-delimited ``.dmp``
# files. Each row ends with a ``\t|`` terminator and fields are separated by
# ``\t|\t``; we parse the members we need directly from the archive rather than
# extracting to disk.
NODES_SCHEMA = {
    "tax_id":          ColumnSpec(identifier=NcbiTaxId, description="NCBI taxonomy ID"),
    "parent_tax_id":   ColumnSpec(identifier=NcbiTaxId, description="Taxonomy ID of the parent node (the root node 1 is its own parent)"),
    "rank":            ColumnSpec(description="Taxonomic rank, e.g. 'species', 'genus', 'no rank'"),
    "division_id":     ColumnSpec(description="GenBank division ID (links to division.dmp)"),
    "genetic_code_id": ColumnSpec(description="Genetic code ID used for nuclear translation"),
}

NAMES_SCHEMA = {
    "tax_id":      ColumnSpec(identifier=NcbiTaxId, description="NCBI taxonomy ID"),
    "name":        ColumnSpec(description="Name string (e.g. scientific name, synonym, common name)"),
    "unique_name": ColumnSpec(description="Disambiguated name, present only when 'name' is not unique across the tree"),
    "name_class":  ColumnSpec(description="Kind of name, e.g. 'scientific name', 'synonym', 'authority', 'genbank common name'"),
}

MERGED_SCHEMA = {
    "old_tax_id": ColumnSpec(identifier=NcbiTaxId, description="Taxonomy ID that was merged into another node"),
    "new_tax_id": ColumnSpec(identifier=NcbiTaxId, description="Current taxonomy ID the old ID now resolves to"),
}


def _read_dmp(tar: tarfile.TarFile, member: str) -> list[list[str]]:
    """Parse one ``.dmp`` member into a list of stripped string fields per row."""
    fh = tar.extractfile(member)
    if fh is None:
        raise FileNotFoundError(f"{member} missing from taxdump archive")
    rows: list[list[str]] = []
    for raw in fh:
        line = raw.decode("utf-8").rstrip("\n")
        if line.endswith("\t|"):
            line = line[:-2]
        rows.append([f.strip() for f in line.split("\t|\t")])
    return rows


@register
class NcbiTaxdump(DatasetPipeline):
    name = "ncbi_taxdump"

    def extract(self) -> None:
        src = self.raw_path() / "taxdump.tar.gz"
        with tarfile.open(src, "r:gz") as tar:
            # nodes.dmp columns: tax_id, parent, rank, embl code, division id,
            # inherited div flag, genetic code id, ... (further flags ignored).
            nodes_rows = _read_dmp(tar, "nodes.dmp")
            nodes = pd.DataFrame(
                {
                    "tax_id":          [r[0] for r in nodes_rows],
                    "parent_tax_id":   [r[1] for r in nodes_rows],
                    "rank":            [r[2] for r in nodes_rows],
                    "division_id":     [r[4] for r in nodes_rows],
                    "genetic_code_id": [r[6] for r in nodes_rows],
                }
            )

            # names.dmp columns: tax_id, name_txt, unique name, name class.
            names_rows = _read_dmp(tar, "names.dmp")
            names = pd.DataFrame(
                {
                    "tax_id":      [r[0] for r in names_rows],
                    "name":        [r[1] for r in names_rows],
                    "unique_name": [r[2] for r in names_rows],
                    "name_class":  [r[3] for r in names_rows],
                }
            )

            # merged.dmp columns: old_tax_id, new_tax_id.
            merged_rows = _read_dmp(tar, "merged.dmp")
            merged = pd.DataFrame(
                {
                    "old_tax_id": [r[0] for r in merged_rows],
                    "new_tax_id": [r[1] for r in merged_rows],
                }
            )

        # Empty string fields (e.g. unique_name) are null.
        for df in (nodes, names, merged):
            df.replace("", pd.NA, inplace=True)

        nodes = self.apply_schema(nodes, NODES_SCHEMA)
        names = self.apply_schema(names, NAMES_SCHEMA)
        merged = self.apply_schema(merged, MERGED_SCHEMA)

        self.save_parquet(nodes, self.intermediate_path() / "taxonomy_nodes.parquet")
        self.save_parquet(names, self.intermediate_path() / "taxonomy_names.parquet")
        self.save_parquet(merged, self.intermediate_path() / "taxonomy_merged.parquet")

    def transform(self) -> None:
        # Pure parse-and-validate dataset; nothing to reconcile across tables.
        pass

    def load(self) -> None:
        for stem, schema in (
            ("taxonomy_nodes", NODES_SCHEMA),
            ("taxonomy_names", NAMES_SCHEMA),
            ("taxonomy_merged", MERGED_SCHEMA),
        ):
            df = self.load_parquet(self.intermediate_path() / f"{stem}.parquet")
            self.save_parquet(df, self.output_path() / f"{stem}.parquet")
            self.save_schema_yaml(schema, stem)
