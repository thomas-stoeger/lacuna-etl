"""Reactome — the open, manually-curated pathway knowledgebase.

Reactome publishes its pathway set and gene/protein→pathway mappings as a handful
of headerless TSVs. This pipeline ingests five of them: the pathway list
(``ReactomePathways.txt``), the pathway hierarchy (``ReactomePathwaysRelation.txt``),
and the three "all levels" mappings that link an external molecule identifier to
every pathway it participates in (``Ensembl2Reactome``, ``UniProt2Reactome``,
``NCBI2Reactome``). The mappings are a few million rows each but read comfortably
with vectorised pandas, so this is an in-memory pipeline: ``extract`` reads and
normalises each file, ``transform`` is a no-op (faithful projection), and ``load``
types, validates, and writes.

The grain key is the Reactome stable pathway id (``ReactomePathwayId``,
``R-<species>-<number>``). The mapping tables' source-molecule columns are kept as
documented plain strings rather than typed: the Reactome files span every model
organism and mix identifier flavours — ``Ensembl2Reactome`` carries gene/transcript/
protein ids across species (human ``ENSG…`` alongside worm/fly forms),
``UniProt2Reactome`` carries isoform-suffixed accessions, and ``NCBI2Reactome`` is
overwhelmingly Entrez Gene ids but mixes in a few GenBank/RefSeq accessions. None has
a single canonical form here, so they follow the heterogeneous-id precedent. The
``evidence_code`` (IEA/TAS/…) and ``species`` are documented free strings. See
docs/DESIGN.md for the table inventory.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from lacuna_etl.core.identifiers import ReactomePathwayId
from lacuna_etl.core.pipeline import DatasetPipeline
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.registry import register

PATHWAYS_SCHEMA = {
    "pathway_id": ColumnSpec(identifier=ReactomePathwayId, required=True, description="Reactome stable pathway id (R-<species>-<number>); the grain key"),
    "pathway_name": ColumnSpec(required=True, description="Human-readable pathway name"),
    "species": ColumnSpec(required=True, description="Species the pathway belongs to (e.g. 'Homo sapiens')"),
}

PATHWAY_RELATIONS_SCHEMA = {
    "parent_pathway_id": ColumnSpec(identifier=ReactomePathwayId, required=True, description="Reactome pathway id of the parent (containing) pathway"),
    "child_pathway_id": ColumnSpec(identifier=ReactomePathwayId, required=True, description="Reactome pathway id of the direct child (sub-)pathway"),
}

# The three "*2Reactome_All_Levels" mappings share a layout; only the source-molecule
# column differs in flavour (and stays a documented plain string in every case).
def _mapping_schema(source_col: str, source_desc: str) -> dict[str, ColumnSpec]:
    return {
        source_col: ColumnSpec(required=True, description=source_desc),
        "pathway_id": ColumnSpec(identifier=ReactomePathwayId, required=True, description="Reactome stable pathway id the molecule participates in (all levels of the hierarchy)"),
        "pathway_browser_url": ColumnSpec(description="Reactome PathwayBrowser URL for the pathway"),
        "pathway_name": ColumnSpec(description="Human-readable pathway name"),
        "evidence_code": ColumnSpec(description="Reactome evidence code (e.g. IEA = inferred from electronic annotation, TAS = traceable author statement); a documented free string"),
        "species": ColumnSpec(description="Species of the mapping (e.g. 'Homo sapiens')"),
    }


ENSEMBL2PATHWAY_SCHEMA = _mapping_schema(
    "ensembl_id",
    "Ensembl identifier (gene/transcript/protein, cross-species; human ENSG… alongside other organisms' forms); kept verbatim as a plain string",
)
UNIPROT2PATHWAY_SCHEMA = _mapping_schema(
    "uniprot_id",
    "UniProt accession (may carry an isoform suffix, cross-species); kept verbatim as a plain string",
)
NCBI2PATHWAY_SCHEMA = _mapping_schema(
    "ncbi_id",
    "NCBI identifier — overwhelmingly an Entrez Gene id, but the source mixes in a few GenBank/RefSeq accessions, so kept verbatim as a plain string",
)


@register
class Reactome(DatasetPipeline):
    name = "reactome"

    _TABLES = [
        ("pathways", PATHWAYS_SCHEMA),
        ("pathway_relations", PATHWAY_RELATIONS_SCHEMA),
        ("ensembl2pathway", ENSEMBL2PATHWAY_SCHEMA),
        ("uniprot2pathway", UNIPROT2PATHWAY_SCHEMA),
        ("ncbi2pathway", NCBI2PATHWAY_SCHEMA),
    ]

    # (output stem, source filename) pairs.
    _SOURCES = {
        "pathways": "ReactomePathways.txt",
        "pathway_relations": "ReactomePathwaysRelation.txt",
        "ensembl2pathway": "Ensembl2Reactome_All_Levels.txt",
        "uniprot2pathway": "UniProt2Reactome_All_Levels.txt",
        "ncbi2pathway": "NCBI2Reactome_All_Levels.txt",
    }

    def _read(self, stem: str, columns: list[str]) -> pd.DataFrame:
        path = self.raw_path() / self._SOURCES[stem]
        if not path.exists():
            raise FileNotFoundError(f"reactome: expected {self._SOURCES[stem]} under {self.raw_path()}")
        df = pd.read_csv(
            path, sep="\t", header=None, names=columns, dtype=str,
            keep_default_na=False, na_filter=False,
        )
        # Strip whitespace (some pathway names carry trailing spaces); empty -> null.
        for c in columns:
            df[c] = df[c].str.strip()
            df[c] = df[c].where(df[c] != "", None)
        return df

    def extract(self) -> None:
        counts = {}
        for stem, schema in self._TABLES:
            df = self._read(stem, list(schema))
            self.save_parquet(df, self.intermediate_path() / f"{stem}.parquet")
            counts[stem] = len(df)
        print("[reactome] " + ", ".join(f"{k}: {v:,}" for k, v in counts.items()))

    def transform(self) -> None:
        # Faithful projection; reading/normalisation in extract, validation in load.
        pass

    def load(self) -> None:
        for stem, schema in self._TABLES:
            df = self.load_parquet(self.intermediate_path() / f"{stem}.parquet")
            df = df[list(schema)]
            df = self.apply_schema(df, schema)
            self.save_parquet(df, self.output_path() / f"{stem}.parquet")
            self.save_schema_yaml(schema, stem)
