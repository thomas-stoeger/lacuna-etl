"""Ensembl GTF — the Ensembl (EMBL-EBI) genome annotation in GTF format.

The download holds one gzipped GTF per species (human, mouse, rat, zebrafish,
chicken) under ``<version>/<species>/<Species>.<assembly>.<release>.gtf.gz``. A GTF
is a 9-column tab file (one row per annotation feature) whose 9th column is a
``key "value";`` attribute string. The human file alone is ~10.6M feature rows, so
this is a polars pipeline: ``extract`` reads each species GTF, parses the attributes
into columns, and writes per-species/per-table shards (restartable via a per-species
done-marker); ``transform`` is a no-op; ``load`` concatenates the shards per table,
validates, and streams them to the output.

Three tables separate the annotation's natural grains: ``genes`` (one row per gene
feature), ``transcripts`` (one row per transcript feature), and ``features`` (one row
per sub-feature line — exon, CDS, UTRs, codons, Selenocysteine). The gene id
(``EnsemblGeneId``) keys ``genes`` and links the others; transcript and protein ids
use the new ``EnsemblTranscriptId``/``EnsemblProteinId`` types (the species infix
varies, so the patterns are permissive). The unversioned id is typed and the source
``*_version`` integer is kept in its own column. See docs/DESIGN.md for the inventory.
"""
from __future__ import annotations

from pathlib import Path

import polars as pl
import yaml

from lacuna_etl.core.identifiers import EnsemblGeneId, EnsemblProteinId, EnsemblTranscriptId
from lacuna_etl.core.pipeline import DatasetPipeline
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.registry import register

_GTF_COLUMNS = ["seqname", "source", "feature_type", "start", "end", "score", "strand", "frame", "attributes"]
_SUB_FEATURE_TYPES = {"exon", "CDS", "five_prime_utr", "three_prime_utr", "start_codon", "stop_codon", "Selenocysteine"}

GENES_SCHEMA = {
    "gene_id": ColumnSpec(identifier=EnsemblGeneId, required=True, description="Unversioned Ensembl gene id; the grain key"),
    "gene_version": ColumnSpec(description="Gene id version (Int64)"),
    "species": ColumnSpec(required=True, description="Source species (the download's species directory, e.g. homo_sapiens)"),
    "seqname": ColumnSpec(required=True, description="Sequence name (chromosome/scaffold)"),
    "start": ColumnSpec(required=True, description="Feature start (1-based, inclusive; Int64)"),
    "end": ColumnSpec(required=True, description="Feature end (Int64)"),
    "strand": ColumnSpec(description="Strand (+ or -)"),
    "gene_source": ColumnSpec(description="Annotation source of the gene (e.g. havana, ensembl)"),
    "gene_biotype": ColumnSpec(description="Gene biotype (e.g. protein_coding, lncRNA)"),
    "gene_name": ColumnSpec(description="Gene name / symbol (nullable)"),
}

TRANSCRIPTS_SCHEMA = {
    "transcript_id": ColumnSpec(identifier=EnsemblTranscriptId, required=True, description="Unversioned Ensembl transcript id; the grain key"),
    "transcript_version": ColumnSpec(description="Transcript id version (Int64)"),
    "gene_id": ColumnSpec(identifier=EnsemblGeneId, required=True, description="Ensembl gene id the transcript belongs to"),
    "species": ColumnSpec(required=True, description="Source species"),
    "seqname": ColumnSpec(required=True, description="Sequence name (chromosome/scaffold)"),
    "start": ColumnSpec(required=True, description="Transcript start (Int64)"),
    "end": ColumnSpec(required=True, description="Transcript end (Int64)"),
    "strand": ColumnSpec(description="Strand (+ or -)"),
    "transcript_source": ColumnSpec(description="Annotation source of the transcript"),
    "transcript_biotype": ColumnSpec(description="Transcript biotype"),
    "transcript_name": ColumnSpec(description="Transcript name (nullable)"),
    "gene_name": ColumnSpec(description="Gene name / symbol (nullable)"),
    "is_canonical": ColumnSpec(required=True, description="Whether the transcript is the Ensembl canonical transcript (tag 'Ensembl_canonical')"),
    "is_mane_select": ColumnSpec(required=True, description="Whether the transcript is MANE Select (tag 'MANE_Select'; human only)"),
}

FEATURES_SCHEMA = {
    "species": ColumnSpec(required=True, description="Source species"),
    "seqname": ColumnSpec(required=True, description="Sequence name (chromosome/scaffold)"),
    "source": ColumnSpec(description="Annotation source (e.g. havana, ensembl)"),
    "feature_type": ColumnSpec(allowed_values=_SUB_FEATURE_TYPES, required=True, description="Sub-feature type: exon, CDS, five_prime_utr, three_prime_utr, start_codon, stop_codon, or Selenocysteine"),
    "start": ColumnSpec(required=True, description="Feature start (Int64)"),
    "end": ColumnSpec(required=True, description="Feature end (Int64)"),
    "score": ColumnSpec(description="Feature score (Float64; Ensembl GTF leaves it '.', so null)"),
    "strand": ColumnSpec(description="Strand (+ or -)"),
    "frame": ColumnSpec(description="Reading frame 0/1/2 for CDS, else null (Int64)"),
    "gene_id": ColumnSpec(identifier=EnsemblGeneId, required=True, description="Ensembl gene id"),
    "transcript_id": ColumnSpec(identifier=EnsemblTranscriptId, description="Ensembl transcript id (nullable)"),
    "exon_id": ColumnSpec(description="Ensembl exon id (for exon features; nullable, plain string)"),
    "exon_number": ColumnSpec(description="Exon ordinal within the transcript (Int64; nullable)"),
    "protein_id": ColumnSpec(identifier=EnsemblProteinId, description="Ensembl protein id (for CDS features; nullable)"),
    "protein_version": ColumnSpec(description="Protein id version (Int64; nullable)"),
}

TABLES_DOC = {"genes": GENES_SCHEMA, "transcripts": TRANSCRIPTS_SCHEMA, "features": FEATURES_SCHEMA}


def _attr(key: str) -> pl.Expr:
    """Polars expr extracting a GTF attribute value: `key "value";` -> value (or null)."""
    return pl.col("attributes").str.extract(rf'{key} "([^"]*)"', 1)


@register
class EnsemblGtf(DatasetPipeline):
    name = "ensembl_gtf"

    def _species_dirs(self) -> list[Path]:
        return sorted(p for p in self.raw_path().iterdir() if p.is_dir())

    def _parse_species(self, gtf: Path, species: str) -> dict[str, pl.DataFrame]:
        df = pl.read_csv(
            gtf, separator="\t", has_header=False, new_columns=_GTF_COLUMNS,
            comment_prefix="#", infer_schema_length=0, quote_char=None,
        ).with_columns(
            pl.lit(species).alias("species"),
            pl.col("start").cast(pl.Int64),
            pl.col("end").cast(pl.Int64),
            pl.when(pl.col("score") == ".").then(None).otherwise(pl.col("score")).cast(pl.Float64).alias("score"),
            pl.when(pl.col("frame") == ".").then(None).otherwise(pl.col("frame")).cast(pl.Int64).alias("frame"),
            _attr("gene_id").alias("gene_id"),
            _attr("gene_version").cast(pl.Int64).alias("gene_version"),
            _attr("gene_name").alias("gene_name"),
            _attr("gene_source").alias("gene_source"),
            _attr("gene_biotype").alias("gene_biotype"),
            _attr("transcript_id").alias("transcript_id"),
            _attr("transcript_version").cast(pl.Int64).alias("transcript_version"),
            _attr("transcript_name").alias("transcript_name"),
            _attr("transcript_source").alias("transcript_source"),
            _attr("transcript_biotype").alias("transcript_biotype"),
            _attr("exon_id").alias("exon_id"),
            _attr("exon_number").cast(pl.Int64).alias("exon_number"),
            _attr("protein_id").alias("protein_id"),
            _attr("protein_version").cast(pl.Int64).alias("protein_version"),
            pl.col("attributes").str.contains('tag "Ensembl_canonical"', literal=True).alias("is_canonical"),
            pl.col("attributes").str.contains('tag "MANE_Select"', literal=True).alias("is_mane_select"),
        )
        return {
            "genes": df.filter(pl.col("feature_type") == "gene").select(list(GENES_SCHEMA)),
            "transcripts": df.filter(pl.col("feature_type") == "transcript").select(list(TRANSCRIPTS_SCHEMA)),
            "features": df.filter(pl.col("feature_type").is_in(list(_SUB_FEATURE_TYPES))).select(list(FEATURES_SCHEMA)),
        }

    def extract(self) -> None:
        done_dir = self.intermediate_path() / "_done"
        done_dir.mkdir(parents=True, exist_ok=True)
        for table in TABLES_DOC:
            (self.intermediate_path() / table).mkdir(parents=True, exist_ok=True)

        species_dirs = self._species_dirs()
        if not species_dirs:
            raise FileNotFoundError(f"ensembl_gtf: no species directories under {self.raw_path()}")
        for sp_dir in species_dirs:
            species = sp_dir.name
            done = done_dir / f"{species}.done"
            if done.exists():  # restartable: skip already-parsed species
                continue
            gtfs = sorted(sp_dir.glob("*.gtf.gz"))
            if len(gtfs) != 1:
                raise FileNotFoundError(f"ensembl_gtf: expected exactly one *.gtf.gz under {sp_dir}, found {len(gtfs)}")
            tables = self._parse_species(gtfs[0], species)
            for table, frame in tables.items():
                frame.write_parquet(self.intermediate_path() / table / f"{species}.parquet", compression="snappy")
            done.touch()
            print(f"[{self.name}] {species}: genes={tables['genes'].height:,}, transcripts={tables['transcripts'].height:,}, features={tables['features'].height:,}")

    def transform(self) -> None:
        # Faithful projection; parsing in extract, validation in load.
        pass

    def load(self) -> None:
        for table, doc in TABLES_DOC.items():
            shards = sorted((self.intermediate_path() / table).glob("*.parquet"))
            out_path = self.output_path() / f"{table}.parquet"
            pl.scan_parquet(shards).sink_parquet(out_path, compression="snappy")
            check_cols = [c for c, spec in doc.items() if spec.identifier is not None or spec.allowed_values is not None or spec.required]
            df = pl.scan_parquet(out_path).select(check_cols).collect()
            for col in check_cols:
                doc[col].validate_polars(df[col])
            (self.output_path() / f"{table}.yml").write_text(
                yaml.dump({c: s.yaml_entry() for c, s in doc.items()}, sort_keys=False, allow_unicode=True)
            )
            print(f"[{self.name}] {table}: {df.height:,} rows")
