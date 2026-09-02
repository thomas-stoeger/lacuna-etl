"""Human Protein Atlas (HPA), the genome-wide map of human protein expression.

The HPA ships its whole knowledge base as one wide zipped TSV
(``proteinatlas.tsv.zip``): ~20.2k rows, one per protein-coding gene, across 119
columns. The file fits comfortably in memory, so this is an in-memory pandas
pipeline: ``extract`` reads the TSV and reshapes it into per-table row lists,
``transform`` is a no-op, and ``load`` validates against each ``SCHEMA`` and writes
final Parquet + sidecar.

The grain key is the Ensembl gene ID (``EnsemblGeneId``; the file is human-only, so
every value is an ``ENSG…``). The parent ``genes`` table keeps the gene-level scalar
metadata; the wide column families are reshaped into tidy child tables keyed by
``ensembl_gene_id``:

- The comma-separated list fields (synonyms, UniProt accessions, protein classes,
  biological processes, molecular functions, disease involvement, subcellular
  locations, antibodies) each become a one-value-per-row child table.
- The expression "specificity / distribution / specificity score" triples that the
  source repeats across 11 RNA contexts (tissue, single cell, cancer, blood, brain,
  …) and 2 protein contexts (cell type, tissue) are unified into one long
  ``expression_specificity`` table with a ``modality`` + ``context`` discriminator,
  rather than ~52 parallel parent columns.
- The matching "specific <unit>" maps (``sample: value;…``, the per-sample elevated
  expression for those contexts) are exploded into ``specific_expression``
  (``modality``/``context``/``sample``/``value``/``unit``).
- The 31 per-cancer prognostics columns become a long ``cancer_prognostics`` table
  (``cancer``/``dataset``/``prognostic_type``/``p_value``).

External identifiers other than the Ensembl key and UniProt accession (antibody IDs,
RRIDs, expression-cluster labels) are heterogeneous and stay documented plain
strings. See docs/DESIGN.md for the full table inventory.
"""
from __future__ import annotations

import re
import zipfile

import pandas as pd

from lacuna_etl.core.identifiers import EnsemblGeneId, UniprotAccession
from lacuna_etl.core.pipeline import DatasetPipeline
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.registry import register

_MODALITIES = {"RNA", "protein"}
_UNITS = {"nTPM", "nCPM", "pTPM", "Intensity"}
_LOCATION_CLASSES = {"main", "additional"}
_DATASETS = {"TCGA", "validation"}

# Each expression context contributes a 4-column block: "<prefix> specificity",
# "<prefix> distribution", "<prefix> specificity score", "<prefix> specific <unit>".
# (modality, context, source_prefix, value_unit)
_CONTEXTS: list[tuple[str, str, str, str]] = [
    ("RNA", "tissue", "RNA tissue", "nTPM"),
    ("RNA", "single_cell_type", "RNA single cell type", "nCPM"),
    ("RNA", "single_cell_type_group", "RNA single cell type group", "nCPM"),
    ("RNA", "single_nuclei_brain", "RNA single nuclei brain", "nCPM"),
    ("RNA", "cancer", "RNA cancer", "pTPM"),
    ("RNA", "brain_regional", "RNA brain regional", "nTPM"),
    ("RNA", "blood_cell", "RNA blood cell", "nTPM"),
    ("RNA", "blood_lineage", "RNA blood lineage", "nTPM"),
    ("RNA", "cell_line", "RNA cell line", "nTPM"),
    ("RNA", "mouse_brain_regional", "RNA mouse brain regional", "nTPM"),
    ("RNA", "pig_brain_regional", "RNA pig brain regional", "nTPM"),
    ("protein", "cell_type", "Protein cell type", "Intensity"),
    ("protein", "tissue", "Protein tissue", "Intensity"),
]

# Gene-level scalar source columns -> snake_case parent columns (verbatim values).
_RENAME = {
    "Gene": "gene_name",
    "Ensembl": "ensembl_gene_id",
    "Gene description": "gene_description",
    "Chromosome": "chromosome",
    "Evidence": "evidence",
    "HPA evidence": "hpa_evidence",
    "UniProt evidence": "uniprot_evidence",
    "NeXtProt evidence": "nextprot_evidence",
    "RNA tissue cell type enrichment": "rna_tissue_cell_type_enrichment",
    "Secretome location": "secretome_location",
    "Secretome function": "secretome_function",
    "Blood expression cluster": "blood_expression_cluster",
    "Tissue expression cluster": "tissue_expression_cluster",
    "Brain expression cluster": "brain_expression_cluster",
    "Cell line expression cluster": "cell_line_expression_cluster",
    "Single cell expression cluster": "single_cell_expression_cluster",
    "Reliability (IH)": "reliability_ih",
    "Reliability (Mouse Brain)": "reliability_mouse_brain",
    "Reliability (IF)": "reliability_if",
}

# --------------------------------------------------------------------------
# Parent: one row per gene.
# --------------------------------------------------------------------------
GENES_SCHEMA = {
    "ensembl_gene_id": ColumnSpec(identifier=EnsemblGeneId, required=True, description="Ensembl gene ID (human, ENSG…); the grain key"),
    "gene_name": ColumnSpec(required=True, description="HGNC-approved gene symbol"),
    "gene_description": ColumnSpec(description="Gene description"),
    "chromosome": ColumnSpec(description="Chromosome (1–22, X, Y, MT, …)"),
    "position_start": ColumnSpec(description="Genomic start position (from the 'Position' start-end range)"),
    "position_end": ColumnSpec(description="Genomic end position (from the 'Position' start-end range)"),
    "evidence": ColumnSpec(description="Overall protein existence evidence level"),
    "hpa_evidence": ColumnSpec(description="HPA evidence level"),
    "uniprot_evidence": ColumnSpec(description="UniProt evidence level"),
    "nextprot_evidence": ColumnSpec(description="neXtProt evidence level"),
    "rna_tissue_cell_type_enrichment": ColumnSpec(description="Tissue cell-type RNA enrichment, raw 'tissue - cell type' comma list (kept verbatim)"),
    "secretome_location": ColumnSpec(description="Secretome location classification"),
    "secretome_function": ColumnSpec(description="Secretome function classification"),
    "ccd_protein": ColumnSpec(description="Cell-cycle-dependent at the protein level (boolean; source 'NA' → null)"),
    "ccd_transcript": ColumnSpec(description="Cell-cycle-dependent at the transcript level (boolean; source 'NA' → null)"),
    "blood_concentration_im_pg_per_l": ColumnSpec(description="Blood concentration by immunoassay, pg/L (Int64)"),
    "blood_concentration_ms_pg_per_l": ColumnSpec(description="Blood concentration by mass spectrometry, pg/L (Int64)"),
    "blood_expression_cluster": ColumnSpec(description="Blood RNA expression cluster ('Cluster N: description')"),
    "tissue_expression_cluster": ColumnSpec(description="Tissue RNA expression cluster ('Cluster N: description')"),
    "brain_expression_cluster": ColumnSpec(description="Brain RNA expression cluster ('Cluster N: description')"),
    "cell_line_expression_cluster": ColumnSpec(description="Cell-line RNA expression cluster ('Cluster N: description')"),
    "single_cell_expression_cluster": ColumnSpec(description="Single-cell RNA expression cluster ('Cluster N: description')"),
    "n_interactions": ColumnSpec(description="Number of protein-protein interactions reported (Int64)"),
    "reliability_ih": ColumnSpec(description="Immunohistochemistry reliability score"),
    "reliability_mouse_brain": ColumnSpec(description="Mouse-brain immunohistochemistry reliability score"),
    "reliability_if": ColumnSpec(description="Immunofluorescence reliability score"),
}

# --------------------------------------------------------------------------
# Child tables keyed by ensembl_gene_id.
# --------------------------------------------------------------------------
GENE_SYNONYMS_SCHEMA = {
    "ensembl_gene_id": ColumnSpec(identifier=EnsemblGeneId, required=True, description="Ensembl gene ID"),
    "synonym": ColumnSpec(required=True, description="A gene name synonym"),
}

GENE_UNIPROT_SCHEMA = {
    "ensembl_gene_id": ColumnSpec(identifier=EnsemblGeneId, required=True, description="Ensembl gene ID"),
    "uniprot_accession": ColumnSpec(identifier=UniprotAccession, required=True, description="A mapped UniProtKB accession"),
}

GENE_PROTEIN_CLASSES_SCHEMA = {
    "ensembl_gene_id": ColumnSpec(identifier=EnsemblGeneId, required=True, description="Ensembl gene ID"),
    "protein_class": ColumnSpec(required=True, description="An HPA protein class the gene belongs to"),
}

GENE_BIOLOGICAL_PROCESSES_SCHEMA = {
    "ensembl_gene_id": ColumnSpec(identifier=EnsemblGeneId, required=True, description="Ensembl gene ID"),
    "biological_process": ColumnSpec(required=True, description="An associated biological process (UniProt keyword)"),
}

GENE_MOLECULAR_FUNCTIONS_SCHEMA = {
    "ensembl_gene_id": ColumnSpec(identifier=EnsemblGeneId, required=True, description="Ensembl gene ID"),
    "molecular_function": ColumnSpec(required=True, description="An associated molecular function (UniProt keyword)"),
}

GENE_DISEASE_INVOLVEMENT_SCHEMA = {
    "ensembl_gene_id": ColumnSpec(identifier=EnsemblGeneId, required=True, description="Ensembl gene ID"),
    "disease": ColumnSpec(required=True, description="A disease the gene is involved in (UniProt disease keyword)"),
}

GENE_SUBCELLULAR_LOCATIONS_SCHEMA = {
    "ensembl_gene_id": ColumnSpec(identifier=EnsemblGeneId, required=True, description="Ensembl gene ID"),
    "location": ColumnSpec(required=True, description="An immunofluorescence subcellular location"),
    "location_class": ColumnSpec(allowed_values=_LOCATION_CLASSES, required=True, description="Whether this is a 'main' or 'additional' location"),
}

GENE_ANTIBODIES_SCHEMA = {
    "ensembl_gene_id": ColumnSpec(identifier=EnsemblGeneId, required=True, description="Ensembl gene ID"),
    "antibody": ColumnSpec(required=True, description="An HPA antibody ID used for the gene (e.g. HPA004109, CAB…)"),
    "rrid": ColumnSpec(description="The antibody's RRID (Research Resource Identifier), when assigned"),
}

EXPRESSION_SPECIFICITY_SCHEMA = {
    "ensembl_gene_id": ColumnSpec(identifier=EnsemblGeneId, required=True, description="Ensembl gene ID"),
    "modality": ColumnSpec(allowed_values=_MODALITIES, required=True, description="Measurement modality: RNA or protein"),
    "context": ColumnSpec(required=True, description="Expression context, e.g. tissue, single_cell_type, cancer, blood_cell, brain_regional, cell_type"),
    "specificity": ColumnSpec(description="Specificity category in this context (e.g. 'Tissue enhanced', 'Low tissue specificity')"),
    "distribution": ColumnSpec(description="Detection distribution (e.g. 'Detected in all', 'Detected in some')"),
    "specificity_score": ColumnSpec(description="Numeric specificity score in this context (Int64)"),
}

SPECIFIC_EXPRESSION_SCHEMA = {
    "ensembl_gene_id": ColumnSpec(identifier=EnsemblGeneId, required=True, description="Ensembl gene ID"),
    "modality": ColumnSpec(allowed_values=_MODALITIES, required=True, description="Measurement modality: RNA or protein"),
    "context": ColumnSpec(required=True, description="Expression context (same vocabulary as expression_specificity.context)"),
    "sample": ColumnSpec(required=True, description="The tissue / cell type / cancer / cell line the value belongs to"),
    "value": ColumnSpec(description="Elevated-expression value for the sample (Float64)"),
    "unit": ColumnSpec(allowed_values=_UNITS, required=True, description="Value unit: nTPM, nCPM, pTPM (RNA) or Intensity (protein)"),
}

CANCER_PROGNOSTICS_SCHEMA = {
    "ensembl_gene_id": ColumnSpec(identifier=EnsemblGeneId, required=True, description="Ensembl gene ID"),
    "cancer": ColumnSpec(required=True, description="Cancer type, from the source column header"),
    "dataset": ColumnSpec(allowed_values=_DATASETS, required=True, description="Source cohort: TCGA or validation"),
    "prognostic_type": ColumnSpec(required=True, description="Prognostic call, e.g. 'unprognostic', 'validated prognostic favorable', 'potential prognostic unfavorable'"),
    "p_value": ColumnSpec(description="Reported p-value for the prognostic association (Float64)"),
}

# value parsing helpers -------------------------------------------------------
_PROGNOSTIC_RE = re.compile(r"^(?P<label>.*?)\s*\((?P<pval>[^)]*)\)\s*$")
_POSITION_RE = re.compile(r"^(\d+)-(\d+)$")


def _clean(v: str | None) -> str | None:
    """Empty / whitespace-only string -> None."""
    if v is None:
        return None
    v = v.strip()
    return v or None


def _split(v: str | None, sep: str = ", ") -> list[str]:
    """Split a delimited list field, dropping empty fragments."""
    if not v or not v.strip():
        return []
    return [p.strip() for p in v.split(sep) if p.strip()]


def _to_int(v: str | None) -> int | None:
    v = _clean(v)
    return int(v) if v is not None and re.fullmatch(r"-?\d+", v) else None


def _to_float(v: str | None) -> float | None:
    v = _clean(v)
    if v is None:
        return None
    try:
        return float(v)
    except ValueError:
        return None


def _yes_no(v: str | None) -> bool | None:
    """HPA 'Yes'/'No'/'NA' (and empty) -> True/False/None."""
    v = _clean(v)
    if v == "Yes":
        return True
    if v == "No":
        return False
    return None


def _parse_map(v: str | None) -> list[tuple[str, float | None]]:
    """Parse a 'sample: value;sample: value' map into (sample, value) pairs.

    Sample names never contain ';'; the value is the last ': '-separated token, so
    a sample name that itself contains ': ' is preserved."""
    out = []
    for item in (v or "").split(";"):
        item = item.strip()
        if not item:
            continue
        sample, _, val = item.rpartition(": ")
        out.append((sample.strip() or item, _to_float(val)))
    return out


@register
class ProteinAtlas(DatasetPipeline):
    name = "proteinatlas"

    _TABLES = [
        ("genes", GENES_SCHEMA),
        ("gene_synonyms", GENE_SYNONYMS_SCHEMA),
        ("gene_uniprot", GENE_UNIPROT_SCHEMA),
        ("gene_protein_classes", GENE_PROTEIN_CLASSES_SCHEMA),
        ("gene_biological_processes", GENE_BIOLOGICAL_PROCESSES_SCHEMA),
        ("gene_molecular_functions", GENE_MOLECULAR_FUNCTIONS_SCHEMA),
        ("gene_disease_involvement", GENE_DISEASE_INVOLVEMENT_SCHEMA),
        ("gene_subcellular_locations", GENE_SUBCELLULAR_LOCATIONS_SCHEMA),
        ("gene_antibodies", GENE_ANTIBODIES_SCHEMA),
        ("expression_specificity", EXPRESSION_SPECIFICITY_SCHEMA),
        ("specific_expression", SPECIFIC_EXPRESSION_SCHEMA),
        ("cancer_prognostics", CANCER_PROGNOSTICS_SCHEMA),
    ]

    def _read_tsv(self) -> pd.DataFrame:
        matches = sorted(self.raw_path().glob("*.tsv.zip"))
        if len(matches) != 1:
            raise FileNotFoundError(f"expected exactly one *.tsv.zip under {self.raw_path()}, found {len(matches)}")
        with zipfile.ZipFile(matches[0]) as zf:
            inner = [n for n in zf.namelist() if n.endswith(".tsv")]
            if len(inner) != 1:
                raise FileNotFoundError(f"expected one .tsv inside {matches[0].name}, found {inner}")
            with zf.open(inner[0]) as fh:
                # keep_default_na=False so the literal 'NA' (used by the CCD columns)
                # is not coerced to a missing value; we handle nulls per column.
                return pd.read_csv(fh, sep="\t", dtype=str, keep_default_na=False, na_filter=False)

    def extract(self) -> None:
        df = self._read_tsv()
        # Column families derived once from the cancer-prognostics headers.
        prog_cols = [c for c in df.columns if c.startswith("Cancer prognostics - ")]
        prog_meta = {}
        for c in prog_cols:
            body = c[len("Cancer prognostics - "):]
            m = re.match(r"^(?P<cancer>.*) \((?P<dataset>TCGA|validation)\)$", body)
            if not m:
                raise ValueError(f"unparseable cancer prognostics header: {c!r}")
            prog_meta[c] = (m["cancer"], m["dataset"])

        out: dict[str, list[dict]] = {stem: [] for stem, _ in self._TABLES}

        for rec in df.to_dict("records"):
            gid = _clean(rec["Ensembl"])
            start = end = None
            pos = _POSITION_RE.match(_clean(rec.get("Position")) or "")
            if pos:
                start, end = int(pos.group(1)), int(pos.group(2))
            out["genes"].append({
                **{col: _clean(rec.get(src)) for src, col in _RENAME.items()},
                "position_start": start,
                "position_end": end,
                "ccd_protein": _yes_no(rec.get("CCD Protein")),
                "ccd_transcript": _yes_no(rec.get("CCD Transcript")),
                "blood_concentration_im_pg_per_l": _to_int(rec.get("Blood concentration - Conc. blood IM [pg/L]")),
                "blood_concentration_ms_pg_per_l": _to_int(rec.get("Blood concentration - Conc. blood MS [pg/L]")),
                "n_interactions": _to_int(rec.get("Interactions")),
            })

            for val in _split(rec.get("Gene synonym")):
                out["gene_synonyms"].append({"ensembl_gene_id": gid, "synonym": val})
            for val in _split(rec.get("Uniprot")):
                out["gene_uniprot"].append({"ensembl_gene_id": gid, "uniprot_accession": val})
            for val in _split(rec.get("Protein class")):
                out["gene_protein_classes"].append({"ensembl_gene_id": gid, "protein_class": val})
            for val in _split(rec.get("Biological process")):
                out["gene_biological_processes"].append({"ensembl_gene_id": gid, "biological_process": val})
            for val in _split(rec.get("Molecular function")):
                out["gene_molecular_functions"].append({"ensembl_gene_id": gid, "molecular_function": val})
            for val in _split(rec.get("Disease involvement")):
                out["gene_disease_involvement"].append({"ensembl_gene_id": gid, "disease": val})
            for cls, src in (("main", "Subcellular main location"), ("additional", "Subcellular additional location")):
                for val in _split(rec.get(src)):
                    out["gene_subcellular_locations"].append({"ensembl_gene_id": gid, "location": val, "location_class": cls})
            # "Antibody RRID" carries 'ANTIBODY: RRID' pairs; fall back to the bare
            # antibody list when it is absent.
            rrid_field = _split(rec.get("Antibody RRID"))
            if rrid_field:
                for pair in rrid_field:
                    ab, _, rr = pair.partition(":")  # 'ANTIBODY: RRID' (RRID often blank in a release)
                    out["gene_antibodies"].append({"ensembl_gene_id": gid, "antibody": ab.strip(), "rrid": _clean(rr)})
            else:
                for ab in _split(rec.get("Antibody")):
                    out["gene_antibodies"].append({"ensembl_gene_id": gid, "antibody": ab, "rrid": None})

            for modality, context, prefix, unit in _CONTEXTS:
                spec = _clean(rec.get(f"{prefix} specificity"))
                dist = _clean(rec.get(f"{prefix} distribution"))
                score = _to_int(rec.get(f"{prefix} specificity score"))
                if spec is not None or dist is not None or score is not None:
                    out["expression_specificity"].append({
                        "ensembl_gene_id": gid, "modality": modality, "context": context,
                        "specificity": spec, "distribution": dist, "specificity_score": score,
                    })
                for sample, value in _parse_map(rec.get(f"{prefix} specific {unit}")):
                    out["specific_expression"].append({
                        "ensembl_gene_id": gid, "modality": modality, "context": context,
                        "sample": sample, "value": value, "unit": unit,
                    })

            for c, (cancer, dataset) in prog_meta.items():
                v = _clean(rec.get(c))
                if v is None:
                    continue
                m = _PROGNOSTIC_RE.match(v)
                if not m:
                    raise ValueError(f"{gid}: unparseable prognostics value in {c!r}: {v!r}")
                out["cancer_prognostics"].append({
                    "ensembl_gene_id": gid, "cancer": cancer, "dataset": dataset,
                    "prognostic_type": m["label"], "p_value": _to_float(m["pval"]),
                })

        for stem, schema in self._TABLES:
            self.save_parquet(pd.DataFrame(out[stem], columns=list(schema)), self.intermediate_path() / f"{stem}.parquet")
        print(
            f"[{self.name}] genes: {len(out['genes']):,}, "
            f"expression_specificity: {len(out['expression_specificity']):,}, "
            f"specific_expression: {len(out['specific_expression']):,}, "
            f"cancer_prognostics: {len(out['cancer_prognostics']):,}, "
            f"antibodies: {len(out['gene_antibodies']):,}"
        )

    def transform(self) -> None:
        # Faithful projection; parsing happens in extract, typing/validation in load.
        pass

    def load(self) -> None:
        int_cols = {
            "genes": ["position_start", "position_end", "blood_concentration_im_pg_per_l", "blood_concentration_ms_pg_per_l", "n_interactions"],
            "expression_specificity": ["specificity_score"],
        }
        bool_cols = {"genes": ["ccd_protein", "ccd_transcript"]}
        float_cols = {"specific_expression": ["value"], "cancer_prognostics": ["p_value"]}
        for stem, schema in self._TABLES:
            df = self.load_parquet(self.intermediate_path() / f"{stem}.parquet")
            df = df[list(schema)]
            for col in int_cols.get(stem, []):
                df[col] = df[col].astype("Int64")
            for col in float_cols.get(stem, []):
                df[col] = df[col].astype("Float64")
            for col in bool_cols.get(stem, []):
                df[col] = df[col].astype("boolean")
            df = self.apply_schema(df, schema)
            self.save_parquet(df, self.output_path() / f"{stem}.parquet")
            self.save_schema_yaml(schema, stem)
