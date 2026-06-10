"""Open Targets targets (genes), fully exploded.

Parent:
  targets                       - one row per Ensembl gene (scalar + scalar-list columns)
Per-target child tables:
  targets_transcripts           - transcripts of the gene
  targets_go                    - Gene Ontology annotations
  targets_synonyms              - symbol/name synonyms (current and obsolete), tagged by type
  targets_subcellular_locations - subcellular localisations
  targets_classes               - ChEMBL target-class assignments
  targets_constraints           - gnomAD constraint metrics
  targets_protein_ids           - protein cross-identifiers
  targets_db_xrefs              - database cross-references
  targets_pathways              - Reactome pathway memberships
  targets_tractability          - tractability assessments by modality
  targets_homologues            - cross-species homologues
  targets_chemical_probes       - chemical probes for the target
  targets_chemical_probe_urls   - probe reference URLs (grandchild of chemical probes)
  targets_safety_liabilities    - known safety liabilities
  targets_safety_effects        - effects of each liability (grandchild)
  targets_safety_biosamples     - biosamples of each liability (grandchild)
  targets_safety_studies        - studies behind each liability (grandchild)
  targets_hallmark_attributes   - cancer-hallmark attribute annotations
  targets_cancer_hallmarks      - cancer-hallmark promote/suppress annotations
"""

import polars as pl

from lacuna_etl.core.identifiers import (
    ChemblId, EnsemblGeneId, EnsemblTranscriptId, GoId, PubmedId, ReactomePathwayId,
)
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.opentargets._base import OpenTargetsProductPipeline
from lacuna_etl.datasets.opentargets._helpers import concat_tagged, explode_struct_list
from lacuna_etl.datasets.registry import register

_KEY = {"id": "target_id"}

_SYNONYM_TYPES = [
    ("synonyms", "synonym"),
    ("symbolSynonyms", "symbol"),
    ("nameSynonyms", "name"),
    ("obsoleteSymbols", "obsolete_symbol"),
    ("obsoleteNames", "obsolete_name"),
]


def _parent(df: pl.DataFrame) -> pl.DataFrame:
    return df.select(
        pl.col("id").alias("target_id"),
        pl.col("approvedSymbol").alias("approved_symbol"),
        pl.col("approvedName").alias("approved_name"),
        pl.col("biotype").alias("biotype"),
        pl.col("tss").cast(pl.Int64).alias("tss"),
        pl.col("genomicLocation").struct.field("chromosome").alias("chromosome"),
        pl.col("genomicLocation").struct.field("start").cast(pl.Int64).alias("genomic_start"),
        pl.col("genomicLocation").struct.field("end").cast(pl.Int64).alias("genomic_end"),
        pl.col("genomicLocation").struct.field("strand").cast(pl.Int64).alias("genomic_strand"),
        pl.col("canonicalTranscript").struct.field("id").alias("canonical_transcript_id"),
        pl.col("canonicalTranscript").struct.field("chromosome").alias("canonical_transcript_chromosome"),
        pl.col("canonicalTranscript").struct.field("start").cast(pl.Int64).alias("canonical_transcript_start"),
        pl.col("canonicalTranscript").struct.field("end").cast(pl.Int64).alias("canonical_transcript_end"),
        pl.col("canonicalTranscript").struct.field("strand").alias("canonical_transcript_strand"),
        pl.col("tep").struct.field("targetFromSourceId").alias("tep_target_from_source_id"),
        pl.col("tep").struct.field("description").alias("tep_description"),
        pl.col("tep").struct.field("therapeuticArea").alias("tep_therapeutic_area"),
        pl.col("tep").struct.field("url").alias("tep_url"),
        pl.col("transcriptIds").alias("transcript_ids"),
        pl.col("canonicalExons").alias("canonical_exons"),
        pl.col("alternativeGenes").alias("alternative_genes"),
        pl.col("functionDescriptions").alias("function_descriptions"),
    )


def _synonyms(df: pl.DataFrame) -> pl.DataFrame:
    parts = []
    for col, tag in _SYNONYM_TYPES:
        part = explode_struct_list(df, _KEY, col, {"label": "label", "source": "source"})
        if not part.is_empty():
            parts.append(part.with_columns(pl.lit(tag).alias("synonym_type")))
    return concat_tagged(parts)


def _chemical_probes(df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    if "chemicalProbes" not in df.columns:
        return pl.DataFrame(), pl.DataFrame()
    raw = (
        df.select(pl.col("id").alias("target_id"), pl.col("chemicalProbes"))
        .explode("chemicalProbes")
        .filter(pl.col("chemicalProbes").is_not_null())
        .unnest("chemicalProbes")
    )
    probes = raw.select(
        pl.col("target_id"),
        pl.col("id").alias("probe_id"),
        pl.col("targetFromSourceId").alias("target_from_source_id"),
        pl.col("drugFromSourceId").alias("drug_from_source_id"),
        pl.col("drugId").alias("drug_id"),
        pl.col("mechanismOfAction").alias("mechanism_of_action"),
        pl.col("origin").alias("origin"),
        pl.col("control").alias("control"),
        pl.col("isHighQuality").alias("is_high_quality"),
        pl.col("probesDrugsScore").alias("probes_drugs_score"),
        pl.col("probeMinerScore").alias("probe_miner_score"),
        pl.col("scoreInCells").alias("score_in_cells"),
        pl.col("scoreInOrganisms").alias("score_in_organisms"),
    )
    urls = pl.DataFrame()
    if "urls" in raw.columns:
        urls = (
            raw.select(pl.col("target_id"), pl.col("id").alias("probe_id"), pl.col("urls"))
            .explode("urls")
            .filter(pl.col("urls").is_not_null())
            .unnest("urls")
            .select(
                pl.col("target_id"),
                pl.col("probe_id"),
                pl.col("niceName").alias("nice_name"),
                pl.col("url"),
            )
        )
    return probes, urls


def _safety(df: pl.DataFrame) -> dict[str, pl.DataFrame]:
    if "safetyLiabilities" not in df.columns:
        return {}
    raw = (
        df.select(pl.col("id").alias("target_id"), pl.col("safetyLiabilities"))
        .with_columns(pl.int_ranges(pl.col("safetyLiabilities").list.len()).alias("liability_index"))
        .explode(["safetyLiabilities", "liability_index"])
        .filter(pl.col("safetyLiabilities").is_not_null())
        .unnest("safetyLiabilities")
    )
    liabilities = raw.select(
        pl.col("target_id"),
        pl.col("liability_index"),
        pl.col("event"),
        pl.col("eventId").alias("event_id"),
        pl.col("datasource"),
        pl.col("literature"),
        pl.col("url"),
    )

    def _grandchild(list_col: str, fields: dict[str, str]) -> pl.DataFrame:
        if list_col not in raw.columns:
            return pl.DataFrame()
        sub = (
            raw.select(pl.col("target_id"), pl.col("liability_index"), pl.col(list_col))
            .explode(list_col)
            .filter(pl.col(list_col).is_not_null())
            .unnest(list_col)
        )
        return sub.select(
            [pl.col("target_id"), pl.col("liability_index")]
            + [pl.col(s).alias(o) for s, o in fields.items()]
        )

    return {
        "targets_safety_liabilities": liabilities,
        "targets_safety_effects": _grandchild("effects", {"direction": "direction", "dosing": "dosing"}),
        "targets_safety_biosamples": _grandchild("biosamples", {
            "tissueLabel": "tissue_label", "tissueId": "tissue_id",
            "cellLabel": "cell_label", "cellFormat": "cell_format", "cellId": "cell_id",
        }),
        "targets_safety_studies": _grandchild("studies", {
            "description": "description", "name": "name", "type": "type",
        }),
    }


def _hallmarks(df: pl.DataFrame) -> dict[str, pl.DataFrame]:
    attrs = pl.DataFrame()
    cancer = pl.DataFrame()
    if "hallmarks" in df.columns and isinstance(df.schema["hallmarks"], pl.Struct):
        names = {f.name for f in df.schema["hallmarks"].fields}
        if "attributes" in names:
            attrs = (
                df.select(pl.col("id").alias("target_id"),
                          pl.col("hallmarks").struct.field("attributes").alias("attributes"))
                .explode("attributes")
                .filter(pl.col("attributes").is_not_null())
                .unnest("attributes")
                .select(
                    pl.col("target_id"),
                    pl.col("pmid").cast(pl.Int64).alias("pmid"),
                    pl.col("description"),
                    pl.col("name"),
                )
            )
        if "cancerHallmarks" in names:
            cancer = (
                df.select(pl.col("id").alias("target_id"),
                          pl.col("hallmarks").struct.field("cancerHallmarks").alias("cancerHallmarks"))
                .explode("cancerHallmarks")
                .filter(pl.col("cancerHallmarks").is_not_null())
                .unnest("cancerHallmarks")
                .select(
                    pl.col("target_id"),
                    pl.col("pmid").cast(pl.Int64).alias("pmid"),
                    pl.col("description"),
                    pl.col("impact"),
                    pl.col("label"),
                )
            )
    return {"targets_hallmark_attributes": attrs, "targets_cancer_hallmarks": cancer}


def transform_file(df: pl.DataFrame) -> dict[str, pl.DataFrame]:
    tables: dict[str, pl.DataFrame] = {
        "targets": _parent(df),
        "targets_transcripts": explode_struct_list(df, _KEY, "transcripts", {
            "transcriptId": "transcript_id", "biotype": "biotype", "uniprotId": "uniprot_id",
            "isUniprotReviewed": "is_uniprot_reviewed", "translationId": "translation_id",
            "alphafoldId": "alphafold_id", "uniprotIsoformId": "uniprot_isoform_id",
            "isEnsemblCanonical": "is_ensembl_canonical",
        }),
        "targets_go": explode_struct_list(df, _KEY, "go", {
            "id": "go_id", "source": "source", "evidence": "evidence",
            "aspect": "aspect", "geneProduct": "gene_product", "ecoId": "eco_id",
        }),
        "targets_synonyms": _synonyms(df),
        "targets_subcellular_locations": explode_struct_list(df, _KEY, "subcellularLocations", {
            "location": "location", "source": "source", "termSL": "term_sl", "labelSL": "label_sl",
        }),
        "targets_classes": explode_struct_list(df, _KEY, "targetClass", {
            "id": "class_id", "label": "label", "level": "level",
        }, casts={"class_id": pl.Int64}),
        "targets_constraints": explode_struct_list(df, _KEY, "constraint", {
            "constraintType": "constraint_type", "score": "score", "exp": "exp", "obs": "obs",
            "oe": "oe", "oeLower": "oe_lower", "oeUpper": "oe_upper", "upperRank": "upper_rank",
            "upperBin": "upper_bin", "upperBin6": "upper_bin6",
        }, casts={"obs": pl.Int64, "upper_rank": pl.Int64, "upper_bin": pl.Int64, "upper_bin6": pl.Int64}),
        "targets_protein_ids": explode_struct_list(df, _KEY, "proteinIds", {
            "id": "protein_id", "source": "source",
        }),
        "targets_db_xrefs": explode_struct_list(df, _KEY, "dbXrefs", {
            "id": "xref_id", "source": "source",
        }),
        "targets_pathways": explode_struct_list(df, _KEY, "pathways", {
            "pathwayId": "pathway_id", "pathway": "pathway", "topLevelTerm": "top_level_term",
        }),
        "targets_tractability": explode_struct_list(df, _KEY, "tractability", {
            "modality": "modality", "id": "modality_id", "value": "value",
        }),
        "targets_homologues": explode_struct_list(df, _KEY, "homologues", {
            "speciesId": "species_id", "speciesName": "species_name", "homologyType": "homology_type",
            "targetGeneId": "target_gene_id", "isHighConfidence": "is_high_confidence",
            "targetGeneSymbol": "target_gene_symbol", "queryPercentageIdentity": "query_percentage_identity",
            "targetPercentageIdentity": "target_percentage_identity", "priority": "priority",
        }, casts={"priority": pl.Int64}),
    }

    probes, probe_urls = _chemical_probes(df)
    tables["targets_chemical_probes"] = probes
    tables["targets_chemical_probe_urls"] = probe_urls
    tables.update(_safety(df))
    tables.update(_hallmarks(df))
    return tables


# --- schema / sidecar documentation ----------------------------------------

_TARGETS_DOC = {
    "target_id":                       ColumnSpec(identifier=EnsemblGeneId, required=True, description="Ensembl gene ID"),
    "approved_symbol":                 ColumnSpec(description="Approved gene symbol"),
    "approved_name":                   ColumnSpec(description="Approved gene name"),
    "biotype":                         ColumnSpec(description="Ensembl biotype (protein_coding, lncRNA, ...)"),
    "tss":                             ColumnSpec(description="Transcription start site coordinate"),
    "chromosome":                      ColumnSpec(description="Chromosome of the gene"),
    "genomic_start":                   ColumnSpec(description="Gene start coordinate"),
    "genomic_end":                     ColumnSpec(description="Gene end coordinate"),
    "genomic_strand":                  ColumnSpec(description="Strand (+1 / -1)"),
    "canonical_transcript_id":         ColumnSpec(description="Ensembl ID of the canonical transcript"),
    "canonical_transcript_chromosome": ColumnSpec(description="Canonical transcript chromosome"),
    "canonical_transcript_start":      ColumnSpec(description="Canonical transcript start coordinate"),
    "canonical_transcript_end":        ColumnSpec(description="Canonical transcript end coordinate"),
    "canonical_transcript_strand":     ColumnSpec(description="Canonical transcript strand"),
    "tep_target_from_source_id":       ColumnSpec(description="Target Enabling Package source target ID"),
    "tep_description":                 ColumnSpec(description="Target Enabling Package description"),
    "tep_therapeutic_area":            ColumnSpec(description="Target Enabling Package therapeutic area"),
    "tep_url":                         ColumnSpec(description="Target Enabling Package URL"),
    "transcript_ids":                  ColumnSpec(description="Ensembl transcript IDs (list)"),
    "canonical_exons":                 ColumnSpec(description="Canonical exon boundary coordinates (list)"),
    "alternative_genes":               ColumnSpec(description="Alternative/overlapping Ensembl gene IDs (list)"),
    "function_descriptions":           ColumnSpec(description="Free-text function descriptions (list)"),
}

_TRANSCRIPTS_DOC = {
    "target_id":           ColumnSpec(identifier=EnsemblGeneId, required=True, description="Ensembl gene ID"),
    "transcript_id":       ColumnSpec(identifier=EnsemblTranscriptId, description="Ensembl transcript ID"),
    "biotype":             ColumnSpec(description="Transcript biotype"),
    "uniprot_id":          ColumnSpec(description="UniProt accession for the translated protein"),
    "is_uniprot_reviewed": ColumnSpec(description="Whether the UniProt entry is reviewed (Swiss-Prot)"),
    "translation_id":      ColumnSpec(description="Ensembl translation (protein) ID"),
    "alphafold_id":        ColumnSpec(description="AlphaFold structure ID"),
    "uniprot_isoform_id":  ColumnSpec(description="UniProt isoform ID"),
    "is_ensembl_canonical": ColumnSpec(description="Whether this is the Ensembl canonical transcript"),
}

_GO_DOC = {
    "target_id":   ColumnSpec(identifier=EnsemblGeneId, required=True, description="Ensembl gene ID"),
    "go_id":       ColumnSpec(identifier=GoId, description="Gene Ontology term ID"),
    "source":      ColumnSpec(description="Annotation source"),
    "evidence":    ColumnSpec(description="GO evidence code"),
    "aspect":      ColumnSpec(description="GO aspect (F=molecular function, P=biological process, C=cellular component)"),
    "gene_product": ColumnSpec(description="Annotated gene product"),
    "eco_id":      ColumnSpec(description="Evidence & Conclusion Ontology ID"),
}

_SYNONYMS_DOC = {
    "target_id":    ColumnSpec(identifier=EnsemblGeneId, required=True, description="Ensembl gene ID"),
    "label":        ColumnSpec(description="Synonym string"),
    "source":       ColumnSpec(description="Synonym source"),
    "synonym_type": ColumnSpec(description="Type: synonym, symbol, name, obsolete_symbol, or obsolete_name"),
}

_SUBCELLULAR_DOC = {
    "target_id": ColumnSpec(identifier=EnsemblGeneId, required=True, description="Ensembl gene ID"),
    "location":  ColumnSpec(description="Subcellular location label"),
    "source":    ColumnSpec(description="Annotation source"),
    "term_sl":   ColumnSpec(description="Subcellular-location ontology term ID"),
    "label_sl":  ColumnSpec(description="Subcellular-location ontology label"),
}

_CLASSES_DOC = {
    "target_id": ColumnSpec(identifier=EnsemblGeneId, required=True, description="Ensembl gene ID"),
    "class_id":  ColumnSpec(description="ChEMBL target-class ID"),
    "label":     ColumnSpec(description="Target-class label"),
    "level":     ColumnSpec(description="Target-class hierarchy level"),
}

_CONSTRAINTS_DOC = {
    "target_id":       ColumnSpec(identifier=EnsemblGeneId, required=True, description="Ensembl gene ID"),
    "constraint_type": ColumnSpec(description="Constraint metric type (syn, mis, lof)"),
    "score":           ColumnSpec(description="Constraint score (e.g. LOEUF/pLI-related)"),
    "exp":             ColumnSpec(description="Expected variant count"),
    "obs":             ColumnSpec(description="Observed variant count"),
    "oe":              ColumnSpec(description="Observed/expected ratio"),
    "oe_lower":        ColumnSpec(description="Lower bound of the observed/expected ratio"),
    "oe_upper":        ColumnSpec(description="Upper bound of the observed/expected ratio"),
    "upper_rank":      ColumnSpec(description="Rank of the upper bound across genes"),
    "upper_bin":       ColumnSpec(description="Decile bin of the upper bound"),
    "upper_bin6":      ColumnSpec(description="Sextile bin of the upper bound"),
}

_PROTEIN_IDS_DOC = {
    "target_id":  ColumnSpec(identifier=EnsemblGeneId, required=True, description="Ensembl gene ID"),
    "protein_id": ColumnSpec(description="Protein identifier in the source database"),
    "source":     ColumnSpec(description="Source database (uniprot_swissprot, uniprot_trembl, ...)"),
}

_DB_XREFS_DOC = {
    "target_id": ColumnSpec(identifier=EnsemblGeneId, required=True, description="Ensembl gene ID"),
    "xref_id":   ColumnSpec(description="Identifier in the cross-referenced database"),
    "source":    ColumnSpec(description="Cross-referenced database"),
}

_PATHWAYS_DOC = {
    "target_id":      ColumnSpec(identifier=EnsemblGeneId, required=True, description="Ensembl gene ID"),
    "pathway_id":     ColumnSpec(identifier=ReactomePathwayId, description="Reactome pathway ID"),
    "pathway":        ColumnSpec(description="Pathway name"),
    "top_level_term": ColumnSpec(description="Top-level Reactome term"),
}

_TRACTABILITY_DOC = {
    "target_id":   ColumnSpec(identifier=EnsemblGeneId, required=True, description="Ensembl gene ID"),
    "modality":    ColumnSpec(description="Modality (SM=small molecule, AB=antibody, PR=PROTAC, OC=other clinical)"),
    "modality_id": ColumnSpec(description="Tractability bucket ID within the modality"),
    "value":       ColumnSpec(description="Whether the tractability bucket applies"),
}

_HOMOLOGUES_DOC = {
    "target_id":                  ColumnSpec(identifier=EnsemblGeneId, required=True, description="Ensembl gene ID of the query (human) gene"),
    "species_id":                 ColumnSpec(description="NCBI taxon ID of the homologue species"),
    "species_name":               ColumnSpec(description="Homologue species name"),
    "homology_type":              ColumnSpec(description="Homology type (ortholog_one2one, paralog, ...)"),
    "target_gene_id":             ColumnSpec(description="Gene ID of the homologue (Ensembl or species-specific, stored as-is)"),
    "is_high_confidence":         ColumnSpec(description="Whether the homology call is high-confidence"),
    "target_gene_symbol":         ColumnSpec(description="Symbol of the homologue gene"),
    "query_percentage_identity":  ColumnSpec(description="Percentage identity from the query's perspective"),
    "target_percentage_identity": ColumnSpec(description="Percentage identity from the target's perspective"),
    "priority":                   ColumnSpec(description="Display priority of the homologue species"),
}

_CHEMICAL_PROBES_DOC = {
    "target_id":            ColumnSpec(identifier=EnsemblGeneId, required=True, description="Ensembl gene ID"),
    "probe_id":             ColumnSpec(description="Chemical probe ID"),
    "target_from_source_id": ColumnSpec(description="Source target ID the probe was reported against"),
    "drug_from_source_id":  ColumnSpec(description="Source drug ID"),
    "drug_id":              ColumnSpec(identifier=ChemblId, description="ChEMBL ID of the probe molecule, if mapped"),
    "mechanism_of_action":  ColumnSpec(description="Mechanism(s) of action (list)"),
    "origin":               ColumnSpec(description="Probe origin(s) (list)"),
    "control":              ColumnSpec(description="Recommended negative-control compound"),
    "is_high_quality":      ColumnSpec(description="Whether the probe is high quality"),
    "probes_drugs_score":   ColumnSpec(description="Probes & Drugs portal score"),
    "probe_miner_score":    ColumnSpec(description="ProbeMiner score"),
    "score_in_cells":       ColumnSpec(description="Score for use in cells"),
    "score_in_organisms":   ColumnSpec(description="Score for use in organisms"),
}

_CHEMICAL_PROBE_URLS_DOC = {
    "target_id": ColumnSpec(identifier=EnsemblGeneId, required=True, description="Ensembl gene ID"),
    "probe_id":  ColumnSpec(description="Chemical probe ID this URL belongs to"),
    "nice_name": ColumnSpec(description="Display name of the link"),
    "url":       ColumnSpec(description="Reference URL"),
}

_SAFETY_LIABILITIES_DOC = {
    "target_id":       ColumnSpec(identifier=EnsemblGeneId, required=True, description="Ensembl gene ID"),
    "liability_index": ColumnSpec(description="Index of the liability within the gene's list (links the grandchild tables)"),
    "event":           ColumnSpec(description="Safety event / adverse outcome"),
    "event_id":        ColumnSpec(description="Ontology ID of the safety event"),
    "datasource":      ColumnSpec(description="Source of the safety liability"),
    "literature":      ColumnSpec(description="Supporting literature reference"),
    "url":             ColumnSpec(description="Source URL"),
}

_SAFETY_EFFECTS_DOC = {
    "target_id":       ColumnSpec(identifier=EnsemblGeneId, required=True, description="Ensembl gene ID"),
    "liability_index": ColumnSpec(description="Index of the parent liability within the gene's list"),
    "direction":       ColumnSpec(description="Direction of the effect (e.g. activation/inhibition)"),
    "dosing":          ColumnSpec(description="Dosing context of the effect"),
}

_SAFETY_BIOSAMPLES_DOC = {
    "target_id":       ColumnSpec(identifier=EnsemblGeneId, required=True, description="Ensembl gene ID"),
    "liability_index": ColumnSpec(description="Index of the parent liability within the gene's list"),
    "tissue_label":    ColumnSpec(description="Tissue label"),
    "tissue_id":       ColumnSpec(description="Tissue ontology ID"),
    "cell_label":      ColumnSpec(description="Cell label"),
    "cell_format":     ColumnSpec(description="Cell format"),
    "cell_id":         ColumnSpec(description="Cell ontology ID"),
}

_SAFETY_STUDIES_DOC = {
    "target_id":       ColumnSpec(identifier=EnsemblGeneId, required=True, description="Ensembl gene ID"),
    "liability_index": ColumnSpec(description="Index of the parent liability within the gene's list"),
    "description":     ColumnSpec(description="Study description"),
    "name":            ColumnSpec(description="Study name"),
    "type":            ColumnSpec(description="Study type"),
}

_HALLMARK_ATTRIBUTES_DOC = {
    "target_id":   ColumnSpec(identifier=EnsemblGeneId, required=True, description="Ensembl gene ID"),
    "pmid":        ColumnSpec(identifier=PubmedId, description="Supporting PubMed ID"),
    "description": ColumnSpec(description="Hallmark attribute description"),
    "name":        ColumnSpec(description="Hallmark attribute name"),
}

_CANCER_HALLMARKS_DOC = {
    "target_id":   ColumnSpec(identifier=EnsemblGeneId, required=True, description="Ensembl gene ID"),
    "pmid":        ColumnSpec(identifier=PubmedId, description="Supporting PubMed ID"),
    "description": ColumnSpec(description="Cancer-hallmark description"),
    "impact":      ColumnSpec(description="Whether the target promotes or suppresses the hallmark"),
    "label":       ColumnSpec(description="Cancer-hallmark label"),
}

TABLES_DOC = {
    "targets": _TARGETS_DOC,
    "targets_transcripts": _TRANSCRIPTS_DOC,
    "targets_go": _GO_DOC,
    "targets_synonyms": _SYNONYMS_DOC,
    "targets_subcellular_locations": _SUBCELLULAR_DOC,
    "targets_classes": _CLASSES_DOC,
    "targets_constraints": _CONSTRAINTS_DOC,
    "targets_protein_ids": _PROTEIN_IDS_DOC,
    "targets_db_xrefs": _DB_XREFS_DOC,
    "targets_pathways": _PATHWAYS_DOC,
    "targets_tractability": _TRACTABILITY_DOC,
    "targets_homologues": _HOMOLOGUES_DOC,
    "targets_chemical_probes": _CHEMICAL_PROBES_DOC,
    "targets_chemical_probe_urls": _CHEMICAL_PROBE_URLS_DOC,
    "targets_safety_liabilities": _SAFETY_LIABILITIES_DOC,
    "targets_safety_effects": _SAFETY_EFFECTS_DOC,
    "targets_safety_biosamples": _SAFETY_BIOSAMPLES_DOC,
    "targets_safety_studies": _SAFETY_STUDIES_DOC,
    "targets_hallmark_attributes": _HALLMARK_ATTRIBUTES_DOC,
    "targets_cancer_hallmarks": _CANCER_HALLMARKS_DOC,
}


@register
class OpenTargetsTarget(OpenTargetsProductPipeline):
    name = "opentargets_target"
    raw_dirname = "target"
    transform_module = "lacuna_etl.datasets.opentargets.target"
    first_table = "targets"
    tables_doc = TABLES_DOC
