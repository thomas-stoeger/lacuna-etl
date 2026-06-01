"""Open Targets variant index (annotated genetic variants).

  variants                        - one row per variant (scalar fields + rsID list)
  variants_effects                - per-variant in-silico effect predictions
  variants_transcript_consequences - per-variant, per-transcript consequences
  variants_allele_frequencies     - per-variant population allele frequencies
  variants_db_xrefs               - per-variant cross-references
"""

import polars as pl

from lacuna_etl.core.identifiers import EnsemblGeneId
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.opentargets._base import OpenTargetsProductPipeline
from lacuna_etl.datasets.opentargets._helpers import explode_struct_list
from lacuna_etl.datasets.registry import register

_KEY = {"variantId": "variant_id"}


def transform_file(df: pl.DataFrame) -> dict[str, pl.DataFrame]:
    variants = df.select(
        pl.col("variantId").alias("variant_id"),
        pl.col("chromosome").alias("chromosome"),
        pl.col("position").cast(pl.Int64).alias("position"),
        pl.col("referenceAllele").alias("reference_allele"),
        pl.col("alternateAllele").alias("alternate_allele"),
        pl.col("mostSevereConsequenceId").alias("most_severe_consequence_id"),
        pl.col("hgvsId").alias("hgvs_id"),
        pl.col("variantDescription").alias("variant_description"),
        pl.col("rsIds").alias("rs_ids"),
    )
    effects = explode_struct_list(
        df, _KEY, "variantEffect",
        {
            "method": "method", "assessment": "assessment", "score": "score",
            "assessmentFlag": "assessment_flag", "targetId": "target_id",
            "normalisedScore": "normalised_score",
        },
    )
    transcript = explode_struct_list(
        df, _KEY, "transcriptConsequences",
        {
            "transcriptId": "transcript_id", "targetId": "target_id",
            "approvedSymbol": "approved_symbol", "biotype": "biotype",
            "isEnsemblCanonical": "is_ensembl_canonical", "impact": "impact",
            "aminoAcidChange": "amino_acid_change", "codons": "codons",
            "consequenceScore": "consequence_score", "distanceFromFootprint": "distance_from_footprint",
            "distanceFromTss": "distance_from_tss", "appris": "appris", "maneSelect": "mane_select",
            "lofteePrediction": "loftee_prediction", "siftPrediction": "sift_prediction",
            "polyphenPrediction": "polyphen_prediction", "transcriptIndex": "transcript_index",
            "variantFunctionalConsequenceIds": "variant_functional_consequence_ids",
            "uniprotAccessions": "uniprot_accessions",
        },
        casts={"distance_from_footprint": pl.Int64, "distance_from_tss": pl.Int64,
               "transcript_index": pl.Int64},
    )
    allele_frequencies = explode_struct_list(
        df, _KEY, "alleleFrequencies",
        {"populationName": "population_name", "alleleFrequency": "allele_frequency"},
    )
    db_xrefs = explode_struct_list(
        df, _KEY, "dbXrefs", {"id": "xref_id", "source": "source"},
    )
    return {
        "variants": variants,
        "variants_effects": effects,
        "variants_transcript_consequences": transcript,
        "variants_allele_frequencies": allele_frequencies,
        "variants_db_xrefs": db_xrefs,
    }


TABLES_DOC = {
    "variants": {
        "variant_id":                 ColumnSpec(required=True, description="Open Targets variant ID (chrom_pos_ref_alt)"),
        "chromosome":                 ColumnSpec(description="Chromosome"),
        "position":                   ColumnSpec(description="1-based position"),
        "reference_allele":           ColumnSpec(description="Reference allele"),
        "alternate_allele":           ColumnSpec(description="Alternate allele"),
        "most_severe_consequence_id": ColumnSpec(description="Sequence Ontology ID of the most severe consequence"),
        "hgvs_id":                    ColumnSpec(description="HGVS identifier"),
        "variant_description":        ColumnSpec(description="Free-text variant description"),
        "rs_ids":                     ColumnSpec(description="dbSNP rsIDs (list)"),
    },
    "variants_effects": {
        "variant_id":       ColumnSpec(required=True, description="Open Targets variant ID"),
        "method":           ColumnSpec(description="Prediction method (e.g. AlphaMissense, FoldX)"),
        "assessment":       ColumnSpec(description="Categorical assessment"),
        "score":            ColumnSpec(description="Raw method score"),
        "assessment_flag":  ColumnSpec(description="Assessment flag"),
        "target_id":        ColumnSpec(identifier=EnsemblGeneId, description="Ensembl gene ID the effect is assessed against"),
        "normalised_score": ColumnSpec(description="Normalised score in [0, 1]"),
    },
    "variants_transcript_consequences": {
        "variant_id":                         ColumnSpec(required=True, description="Open Targets variant ID"),
        "transcript_id":                      ColumnSpec(description="Ensembl transcript ID"),
        "target_id":                          ColumnSpec(identifier=EnsemblGeneId, description="Ensembl gene ID of the transcript"),
        "approved_symbol":                    ColumnSpec(description="Approved gene symbol"),
        "biotype":                            ColumnSpec(description="Transcript biotype"),
        "is_ensembl_canonical":               ColumnSpec(description="Whether this is the Ensembl canonical transcript"),
        "impact":                             ColumnSpec(description="VEP impact (HIGH, MODERATE, LOW, MODIFIER)"),
        "amino_acid_change":                  ColumnSpec(description="Amino-acid change"),
        "codons":                             ColumnSpec(description="Codon change"),
        "consequence_score":                  ColumnSpec(description="Consequence severity score"),
        "distance_from_footprint":            ColumnSpec(description="Distance from the transcript footprint (bp)"),
        "distance_from_tss":                  ColumnSpec(description="Distance from the transcript TSS (bp)"),
        "appris":                             ColumnSpec(description="APPRIS annotation"),
        "mane_select":                        ColumnSpec(description="MANE Select transcript annotation"),
        "loftee_prediction":                  ColumnSpec(description="LOFTEE loss-of-function prediction"),
        "sift_prediction":                    ColumnSpec(description="SIFT score"),
        "polyphen_prediction":                ColumnSpec(description="PolyPhen score"),
        "transcript_index":                   ColumnSpec(description="Index of the transcript consequence within the variant"),
        "variant_functional_consequence_ids": ColumnSpec(description="Sequence Ontology functional-consequence IDs (list)"),
        "uniprot_accessions":                 ColumnSpec(description="UniProt accessions of the transcript's protein (list)"),
    },
    "variants_allele_frequencies": {
        "variant_id":       ColumnSpec(required=True, description="Open Targets variant ID"),
        "population_name":  ColumnSpec(description="Population (e.g. gnomAD subpopulation)"),
        "allele_frequency": ColumnSpec(description="Alternate-allele frequency in the population"),
    },
    "variants_db_xrefs": {
        "variant_id": ColumnSpec(required=True, description="Open Targets variant ID"),
        "xref_id":    ColumnSpec(description="Identifier in the cross-referenced database"),
        "source":     ColumnSpec(description="Cross-referenced database"),
    },
}


@register
class OpenTargetsVariant(OpenTargetsProductPipeline):
    name = "opentargets_variant"
    raw_dirname = "variant"
    transform_module = "lacuna_etl.datasets.opentargets.variant"
    first_table = "variants"
    tables_doc = TABLES_DOC
