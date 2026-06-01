"""Open Targets credible sets (fine-mapped GWAS/QTL association signals).

  credible_sets       - one row per study-locus (fine-mapped credible set)
  credible_sets_locus - per-credible-set member variants with posterior probabilities
  credible_sets_ld    - per-credible-set LD tag variants
"""

import polars as pl

from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.opentargets._base import OpenTargetsProductPipeline
from lacuna_etl.datasets.opentargets._helpers import explode_struct_list
from lacuna_etl.datasets.registry import register

_KEY = {"studyLocusId": "study_locus_id"}


def transform_file(df: pl.DataFrame) -> dict[str, pl.DataFrame]:
    credible_sets = df.select(
        pl.col("studyLocusId").alias("study_locus_id"),
        pl.col("studyId").alias("study_id"),
        pl.col("studyType").alias("study_type"),
        pl.col("variantId").alias("variant_id"),
        pl.col("chromosome").alias("chromosome"),
        pl.col("position").cast(pl.Int64).alias("position"),
        pl.col("region").alias("region"),
        pl.col("beta").alias("beta"),
        pl.col("zScore").alias("z_score"),
        pl.col("pValueMantissa").alias("p_value_mantissa"),
        pl.col("pValueExponent").cast(pl.Int64).alias("p_value_exponent"),
        pl.col("effectAlleleFrequencyFromSource").alias("effect_allele_frequency_from_source"),
        pl.col("standardError").alias("standard_error"),
        pl.col("subStudyDescription").alias("sub_study_description"),
        pl.col("finemappingMethod").alias("finemapping_method"),
        pl.col("credibleSetIndex").cast(pl.Int64).alias("credible_set_index"),
        pl.col("credibleSetlog10BF").alias("credible_set_log10_bf"),
        pl.col("purityMeanR2").alias("purity_mean_r2"),
        pl.col("purityMinR2").alias("purity_min_r2"),
        pl.col("locusStart").cast(pl.Int64).alias("locus_start"),
        pl.col("locusEnd").cast(pl.Int64).alias("locus_end"),
        pl.col("sampleSize").cast(pl.Int64).alias("sample_size"),
        pl.col("confidence").alias("confidence"),
        pl.col("isTransQtl").alias("is_trans_qtl"),
        pl.col("qualityControls").alias("quality_controls"),
    )
    locus = explode_struct_list(
        df, _KEY, "locus",
        {
            "variantId": "variant_id", "is95CredibleSet": "is_95_credible_set",
            "is99CredibleSet": "is_99_credible_set", "logBF": "log_bf",
            "posteriorProbability": "posterior_probability", "pValueMantissa": "p_value_mantissa",
            "pValueExponent": "p_value_exponent", "beta": "beta",
            "standardError": "standard_error", "r2Overall": "r2_overall",
        },
        casts={"p_value_exponent": pl.Int64},
    )
    ld = explode_struct_list(
        df, _KEY, "ldSet", {"tagVariantId": "tag_variant_id", "r2Overall": "r2_overall"},
    )
    return {"credible_sets": credible_sets, "credible_sets_locus": locus, "credible_sets_ld": ld}


TABLES_DOC = {
    "credible_sets": {
        "study_locus_id":                      ColumnSpec(required=True, description="Study-locus ID (credible set ID)"),
        "study_id":                            ColumnSpec(description="GWAS/QTL study ID"),
        "study_type":                          ColumnSpec(description="Study type (gwas, eqtl, pqtl, ...)"),
        "variant_id":                          ColumnSpec(description="Lead variant ID (chrom_pos_ref_alt)"),
        "chromosome":                          ColumnSpec(description="Chromosome of the locus"),
        "position":                            ColumnSpec(description="Position of the lead variant"),
        "region":                              ColumnSpec(description="Genomic region of the credible set"),
        "beta":                                ColumnSpec(description="Effect size of the lead variant"),
        "z_score":                             ColumnSpec(description="Z-score of the lead variant"),
        "p_value_mantissa":                    ColumnSpec(description="Mantissa of the lead-variant p-value"),
        "p_value_exponent":                    ColumnSpec(description="Base-10 exponent of the lead-variant p-value"),
        "effect_allele_frequency_from_source": ColumnSpec(description="Effect-allele frequency reported by the source"),
        "standard_error":                      ColumnSpec(description="Standard error of the effect size"),
        "sub_study_description":               ColumnSpec(description="Sub-study description (e.g. QTL gene/tissue)"),
        "finemapping_method":                  ColumnSpec(description="Fine-mapping method (SuSiE, PICS, ...)"),
        "credible_set_index":                  ColumnSpec(description="Index of the credible set within the locus"),
        "credible_set_log10_bf":               ColumnSpec(description="log10 Bayes factor of the credible set"),
        "purity_mean_r2":                      ColumnSpec(description="Mean pairwise r2 within the credible set"),
        "purity_min_r2":                       ColumnSpec(description="Minimum pairwise r2 within the credible set"),
        "locus_start":                         ColumnSpec(description="Start coordinate of the fine-mapped locus"),
        "locus_end":                           ColumnSpec(description="End coordinate of the fine-mapped locus"),
        "sample_size":                         ColumnSpec(description="Sample size of the underlying study"),
        "confidence":                          ColumnSpec(description="Confidence category of the credible set"),
        "is_trans_qtl":                        ColumnSpec(description="Whether the QTL signal is trans-acting"),
        "quality_controls":                    ColumnSpec(description="Quality-control flags (list)"),
    },
    "credible_sets_locus": {
        "study_locus_id":        ColumnSpec(required=True, description="Study-locus ID (credible set ID)"),
        "variant_id":            ColumnSpec(description="Member variant ID (chrom_pos_ref_alt)"),
        "is_95_credible_set":    ColumnSpec(description="Whether the variant is in the 95% credible set"),
        "is_99_credible_set":    ColumnSpec(description="Whether the variant is in the 99% credible set"),
        "log_bf":                ColumnSpec(description="log Bayes factor of the variant"),
        "posterior_probability": ColumnSpec(description="Posterior inclusion probability of the variant"),
        "p_value_mantissa":      ColumnSpec(description="Mantissa of the variant p-value"),
        "p_value_exponent":      ColumnSpec(description="Base-10 exponent of the variant p-value"),
        "beta":                  ColumnSpec(description="Effect size of the variant"),
        "standard_error":        ColumnSpec(description="Standard error of the effect size"),
        "r2_overall":            ColumnSpec(description="r2 of the variant with the lead variant"),
    },
    "credible_sets_ld": {
        "study_locus_id": ColumnSpec(required=True, description="Study-locus ID (credible set ID)"),
        "tag_variant_id": ColumnSpec(description="LD tag variant ID (chrom_pos_ref_alt)"),
        "r2_overall":     ColumnSpec(description="r2 between the tag variant and the lead variant"),
    },
}


@register
class OpenTargetsCredibleSet(OpenTargetsProductPipeline):
    name = "opentargets_credible_set"
    raw_dirname = "credible_set"
    transform_module = "lacuna_etl.datasets.opentargets.credible_set"
    first_table = "credible_sets"
    tables_doc = TABLES_DOC
