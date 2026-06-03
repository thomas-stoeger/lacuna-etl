"""Open Targets colocalisation (pairwise colocalisation between GWAS/QTL credible sets).

  colocalisations - one row per (left study-locus, right study-locus) colocalisation test
"""

import polars as pl

from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.opentargets._base import OpenTargetsProductPipeline
from lacuna_etl.datasets.registry import register


def transform_file(df: pl.DataFrame) -> dict[str, pl.DataFrame]:
    colocalisations = df.select(
        pl.col("leftStudyLocusId").alias("left_study_locus_id"),
        pl.col("rightStudyLocusId").alias("right_study_locus_id"),
        pl.col("rightStudyType").alias("right_study_type"),
        pl.col("chromosome").alias("chromosome"),
        pl.col("colocalisationMethod").alias("colocalisation_method"),
        pl.col("numberColocalisingVariants").cast(pl.Int64).alias("number_colocalising_variants"),
        pl.col("h3").alias("h3"),
        pl.col("h4").alias("h4"),
        pl.col("clpp").alias("clpp"),
        pl.col("betaRatioSignAverage").alias("beta_ratio_sign_average"),
    )
    return {"colocalisations": colocalisations}


TABLES_DOC = {
    "colocalisations": {
        "left_study_locus_id":          ColumnSpec(required=True, description="Study-locus ID of the left credible set"),
        "right_study_locus_id":         ColumnSpec(required=True, description="Study-locus ID of the right credible set"),
        "right_study_type":             ColumnSpec(description="Study type of the right credible set (gwas, eqtl, ...)"),
        "chromosome":                   ColumnSpec(description="Chromosome of the colocalising locus"),
        "colocalisation_method":        ColumnSpec(description="Colocalisation method (COLOC, eCAVIAR)"),
        "number_colocalising_variants": ColumnSpec(description="Number of variants in the colocalisation analysis"),
        "h3":                           ColumnSpec(description="COLOC posterior probability of distinct causal variants (H3)"),
        "h4":                           ColumnSpec(description="COLOC posterior probability of a shared causal variant (H4)"),
        "clpp":                         ColumnSpec(description="eCAVIAR colocalisation posterior probability (CLPP)"),
        "beta_ratio_sign_average":      ColumnSpec(description="Average sign of the beta ratio between the two signals"),
    },
}


@register
class OpenTargetsColocalisation(OpenTargetsProductPipeline):
    name = "opentargets_colocalisation"
    raw_dirname = "colocalisation"
    transform_module = "lacuna_etl.datasets.opentargets.colocalisation"
    first_table = "colocalisations"
    tables_doc = TABLES_DOC
