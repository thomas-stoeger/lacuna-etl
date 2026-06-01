"""Open Targets pharmacogenomics (variant/haplotype -> drug response).

Source records are keyless and carry a `variantAnnotation` list, so the table is
denormalised to one row per (pharmacogenomics evidence, variant annotation): the
evidence-level scalar fields repeat across a record's annotations, and the
associated drugs are flattened into parallel list columns. Records with no
annotation survive as a single row with null annotation fields.

  pharmacogenomics - one row per (pharmacogenomics evidence, variant annotation)
"""

import polars as pl

from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.opentargets._base import OpenTargetsProductPipeline
from lacuna_etl.datasets.registry import register


def transform_file(df: pl.DataFrame) -> dict[str, pl.DataFrame]:
    ann = pl.col("variantAnnotation")
    pgx = (
        df.with_columns(
            pl.col("drugs").list.eval(pl.element().struct.field("drugId")).alias("drug_ids"),
            pl.col("drugs").list.eval(pl.element().struct.field("drugFromSource")).alias("drug_from_sources"),
        )
        .explode("variantAnnotation")
        .select(
            pl.col("datasourceId").alias("datasource_id"),
            pl.col("datasourceVersion").alias("datasource_version"),
            pl.col("datatypeId").alias("datatype_id"),
            pl.col("targetFromSourceId").alias("target_from_source_id"),
            pl.col("genotypeId").alias("genotype_id"),
            pl.col("genotype").alias("genotype"),
            pl.col("genotypeAnnotationText").alias("genotype_annotation_text"),
            pl.col("haplotypeId").alias("haplotype_id"),
            pl.col("haplotypeFromSourceId").alias("haplotype_from_source_id"),
            pl.col("variantId").alias("variant_id"),
            pl.col("variantRsId").alias("variant_rs_id"),
            pl.col("variantFunctionalConsequenceId").alias("variant_functional_consequence_id"),
            pl.col("phenotypeFromSourceId").alias("phenotype_from_source_id"),
            pl.col("phenotypeText").alias("phenotype_text"),
            pl.col("pgxCategory").alias("pgx_category"),
            pl.col("evidenceLevel").alias("evidence_level"),
            pl.col("directionality").alias("directionality"),
            pl.col("isDirectTarget").alias("is_direct_target"),
            pl.col("studyId").alias("study_id"),
            pl.col("literature").alias("literature"),
            pl.col("drug_ids"),
            pl.col("drug_from_sources"),
            ann.struct.field("id").alias("variant_annotation_id"),
            ann.struct.field("entity").alias("variant_annotation_entity"),
            ann.struct.field("baseAlleleOrGenotype").alias("variant_annotation_base_allele_or_genotype"),
            ann.struct.field("comparisonAlleleOrGenotype").alias("variant_annotation_comparison_allele_or_genotype"),
            ann.struct.field("directionality").alias("variant_annotation_directionality"),
            ann.struct.field("effect").alias("variant_annotation_effect"),
            ann.struct.field("effectType").alias("variant_annotation_effect_type"),
            ann.struct.field("effectDescription").alias("variant_annotation_effect_description"),
            ann.struct.field("literature").alias("variant_annotation_literature"),
        )
    )
    return {"pharmacogenomics": pgx}


TABLES_DOC = {
    "pharmacogenomics": {
        "datasource_id":                     ColumnSpec(description="Pharmacogenomics datasource ID (e.g. pharmgkb)"),
        "datasource_version":                ColumnSpec(description="Datasource version"),
        "datatype_id":                       ColumnSpec(description="Datatype ID"),
        "target_from_source_id":             ColumnSpec(description="Target identifier as reported by the source"),
        "genotype_id":                       ColumnSpec(description="Genotype ID"),
        "genotype":                          ColumnSpec(description="Genotype call"),
        "genotype_annotation_text":          ColumnSpec(description="Free-text genotype annotation"),
        "haplotype_id":                      ColumnSpec(description="Haplotype ID"),
        "haplotype_from_source_id":          ColumnSpec(description="Haplotype identifier as reported by the source"),
        "variant_id":                        ColumnSpec(description="Open Targets variant ID (chrom_pos_ref_alt)"),
        "variant_rs_id":                     ColumnSpec(description="dbSNP rsID of the variant"),
        "variant_functional_consequence_id": ColumnSpec(description="Sequence Ontology functional-consequence ID"),
        "phenotype_from_source_id":          ColumnSpec(description="Phenotype identifier as reported by the source"),
        "phenotype_text":                    ColumnSpec(description="Phenotype description"),
        "pgx_category":                      ColumnSpec(description="Pharmacogenomics category (toxicity, efficacy, ...)"),
        "evidence_level":                    ColumnSpec(description="Source evidence level"),
        "directionality":                    ColumnSpec(description="Directionality of the pharmacogenomic effect"),
        "is_direct_target":                  ColumnSpec(description="Whether the gene is the drug's direct target"),
        "study_id":                          ColumnSpec(description="Source study ID"),
        "literature":                        ColumnSpec(description="Supporting PubMed IDs (list)"),
        "drug_ids":                          ColumnSpec(description="ChEMBL IDs of the associated drugs (list)"),
        "drug_from_sources":                 ColumnSpec(description="Drug identifiers/names as reported by the source (list)"),
        "variant_annotation_id":             ColumnSpec(description="Variant-annotation ID"),
        "variant_annotation_entity":         ColumnSpec(description="Entity the annotation concerns (variant, haplotype, ...)"),
        "variant_annotation_base_allele_or_genotype":       ColumnSpec(description="Base allele/genotype of the annotation"),
        "variant_annotation_comparison_allele_or_genotype": ColumnSpec(description="Comparison allele/genotype of the annotation"),
        "variant_annotation_directionality": ColumnSpec(description="Directionality of the annotation"),
        "variant_annotation_effect":         ColumnSpec(description="Annotated effect"),
        "variant_annotation_effect_type":    ColumnSpec(description="Effect type"),
        "variant_annotation_effect_description": ColumnSpec(description="Effect description"),
        "variant_annotation_literature":     ColumnSpec(description="Supporting literature reference for the annotation"),
    },
}


@register
class OpenTargetsPharmacogenomics(OpenTargetsProductPipeline):
    name = "opentargets_pharmacogenomics"
    raw_dirname = "pharmacogenomics"
    transform_module = "lacuna_etl.datasets.opentargets.pharmacogenomics"
    first_table = "pharmacogenomics"
    tables_doc = TABLES_DOC
