"""Open Targets studies (GWAS and molecular-QTL studies behind the genetics evidence).

  studies                    - one row per study (scalar + list-scalar columns)
  studies_discovery_samples  - discovery-stage sample sizes by ancestry
  studies_replication_samples - replication-stage sample sizes by ancestry
  studies_ld_populations     - LD reference population structure
  studies_sumstat_qc         - summary-statistics QC metrics
"""

import polars as pl

from lacuna_etl.core.identifiers import EnsemblGeneId, PubmedId
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.opentargets._base import OpenTargetsProductPipeline
from lacuna_etl.datasets.opentargets._helpers import explode_struct_list
from lacuna_etl.datasets.registry import register

_KEY = {"studyId": "study_id"}


def transform_file(df: pl.DataFrame) -> dict[str, pl.DataFrame]:
    studies = df.select(
        pl.col("studyId").alias("study_id"),
        pl.col("studyType").alias("study_type"),
        pl.col("projectId").alias("project_id"),
        pl.col("geneId").alias("gene_id"),
        pl.col("traitFromSource").alias("trait_from_source"),
        pl.col("biosampleFromSourceId").alias("biosample_from_source_id"),
        pl.col("biosampleId").alias("biosample_id"),
        pl.col("pubmedId").cast(pl.Int64).alias("pmid"),
        pl.col("publicationTitle").alias("publication_title"),
        pl.col("publicationFirstAuthor").alias("publication_first_author"),
        pl.col("publicationDate").alias("publication_date"),
        pl.col("publicationJournal").alias("publication_journal"),
        pl.col("initialSampleSize").alias("initial_sample_size"),
        pl.col("nCases").cast(pl.Int64).alias("n_cases"),
        pl.col("nControls").cast(pl.Int64).alias("n_controls"),
        pl.col("nSamples").cast(pl.Int64).alias("n_samples"),
        pl.col("summarystatsLocation").alias("summarystats_location"),
        pl.col("hasSumstats").alias("has_sumstats"),
        pl.col("condition").alias("condition"),
        pl.col("traitFromSourceMappedIds").alias("trait_from_source_mapped_ids"),
        pl.col("backgroundTraitFromSourceMappedIds").alias("background_trait_from_source_mapped_ids"),
        pl.col("diseaseIds").alias("disease_ids"),
        pl.col("backgroundDiseaseIds").alias("background_disease_ids"),
        pl.col("cohorts").alias("cohorts"),
        pl.col("analysisFlags").alias("analysis_flags"),
        pl.col("qualityControls").alias("quality_controls"),
    )
    discovery = explode_struct_list(
        df, _KEY, "discoverySamples",
        {"sampleSize": "sample_size", "ancestry": "ancestry"}, casts={"sample_size": pl.Int64},
    )
    replication = explode_struct_list(
        df, _KEY, "replicationSamples",
        {"sampleSize": "sample_size", "ancestry": "ancestry"}, casts={"sample_size": pl.Int64},
    )
    ld_populations = explode_struct_list(
        df, _KEY, "ldPopulationStructure",
        {"ldPopulation": "ld_population", "relativeSampleSize": "relative_sample_size"},
    )
    sumstat_qc = explode_struct_list(
        df, _KEY, "sumstatQCValues",
        {"QCCheckName": "qc_check_name", "QCCheckValue": "qc_check_value"},
    )
    return {
        "studies": studies,
        "studies_discovery_samples": discovery,
        "studies_replication_samples": replication,
        "studies_ld_populations": ld_populations,
        "studies_sumstat_qc": sumstat_qc,
    }


_SAMPLES_DOC = {
    "study_id":    ColumnSpec(required=True, description="Study ID"),
    "sample_size": ColumnSpec(description="Sample size for this ancestry group"),
    "ancestry":    ColumnSpec(description="Ancestry group"),
}

TABLES_DOC = {
    "studies": {
        "study_id":                                ColumnSpec(required=True, description="Study ID"),
        "study_type":                              ColumnSpec(description="Study type (gwas, eqtl, pqtl, sqtl, ...)"),
        "project_id":                              ColumnSpec(description="Project ID the study belongs to"),
        "gene_id":                                 ColumnSpec(identifier=EnsemblGeneId, description="Ensembl gene ID (for molecular-QTL studies)"),
        "trait_from_source":                       ColumnSpec(description="Trait label as reported by the source"),
        "biosample_from_source_id":                ColumnSpec(description="Biosample identifier as reported by the source"),
        "biosample_id":                            ColumnSpec(description="Mapped biosample ontology ID"),
        "pmid":                                    ColumnSpec(identifier=PubmedId, description="PubMed ID of the study publication"),
        "publication_title":                       ColumnSpec(description="Publication title"),
        "publication_first_author":                ColumnSpec(description="Publication first author"),
        "publication_date":                        ColumnSpec(description="Publication date"),
        "publication_journal":                     ColumnSpec(description="Publication journal"),
        "initial_sample_size":                     ColumnSpec(description="Free-text initial sample-size description"),
        "n_cases":                                 ColumnSpec(description="Number of cases"),
        "n_controls":                              ColumnSpec(description="Number of controls"),
        "n_samples":                               ColumnSpec(description="Total number of samples"),
        "summarystats_location":                   ColumnSpec(description="Location of the summary statistics, if available"),
        "has_sumstats":                            ColumnSpec(description="Whether summary statistics are available"),
        "condition":                               ColumnSpec(description="Study condition/context"),
        "trait_from_source_mapped_ids":            ColumnSpec(description="EFO IDs the trait maps to (list)"),
        "background_trait_from_source_mapped_ids": ColumnSpec(description="EFO IDs of the background trait (list)"),
        "disease_ids":                             ColumnSpec(description="Mapped disease/EFO IDs (list)"),
        "background_disease_ids":                  ColumnSpec(description="Background disease/EFO IDs (list)"),
        "cohorts":                                 ColumnSpec(description="Contributing cohorts (list)"),
        "analysis_flags":                          ColumnSpec(description="Analysis flags (list)"),
        "quality_controls":                        ColumnSpec(description="Quality-control flags (list)"),
    },
    "studies_discovery_samples": _SAMPLES_DOC,
    "studies_replication_samples": _SAMPLES_DOC,
    "studies_ld_populations": {
        "study_id":            ColumnSpec(required=True, description="Study ID"),
        "ld_population":       ColumnSpec(description="LD reference population"),
        "relative_sample_size": ColumnSpec(description="Relative size of this population in the study"),
    },
    "studies_sumstat_qc": {
        "study_id":       ColumnSpec(required=True, description="Study ID"),
        "qc_check_name":  ColumnSpec(description="Summary-statistics QC check name"),
        "qc_check_value": ColumnSpec(description="QC check value"),
    },
}


@register
class OpenTargetsStudy(OpenTargetsProductPipeline):
    name = "opentargets_study"
    raw_dirname = "study"
    transform_module = "lacuna_etl.datasets.opentargets.study"
    first_table = "studies"
    tables_doc = TABLES_DOC
