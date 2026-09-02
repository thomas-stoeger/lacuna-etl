"""Shared machinery for the Open Targets `evidence_*` products.

Every evidence product is a flat table of one row per evidence record (keyed by a
stable `id` hash) linking a target to a disease, plus a common envelope of
provenance columns (`datasourceId`, `score`, `literature`, dates, ...) and a
handful of source-specific scalar / list-scalar columns. A few products also
carry `List(Struct)` columns, which are exploded into child tables keyed by
`evidence_id`.

To keep one source of truth for column names, types, and documentation across all
~20 products, every column the platform emits is registered once here:

  FIELDS    - scalar / list-scalar columns: output_name -> (source_col, ColumnSpec)
  CHILDREN  - List(Struct) columns: source_col -> (table_suffix, {subfield: (out, cast)}, doc)

Each product module then declares only the subset of column names it uses (its
parent columns and which struct children it has); `build()` and `build_doc()`
derive the tables and the sidecar documentation from this registry. `build()`
also asserts the input has no unregistered columns, so a new field in a future
release aborts the run rather than being silently dropped.
"""

from __future__ import annotations

import polars as pl

from lacuna_etl.core.identifiers import ChemblId, EnsemblGeneId, ReactomePathwayId
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.opentargets._helpers import explode_struct_list

_S = ColumnSpec

# --- scalar / list-scalar columns -------------------------------------------
# output_name -> (source column, ColumnSpec). Insertion order is the output column
# order of the `evidence` parent table.
FIELDS: dict[str, tuple[str, ColumnSpec]] = {
    "evidence_id":                  ("id", _S(required=True, description="Open Targets evidence record ID (stable hash of the evidence)")),
    "datasource_id":                ("datasourceId", _S(description="Evidence datasource ID (e.g. eva, europepmc, ot_crispr)")),
    "datatype_id":                  ("datatypeId", _S(description="Evidence datatype ID (e.g. genetic_association, literature)")),
    "target_id":                    ("targetId", _S(identifier=EnsemblGeneId, description="Ensembl gene ID of the target")),
    "target_from_source_id":        ("targetFromSourceId", _S(description="Target identifier as reported by the source")),
    "target_from_source":           ("targetFromSource", _S(description="Target label/symbol as reported by the source")),
    "disease_id":                   ("diseaseId", _S(description="Open Targets / EFO disease (or phenotype) ontology ID")),
    "disease_from_source":          ("diseaseFromSource", _S(description="Disease label as reported by the source")),
    "disease_from_source_id":       ("diseaseFromSourceId", _S(description="Disease identifier as reported by the source")),
    "disease_from_source_mapped_id": ("diseaseFromSourceMappedId", _S(description="Source disease mapped to an EFO ontology ID")),
    "drug_id":                      ("drugId", _S(identifier=ChemblId, description="ChEMBL ID of the associated drug")),
    "drug_from_source":             ("drugFromSource", _S(description="Drug identifier/name as reported by the source")),
    "drug_response":                ("drugResponse", _S(description="Drug-response phenotype (EFO ID)")),
    "biomarker_name":               ("biomarkerName", _S(description="Biomarker name")),
    "confidence":                   ("confidence", _S(description="Source-reported confidence level")),
    "clinical_report_id":           ("clinicalReportId", _S(description="Clinical report / trial ID backing the evidence")),
    "clinical_stage":               ("clinicalStage", _S(description="Maximum clinical trial stage reached")),
    "trial_why_stopped":            ("trialWhyStopped", _S(description="Reason a clinical trial was stopped, if any")),
    "study_start_date":             ("studyStartDate", _S(description="Clinical study start date")),
    "study_id":                     ("studyId", _S(description="Source study ID")),
    "study_locus_id":               ("studyLocusId", _S(description="GWAS study-locus ID backing the evidence")),
    "study_overview":               ("studyOverview", _S(description="Free-text study overview")),
    "project_id":                   ("projectId", _S(description="Source project ID")),
    "cohort_id":                    ("cohortId", _S(description="Cohort ID")),
    "cohort_short_name":            ("cohortShortName", _S(description="Cohort short name")),
    "cohort_description":           ("cohortDescription", _S(description="Cohort description")),
    "contrast":                     ("contrast", _S(description="Experimental contrast compared")),
    "cell_type":                    ("cellType", _S(description="Cell type studied")),
    "genetic_background":           ("geneticBackground", _S(description="Genetic background of the model/screen")),
    "crispr_screen_library":        ("crisprScreenLibrary", _S(description="CRISPR screen library used")),
    "statistical_test_tail":        ("statisticalTestTail", _S(description="Tail of the statistical test")),
    "statistical_method":           ("statisticalMethod", _S(description="Statistical method used")),
    "statistical_method_overview":  ("statisticalMethodOverview", _S(description="Statistical method description")),
    "log2_fold_change_value":       ("log2FoldChangeValue", _S(description="log2 fold change")),
    "log2_fold_change_percentile_rank": ("log2FoldChangePercentileRank", _S(description="Percentile rank of the log2 fold change")),
    "p_value_mantissa":             ("pValueMantissa", _S(description="Mantissa of the association p-value")),
    "p_value_exponent":             ("pValueExponent", _S(description="Base-10 exponent of the association p-value")),
    "beta":                         ("beta", _S(description="Effect size (beta)")),
    "beta_confidence_interval_lower": ("betaConfidenceIntervalLower", _S(description="Lower bound of the beta confidence interval")),
    "beta_confidence_interval_upper": ("betaConfidenceIntervalUpper", _S(description="Upper bound of the beta confidence interval")),
    "odds_ratio":                   ("oddsRatio", _S(description="Odds ratio")),
    "odds_ratio_confidence_interval_lower": ("oddsRatioConfidenceIntervalLower", _S(description="Lower bound of the odds-ratio confidence interval")),
    "odds_ratio_confidence_interval_upper": ("oddsRatioConfidenceIntervalUpper", _S(description="Upper bound of the odds-ratio confidence interval")),
    "ancestry":                     ("ancestry", _S(description="Study ancestry")),
    "ancestry_id":                  ("ancestryId", _S(description="Study ancestry ID (HANCESTRO)")),
    "study_sample_size":            ("studySampleSize", _S(description="Study sample size")),
    "study_cases":                  ("studyCases", _S(description="Number of cases in the study")),
    "study_cases_with_qualifying_variants": ("studyCasesWithQualifyingVariants", _S(description="Number of cases carrying qualifying variants")),
    "target_in_model":             ("targetInModel", _S(description="Model organism gene symbol")),
    "target_in_model_mgi_id":      ("targetInModelMgiId", _S(description="MGI ID of the model gene")),
    "target_in_model_ensembl_id":  ("targetInModelEnsemblId", _S(identifier=EnsemblGeneId, description="Ensembl gene ID of the model gene")),
    "biological_model_id":         ("biologicalModelId", _S(description="Source biological-model ID")),
    "biological_model_genetic_background": ("biologicalModelGeneticBackground", _S(description="Genetic background of the biological model")),
    "biological_model_allelic_composition": ("biologicalModelAllelicComposition", _S(description="Allelic composition of the biological model")),
    "variant_id":                  ("variantId", _S(description="Open Targets variant ID (chrom_pos_ref_alt)")),
    "variant_rs_id":               ("variantRsId", _S(description="dbSNP rsID of the variant")),
    "variant_from_source_id":      ("variantFromSourceId", _S(description="Variant identifier as reported by the source")),
    "variant_hgvs_id":             ("variantHgvsId", _S(description="HGVS identifier of the variant")),
    "variant_functional_consequence_id": ("variantFunctionalConsequenceId", _S(description="Sequence Ontology functional-consequence ID")),
    "reaction_id":                 ("reactionId", _S(description="Reactome reaction ID")),
    "reaction_name":               ("reactionName", _S(description="Reactome reaction name")),
    "target_modulation":           ("targetModulation", _S(description="Direction the target is modulated (up/down)")),
    "publication_year":            ("publicationYear", _S(description="Publication year of the supporting literature")),
    "resource_score":              ("resourceScore", _S(description="Source-specific resource score backing the evidence")),
    "release_date":                ("releaseDate", _S(description="Source release date")),
    "release_version":             ("releaseVersion", _S(description="Source release version")),
    "curation_date":               ("curationDate", _S(description="Curation date")),
    "direction_on_trait":          ("directionOnTrait", _S(description="Direction of effect on the trait (risk/protective)")),
    "direction_on_target":         ("directionOnTarget", _S(description="Direction of effect on the target (gain/loss of function)")),
    # list-scalar columns kept as list columns on the parent
    "literature":                  ("literature", _S(description="Supporting PubMed IDs (list)")),
    "pmc_ids":                     ("pmcIds", _S(description="Supporting PubMed Central IDs (list)")),
    "allele_origins":              ("alleleOrigins", _S(description="Allele origins (germline/somatic) (list)")),
    "allelic_requirements":        ("allelicRequirements", _S(description="Allelic requirements / inheritance modes (list)")),
    "clinical_significances":      ("clinicalSignificances", _S(description="Clinical significance terms (list)")),
    "cohort_phenotypes":           ("cohortPhenotypes", _S(description="Cohort phenotype descriptions (list)")),
    "significant_driver_methods":  ("significantDriverMethods", _S(description="Methods calling the gene a significant driver (list)")),
    "biosamples_from_source":      ("biosamplesFromSource", _S(description="Biosample identifiers from the source (list)")),
    "trial_stop_reason_categories": ("trialStopReasonCategories", _S(description="Categorised trial stop reasons (list)")),
    "variant_aminoacid_descriptions": ("variantAminoacidDescriptions", _S(description="Amino-acid change descriptions (list)")),
    "sex":                         ("sex", _S(description="Sexes the analysis applies to (list)")),
    "quality_controls":            ("qualityControls", _S(description="Quality-control flags raised on the evidence (list)")),
    # dates / score, emitted last
    "publication_date":            ("publicationDate", _S(description="Publication date of the supporting evidence")),
    "evidence_date":               ("evidenceDate", _S(description="Date the evidence was generated/recorded")),
    "score":                       ("score", _S(description="Harmonised evidence score in [0, 1]")),
}

_SRC_TO_OUT = {src: out for out, (src, _) in FIELDS.items()}

# --- List(Struct) columns -> child tables -----------------------------------
# source_col -> (table_suffix, {subfield: (output_name, cast_dtype_or_None)}, {output_name: ColumnSpec})
CHILDREN: dict[str, tuple[str, dict[str, tuple[str, object]], dict[str, ColumnSpec]]] = {
    "urls": ("urls",
             {"niceName": ("nice_name", None), "url": ("url", None)},
             {"nice_name": _S(description="Display name of the reference link"),
              "url": _S(description="Reference URL")}),
    "mutatedSamples": ("mutated_samples",
             {"functionalConsequenceId": ("functional_consequence_id", None),
              "numberMutatedSamples": ("number_mutated_samples", pl.Int64),
              "numberSamplesTested": ("number_samples_tested", pl.Int64),
              "numberSamplesWithMutationType": ("number_samples_with_mutation_type", pl.Int64)},
             {"functional_consequence_id": _S(description="Sequence Ontology functional-consequence ID"),
              "number_mutated_samples": _S(description="Number of mutated samples"),
              "number_samples_tested": _S(description="Number of samples tested"),
              "number_samples_with_mutation_type": _S(description="Number of samples with this mutation type")}),
    "diseaseCellLines": ("disease_cell_lines",
             {"id": ("cell_line_id", None), "name": ("cell_line_name", None),
              "tissue": ("tissue", None), "tissueId": ("tissue_id", None)},
             {"cell_line_id": _S(description="Cell-line ID"),
              "cell_line_name": _S(description="Cell-line name"),
              "tissue": _S(description="Tissue of origin"),
              "tissue_id": _S(description="UBERON tissue ID")}),
    "textMiningSentences": ("sentences",
             {"section": ("section", None), "text": ("text", None),
              "tStart": ("target_start", pl.Int64), "tEnd": ("target_end", pl.Int64),
              "dStart": ("disease_start", pl.Int64), "dEnd": ("disease_end", pl.Int64)},
             {"section": _S(description="Publication section the sentence was mined from"),
              "text": _S(description="Mined sentence text"),
              "target_start": _S(description="Start offset of the target mention"),
              "target_end": _S(description="End offset of the target mention"),
              "disease_start": _S(description="Start offset of the disease mention"),
              "disease_end": _S(description="End offset of the disease mention")}),
    "diseaseModelAssociatedModelPhenotypes": ("model_phenotypes",
             {"id": ("phenotype_id", None), "label": ("phenotype_label", None)},
             {"phenotype_id": _S(description="Model (e.g. Mammalian Phenotype) term ID"),
              "phenotype_label": _S(description="Model phenotype label")}),
    "diseaseModelAssociatedHumanPhenotypes": ("human_phenotypes",
             {"id": ("phenotype_id", None), "label": ("phenotype_label", None)},
             {"phenotype_id": _S(description="Human (HPO) phenotype term ID"),
              "phenotype_label": _S(description="Human phenotype label")}),
    "pathways": ("pathways",
             {"id": ("pathway_id", None), "name": ("pathway_name", None)},
             {"pathway_id": _S(identifier=ReactomePathwayId, description="Reactome pathway ID"),
              "pathway_name": _S(description="Reactome pathway name")}),
}

_KEY = {"id": "evidence_id"}


def build(df: pl.DataFrame, parent_cols: list[str], children: list[str]) -> dict[str, pl.DataFrame]:
    """Build the `evidence` parent table plus a child table per struct column.

    parent_cols: output names (keys of FIELDS) to emit on the parent, in order.
    children:    source column names (keys of CHILDREN) to explode into child tables.
    """
    expected = {FIELDS[c][0] for c in parent_cols} | set(children)
    unexpected = [c for c in df.columns if c not in expected]
    if unexpected:
        raise ValueError(f"unregistered evidence columns {unexpected}; register them in _evidence.FIELDS/CHILDREN")
    missing = [FIELDS[c][0] for c in parent_cols if FIELDS[c][0] not in df.columns]
    if missing:
        raise ValueError(f"declared parent columns absent from input: {missing}")

    parent = df.select([pl.col(FIELDS[c][0]).alias(c) for c in parent_cols])
    tables = {"evidence": parent}
    for src in children:
        suffix, fmap, _ = CHILDREN[src]
        fields = {sub: out for sub, (out, _) in fmap.items()}
        casts = {out: cast for sub, (out, cast) in fmap.items() if cast is not None}
        tables[f"evidence_{suffix}"] = explode_struct_list(df, _KEY, src, fields, casts=casts or None)
    return tables


def build_doc(parent_cols: list[str], children: list[str]) -> dict[str, dict[str, ColumnSpec]]:
    """Build TABLES_DOC mirroring `build()`."""
    doc = {"evidence": {c: FIELDS[c][1] for c in parent_cols}}
    for src in children:
        suffix, _, child_doc = CHILDREN[src]
        doc[f"evidence_{suffix}"] = {"evidence_id": FIELDS["evidence_id"][1], **child_doc}
    return doc
