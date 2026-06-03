"""Open Targets per-product pipelines.

Each module under this package registers one DatasetPipeline subclass
(`opentargets_target`, `opentargets_disease`, ...) that transforms the
corresponding Open Targets Parquet product into per-table Parquet shards.
Importing the module here is what registers its pipeline with the registry.
"""

from lacuna_etl.datasets.opentargets import association_by_datasource_direct as _association_by_datasource_direct  # noqa: F401
from lacuna_etl.datasets.opentargets import association_by_datasource_indirect as _association_by_datasource_indirect  # noqa: F401
from lacuna_etl.datasets.opentargets import association_by_datatype_direct as _association_by_datatype_direct  # noqa: F401
from lacuna_etl.datasets.opentargets import association_by_datatype_indirect as _association_by_datatype_indirect  # noqa: F401
from lacuna_etl.datasets.opentargets import association_overall_direct as _association_overall_direct  # noqa: F401
from lacuna_etl.datasets.opentargets import association_overall_indirect as _association_overall_indirect  # noqa: F401
from lacuna_etl.datasets.opentargets import biosample as _biosample  # noqa: F401
from lacuna_etl.datasets.opentargets import clinical_indication as _clinical_indication  # noqa: F401
from lacuna_etl.datasets.opentargets import clinical_report as _clinical_report  # noqa: F401
from lacuna_etl.datasets.opentargets import clinical_target as _clinical_target  # noqa: F401
from lacuna_etl.datasets.opentargets import colocalisation as _colocalisation  # noqa: F401
from lacuna_etl.datasets.opentargets import credible_set as _credible_set  # noqa: F401
from lacuna_etl.datasets.opentargets import disease as _disease  # noqa: F401
from lacuna_etl.datasets.opentargets import disease_hpo as _disease_hpo  # noqa: F401
from lacuna_etl.datasets.opentargets import disease_phenotype as _disease_phenotype  # noqa: F401
from lacuna_etl.datasets.opentargets import drug_mechanism_of_action as _drug_mechanism_of_action  # noqa: F401
from lacuna_etl.datasets.opentargets import drug_molecule as _drug_molecule  # noqa: F401
from lacuna_etl.datasets.opentargets import drug_warning as _drug_warning  # noqa: F401
from lacuna_etl.datasets.opentargets import enhancer_to_gene as _enhancer_to_gene  # noqa: F401
from lacuna_etl.datasets.opentargets import evidence_cancer_biomarkers as _evidence_cancer_biomarkers  # noqa: F401
from lacuna_etl.datasets.opentargets import evidence_cancer_gene_census as _evidence_cancer_gene_census  # noqa: F401
from lacuna_etl.datasets.opentargets import evidence_clingen as _evidence_clingen  # noqa: F401
from lacuna_etl.datasets.opentargets import evidence_clinical_precedence as _evidence_clinical_precedence  # noqa: F401
from lacuna_etl.datasets.opentargets import evidence_crispr as _evidence_crispr  # noqa: F401
from lacuna_etl.datasets.opentargets import evidence_crispr_screen as _evidence_crispr_screen  # noqa: F401
from lacuna_etl.datasets.opentargets import evidence_europepmc as _evidence_europepmc  # noqa: F401
from lacuna_etl.datasets.opentargets import evidence_eva as _evidence_eva  # noqa: F401
from lacuna_etl.datasets.opentargets import evidence_eva_somatic as _evidence_eva_somatic  # noqa: F401
from lacuna_etl.datasets.opentargets import evidence_expression_atlas as _evidence_expression_atlas  # noqa: F401
from lacuna_etl.datasets.opentargets import evidence_gene2phenotype as _evidence_gene2phenotype  # noqa: F401
from lacuna_etl.datasets.opentargets import evidence_gene_burden as _evidence_gene_burden  # noqa: F401
from lacuna_etl.datasets.opentargets import evidence_genomics_england as _evidence_genomics_england  # noqa: F401
from lacuna_etl.datasets.opentargets import evidence_gwas_credible_sets as _evidence_gwas_credible_sets  # noqa: F401
from lacuna_etl.datasets.opentargets import evidence_impc as _evidence_impc  # noqa: F401
from lacuna_etl.datasets.opentargets import evidence_intogen as _evidence_intogen  # noqa: F401
from lacuna_etl.datasets.opentargets import evidence_orphanet as _evidence_orphanet  # noqa: F401
from lacuna_etl.datasets.opentargets import evidence_reactome as _evidence_reactome  # noqa: F401
from lacuna_etl.datasets.opentargets import evidence_uniprot_literature as _evidence_uniprot_literature  # noqa: F401
from lacuna_etl.datasets.opentargets import evidence_uniprot_variants as _evidence_uniprot_variants  # noqa: F401
from lacuna_etl.datasets.opentargets import expression as _expression  # noqa: F401
from lacuna_etl.datasets.opentargets import go as _go  # noqa: F401
from lacuna_etl.datasets.opentargets import interaction as _interaction  # noqa: F401
from lacuna_etl.datasets.opentargets import interaction_evidence as _interaction_evidence  # noqa: F401
from lacuna_etl.datasets.opentargets import l2g_prediction as _l2g_prediction  # noqa: F401
from lacuna_etl.datasets.opentargets import literature as _literature  # noqa: F401
from lacuna_etl.datasets.opentargets import literature_vector as _literature_vector  # noqa: F401
from lacuna_etl.datasets.opentargets import mouse_phenotype as _mouse_phenotype  # noqa: F401
from lacuna_etl.datasets.opentargets import openfda_significant_adverse_drug_reactions as _openfda_significant_adverse_drug_reactions  # noqa: F401
from lacuna_etl.datasets.opentargets import pharmacogenomics as _pharmacogenomics  # noqa: F401
from lacuna_etl.datasets.opentargets import so as _so  # noqa: F401
from lacuna_etl.datasets.opentargets import study as _study  # noqa: F401
from lacuna_etl.datasets.opentargets import target as _target  # noqa: F401
from lacuna_etl.datasets.opentargets import target_essentiality as _target_essentiality  # noqa: F401
from lacuna_etl.datasets.opentargets import target_prioritisation as _target_prioritisation  # noqa: F401
from lacuna_etl.datasets.opentargets import variant as _variant  # noqa: F401
