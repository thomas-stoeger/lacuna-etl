"""Open Targets drug molecules (ChEMBL).

  drugs                 - one row per ChEMBL molecule, scalar fields plus scalar-list columns
  drugs_cross_references - one row per (molecule, source, external id)
"""

import polars as pl

from lacuna_etl.core.identifiers import ChemblId
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.opentargets._base import OpenTargetsProductPipeline
from lacuna_etl.datasets.registry import register


def transform_file(df: pl.DataFrame) -> dict[str, pl.DataFrame]:
    drugs = df.select(
        pl.col("id").alias("chembl_id"),
        pl.col("name").alias("name"),
        pl.col("drugType").alias("drug_type"),
        pl.col("canonicalSmiles").alias("canonical_smiles"),
        pl.col("inchiKey").alias("inchi_key"),
        pl.col("parentId").alias("parent_id"),
        pl.col("maximumClinicalStage").alias("maximum_clinical_stage"),
        pl.col("description").alias("description"),
        pl.col("tradeNames").alias("trade_names"),
        pl.col("synonyms").alias("synonyms"),
        pl.col("childChemblIds").alias("child_chembl_ids"),
    )

    # crossReferences: list of {source, ids[]} -> one row per (chembl_id, source, external id)
    cross_refs = pl.DataFrame()
    if "crossReferences" in df.columns:
        cr = (
            df.select(pl.col("id").alias("chembl_id"), pl.col("crossReferences"))
            .explode("crossReferences")
            .filter(pl.col("crossReferences").is_not_null())
            .unnest("crossReferences")
            .explode("ids")
            .filter(pl.col("ids").is_not_null())
        )
        cross_refs = cr.select(
            pl.col("chembl_id"),
            pl.col("source"),
            pl.col("ids").alias("ref_id"),
        )

    return {"drugs": drugs, "drugs_cross_references": cross_refs}


_DRUGS_DOC = {
    "chembl_id":              ColumnSpec(identifier=ChemblId, required=True, description="ChEMBL molecule ID"),
    "name":                   ColumnSpec(description="Preferred drug name"),
    "drug_type":              ColumnSpec(description="Molecule type (Small molecule, Antibody, Protein, ...)"),
    "canonical_smiles":       ColumnSpec(description="Canonical SMILES string"),
    "inchi_key":              ColumnSpec(description="Standard InChIKey"),
    "parent_id":              ColumnSpec(identifier=ChemblId, description="Parent molecule ChEMBL ID, if this is a child/salt form"),
    "maximum_clinical_stage": ColumnSpec(description="Highest clinical trial phase reached"),
    "description":            ColumnSpec(description="Free-text drug description"),
    "trade_names":            ColumnSpec(description="Trade names (list)"),
    "synonyms":               ColumnSpec(description="Synonyms (list)"),
    "child_chembl_ids":       ColumnSpec(description="Child molecule ChEMBL IDs (list)"),
}

_CROSS_REFS_DOC = {
    "chembl_id": ColumnSpec(identifier=ChemblId, required=True, description="ChEMBL molecule ID"),
    "source":    ColumnSpec(description="Cross-reference source database"),
    "ref_id":    ColumnSpec(description="Identifier of the molecule in the source database"),
}

TABLES_DOC = {
    "drugs": _DRUGS_DOC,
    "drugs_cross_references": _CROSS_REFS_DOC,
}


@register
class OpenTargetsDrugMolecule(OpenTargetsProductPipeline):
    name = "opentargets_drug_molecule"
    raw_dirname = "drug_molecule"
    transform_module = "lacuna_etl.datasets.opentargets.drug_molecule"
    first_table = "drugs"
    tables_doc = TABLES_DOC
