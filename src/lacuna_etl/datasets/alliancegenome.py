import csv
import gzip
from pathlib import Path

import pandas as pd

from lacuna_etl.core.identifiers import AllianceGeneId, NcbiTaxId
from lacuna_etl.core.pipeline import DatasetPipeline
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.registry import register

# The Alliance of Genome Resources ships one download release (e.g. 9.0.0) holding
# many products, each as a directory of gzipped TSVs split by member database /
# species plus an all-species ``COMBINED`` rollup. We ingest the COMBINED rollup
# per product (one faithful one-row-per-source-line table); GENE-DESCRIPTION-TSV
# has no COMBINED file, so its per-species shards are concatenated.
#
# Every Alliance TSV starts with a ``#``-prefixed comment header block. Member-
# database gene identifiers are heterogeneous CURIEs (HGNC:, MGI:, SGD:,
# wormbase:WBGene…, ZFIN:, RGD:, FB:, Xenbase:, …) with no single canonical form,
# so gene/allele/variant/disease IDs are documented plain strings (the OpenTargets
# disease-ID precedent), required where they are a grain key. Species taxon
# columns are the one exception: they are uniformly ``NCBITaxon:<id>``, so the
# prefix is stripped and they are typed ``NcbiTaxId``. ``-`` and empty strings are
# the source null markers and are normalized to null (the MITAB ``-`` convention,
# matching NCBI tab files elsewhere in this repo).

# ---------------------------------------------------------------------------
# Orthology (ORTHOLOGY-ALLIANCE/COMBINED) -- cross-species ortholog gene pairs.
# ---------------------------------------------------------------------------
_ORTHOLOGY_RENAME = {
    "Gene1ID": "gene1_id",
    "Gene1Symbol": "gene1_symbol",
    "Gene1SpeciesTaxonID": "gene1_tax_id",
    "Gene1SpeciesName": "gene1_species_name",
    "Gene2ID": "gene2_id",
    "Gene2Symbol": "gene2_symbol",
    "Gene2SpeciesTaxonID": "gene2_tax_id",
    "Gene2SpeciesName": "gene2_species_name",
    "Algorithms": "algorithms",
    "AlgorithmsMatch": "algorithms_match",
    "OutOfAlgorithms": "out_of_algorithms",
    "IsBestScore": "is_best_score",
    "IsBestRevScore": "is_best_reverse_score",
}

_BEST_SCORE_VALUES = {"Yes", "No", "Yes_Adjusted"}

ORTHOLOGY_SCHEMA = {
    "gene1_id": ColumnSpec(identifier=AllianceGeneId, description="Subject gene identifier (Alliance gene curie, e.g. HGNC:5, MGI:1919304, WB:WBGene...)", required=True),
    "gene1_symbol": ColumnSpec(description="Subject gene symbol"),
    "gene1_tax_id": ColumnSpec(identifier=NcbiTaxId, description="NCBI taxonomy ID of the subject gene's species"),
    "gene1_species_name": ColumnSpec(description="Subject gene's species name"),
    "gene2_id": ColumnSpec(identifier=AllianceGeneId, description="Object (ortholog) gene identifier (Alliance gene curie)", required=True),
    "gene2_symbol": ColumnSpec(description="Object gene symbol"),
    "gene2_tax_id": ColumnSpec(identifier=NcbiTaxId, description="NCBI taxonomy ID of the object gene's species"),
    "gene2_species_name": ColumnSpec(description="Object gene's species name"),
    "algorithms": ColumnSpec(description="Pipe-delimited list of orthology-prediction algorithms calling this pair (e.g. InParanoid|SonicParanoid|OMA)"),
    "algorithms_match": ColumnSpec(description="Number of algorithms that called this ortholog pair"),
    "out_of_algorithms": ColumnSpec(description="Number of algorithms that compared the two genes"),
    "is_best_score": ColumnSpec(description="Whether gene2 is gene1's best-score match", allowed_values=_BEST_SCORE_VALUES),
    "is_best_reverse_score": ColumnSpec(description="Whether gene1 is gene2's best-score match (reverse direction)", allowed_values=_BEST_SCORE_VALUES),
}

# ---------------------------------------------------------------------------
# Disease associations (DISEASE-ALLIANCE/COMBINED).
# ---------------------------------------------------------------------------
_DISEASE_RENAME = {
    "Taxon": "tax_id",
    "SpeciesName": "species_name",
    "DBobjectType": "db_object_type",
    "DBObjectID": "db_object_id",
    "DBObjectSymbol": "db_object_symbol",
    "AssociationType": "association_type",
    "DOID": "do_id",
    "DOtermName": "do_term_name",
    "WithOrtholog": "with_ortholog",
    "InferredFromID": "inferred_from_id",
    "InferredFromSymbol": "inferred_from_symbol",
    "ExperimentalCondition": "experimental_condition",
    "Modifier": "modifier",
    "EvidenceCode": "evidence_code",
    "EvidenceCodeName": "evidence_code_name",
    "Reference": "reference",
    "Date": "date",
    "Source": "source",
}

DISEASE_SCHEMA = {
    "tax_id": ColumnSpec(identifier=NcbiTaxId, description="NCBI taxonomy ID of the annotated object's species"),
    "species_name": ColumnSpec(description="Species name"),
    "db_object_type": ColumnSpec(description="Type of the annotated object: gene, allele, or affected_genomic_model"),
    "db_object_id": ColumnSpec(description="Identifier of the annotated object (member-database CURIE)", required=True),
    "db_object_symbol": ColumnSpec(description="Symbol/name of the annotated object"),
    "association_type": ColumnSpec(description="Gene-to-disease relationship, e.g. is_implicated_in, is_model_of, is_marker_for"),
    "do_id": ColumnSpec(description="Disease Ontology term ID (DOID:...)", required=True),
    "do_term_name": ColumnSpec(description="Disease Ontology term name"),
    "with_ortholog": ColumnSpec(description="Ortholog(s) the annotation was projected through, when inferred via orthology"),
    "inferred_from_id": ColumnSpec(description="Identifier(s) the annotation was inferred from (e.g. the allele/model for a gene annotation)"),
    "inferred_from_symbol": ColumnSpec(description="Symbol(s) corresponding to inferred_from_id"),
    "experimental_condition": ColumnSpec(description="Experimental condition qualifying the annotation"),
    "modifier": ColumnSpec(description="Annotation modifier (e.g. ameliorated_by, induced_by)"),
    "evidence_code": ColumnSpec(description="Evidence & Conclusion Ontology code (ECO:...)"),
    "evidence_code_name": ColumnSpec(description="Human-readable evidence code name"),
    "reference": ColumnSpec(description="Supporting reference(s), e.g. PMID:... or member-database reference IDs"),
    "date": ColumnSpec(description="Date of the annotation (source YYYYMMDD string)"),
    "source": ColumnSpec(description="Contributing member database"),
}

# ---------------------------------------------------------------------------
# Expression (EXPRESSION-ALLIANCE/COMBINED).
# ---------------------------------------------------------------------------
_EXPRESSION_RENAME = {
    "Species": "species_name",
    "SpeciesID": "tax_id",
    "GeneID": "gene_id",
    "GeneSymbol": "gene_symbol",
    "Location": "location",
    "StageTerm": "stage_term",
    "AssayID": "assay_id",
    "AssayTermName": "assay_term_name",
    "CellularComponentID": "cellular_component_id",
    "CellularComponentTerm": "cellular_component_term",
    "CellularComponentQualifierIDs": "cellular_component_qualifier_ids",
    "CellularComponentQualifierTermNames": "cellular_component_qualifier_term_names",
    "SubStructureID": "substructure_id",
    "SubStructureName": "substructure_name",
    "SubStructureQualifierIDs": "substructure_qualifier_ids",
    "SubStructureQualifierTermNames": "substructure_qualifier_term_names",
    "AnatomyTermID": "anatomy_term_id",
    "AnatomyTermName": "anatomy_term_name",
    "AnatomyTermQualifierIDs": "anatomy_term_qualifier_ids",
    "AnatomyTermQualifierTermNames": "anatomy_term_qualifier_term_names",
    "SourceURL": "source_url",
    "Source": "source",
    "Reference": "reference",
}

EXPRESSION_SCHEMA = {
    "species_name": ColumnSpec(description="Species name"),
    "tax_id": ColumnSpec(identifier=NcbiTaxId, description="NCBI taxonomy ID of the species"),
    "gene_id": ColumnSpec(identifier=AllianceGeneId, description="Gene identifier (Alliance gene curie)", required=True),
    "gene_symbol": ColumnSpec(description="Gene symbol"),
    "location": ColumnSpec(description="Expression location as reported (free-text anatomy/cellular location)"),
    "stage_term": ColumnSpec(description="Developmental stage term"),
    "assay_id": ColumnSpec(description="Assay ontology ID (MMO:...)"),
    "assay_term_name": ColumnSpec(description="Assay ontology term name"),
    "cellular_component_id": ColumnSpec(description="GO cellular component ID"),
    "cellular_component_term": ColumnSpec(description="GO cellular component term name"),
    "cellular_component_qualifier_ids": ColumnSpec(description="Pipe-delimited cellular-component qualifier ontology IDs"),
    "cellular_component_qualifier_term_names": ColumnSpec(description="Pipe-delimited cellular-component qualifier term names"),
    "substructure_id": ColumnSpec(description="Anatomical substructure ontology ID"),
    "substructure_name": ColumnSpec(description="Anatomical substructure name"),
    "substructure_qualifier_ids": ColumnSpec(description="Pipe-delimited substructure qualifier ontology IDs"),
    "substructure_qualifier_term_names": ColumnSpec(description="Pipe-delimited substructure qualifier term names"),
    "anatomy_term_id": ColumnSpec(description="Anatomy ontology ID"),
    "anatomy_term_name": ColumnSpec(description="Anatomy ontology term name"),
    "anatomy_term_qualifier_ids": ColumnSpec(description="Pipe-delimited anatomy qualifier ontology IDs"),
    "anatomy_term_qualifier_term_names": ColumnSpec(description="Pipe-delimited anatomy qualifier term names"),
    "source_url": ColumnSpec(description="Member-database URL for the annotation"),
    "source": ColumnSpec(description="Contributing member database"),
    "reference": ColumnSpec(description="Comma-delimited supporting reference(s), e.g. PMID:..."),
}

# ---------------------------------------------------------------------------
# Variants / alleles (VARIANT-ALLELE/COMBINED).
# ---------------------------------------------------------------------------
_VARIANT_RENAME = {
    "Taxon": "tax_id",
    "SpeciesName": "species_name",
    "AlleleId": "allele_id",
    "AlleleSymbol": "allele_symbol",
    "AlleleSynonyms": "allele_synonyms",
    "VariantId": "variant_id",
    "VariantSymbol": "variant_symbol",
    "VariantSynonyms": "variant_synonyms",
    "VariantCrossReferences": "variant_cross_references",
    "AlleleAssociatedGeneId": "allele_associated_gene_id",
    "AlleleAssociatedGeneSymbol": "allele_associated_gene_symbol",
    "VariantAffectedGeneId": "variant_affected_gene_id",
    "VariantAffectedGeneSymbol": "variant_affected_gene_symbol",
    "Category": "category",
    "VariantsTypeId": "variant_type_id",
    "VariantsTypeName": "variant_type_name",
    "VariantsHgvsNames": "variant_hgvs_names",
    "Assembly": "assembly",
    "Chromosome": "chromosome",
    "StartPosition": "start_position",
    "EndPosition": "end_position",
    "SequenceOfReference": "reference_sequence",
    "SequenceOfVariant": "variant_sequence",
    "MostSevereConsequenceName": "most_severe_consequence_name",
    "VariantInformationReference": "variant_information_reference",
    "HasDiseaseAnnotations": "has_disease_annotations",
    "HasPhenotypeAnnotations": "has_phenotype_annotations",
}

# The annotation-presence flags use ``yes`` / ``-`` (not yes/no), so ``-`` is a
# real "false" value here and must be mapped before the generic ``-`` -> null pass.
_VARIANT_FLAG_MAP = {"yes": True, "-": False}

VARIANT_SCHEMA = {
    "tax_id": ColumnSpec(identifier=NcbiTaxId, description="NCBI taxonomy ID of the species"),
    "species_name": ColumnSpec(description="Species name"),
    "allele_id": ColumnSpec(description="Allele identifier (member-database CURIE)"),
    "allele_symbol": ColumnSpec(description="Allele symbol"),
    "allele_synonyms": ColumnSpec(description="Pipe-delimited allele synonyms"),
    "variant_id": ColumnSpec(description="Variant identifier (member-database CURIE)"),
    "variant_symbol": ColumnSpec(description="Variant symbol"),
    "variant_synonyms": ColumnSpec(description="Pipe-delimited variant synonyms"),
    "variant_cross_references": ColumnSpec(description="Pipe-delimited variant cross-reference IDs"),
    "allele_associated_gene_id": ColumnSpec(identifier=AllianceGeneId, description="Gene the allele is associated with (Alliance gene curie)"),
    "allele_associated_gene_symbol": ColumnSpec(description="Symbol of the allele-associated gene"),
    "variant_affected_gene_id": ColumnSpec(identifier=AllianceGeneId, description="Gene affected by the variant (Alliance gene curie)"),
    "variant_affected_gene_symbol": ColumnSpec(description="Symbol of the variant-affected gene"),
    "category": ColumnSpec(description="Record category, e.g. allele or variant"),
    "variant_type_id": ColumnSpec(description="Sequence Ontology variant-type ID (SO:...)"),
    "variant_type_name": ColumnSpec(description="Variant-type name"),
    "variant_hgvs_names": ColumnSpec(description="Pipe-delimited HGVS names for the variant"),
    "assembly": ColumnSpec(description="Genome assembly the coordinates refer to"),
    "chromosome": ColumnSpec(description="Chromosome"),
    "start_position": ColumnSpec(description="Start position on the assembly (1-based)"),
    "end_position": ColumnSpec(description="End position on the assembly"),
    "reference_sequence": ColumnSpec(description="Reference allele sequence"),
    "variant_sequence": ColumnSpec(description="Variant allele sequence"),
    "most_severe_consequence_name": ColumnSpec(description="Most severe predicted molecular consequence"),
    "variant_information_reference": ColumnSpec(description="Reference(s) for the variant information"),
    "has_disease_annotations": ColumnSpec(description="Whether the allele/variant carries disease annotations"),
    "has_phenotype_annotations": ColumnSpec(description="Whether the allele/variant carries phenotype annotations"),
}

# ---------------------------------------------------------------------------
# Gene descriptions (GENE-DESCRIPTION-TSV; per-species, headerless, concatenated).
# ---------------------------------------------------------------------------
_GENE_DESCRIPTION_NAMES = ["gene_id", "gene_symbol", "description"]

GENE_DESCRIPTION_SCHEMA = {
    "gene_id": ColumnSpec(identifier=AllianceGeneId, description="Gene identifier (Alliance gene curie)", required=True),
    "gene_symbol": ColumnSpec(description="Gene symbol"),
    "description": ColumnSpec(description="Automated gene description generated by the Alliance"),
}

# ---------------------------------------------------------------------------
# Gene cross-references (GENECROSSREFERENCE/COMBINED).
# ---------------------------------------------------------------------------
_GENE_XREF_RENAME = {
    "GeneID": "gene_id",
    "GlobalCrossReferenceID": "cross_reference_id",
    "CrossReferenceCompleteURL": "cross_reference_url",
    "ResourceDescriptorPage": "resource_descriptor_page",
    "TaxonID": "tax_id",
}

GENE_XREF_SCHEMA = {
    "gene_id": ColumnSpec(description="Alliance gene identifier (member-database CURIE)", required=True),
    "cross_reference_id": ColumnSpec(description="Cross-referenced external identifier (prefixed CURIE)", required=True),
    "cross_reference_url": ColumnSpec(description="Complete URL for the cross-reference"),
    "resource_descriptor_page": ColumnSpec(description="Resource-descriptor page type for the cross-reference"),
    "tax_id": ColumnSpec(identifier=NcbiTaxId, description="NCBI taxonomy ID of the gene's species"),
}

# ---------------------------------------------------------------------------
# UniProt cross-references (CROSSREFERENCEUNIPROT/COMBINED; headerless, 2 columns).
# ---------------------------------------------------------------------------
_UNIPROT_XREF_NAMES = ["uniprot_id", "cross_reference_id"]

UNIPROT_XREF_SCHEMA = {
    "uniprot_id": ColumnSpec(description="UniProtKB identifier (incl. chain/PRO suffixes), prefixed UniProtKB:", required=True),
    "cross_reference_id": ColumnSpec(description="Cross-referenced identifier (prefixed CURIE, e.g. RefSeq:...)", required=True),
}

# ---------------------------------------------------------------------------
# Interactions (INTERACTION-GEN / INTERACTION-MOL COMBINED): PSI-MITAB 2.7, 42
# columns, headerless data (the column header is in the ``#`` comment block).
# Fields are heterogeneous controlled-vocabulary CURIEs (psi-mi:"MI:nnnn"(label),
# pipe-delimited lists), so all are documented plain strings; ``Negative`` is a
# true boolean.
# ---------------------------------------------------------------------------
_MITAB_NAMES = [
    "interactor_a_id",
    "interactor_b_id",
    "interactor_a_alt_ids",
    "interactor_b_alt_ids",
    "interactor_a_aliases",
    "interactor_b_aliases",
    "detection_methods",
    "publication_first_authors",
    "publication_ids",
    "interactor_a_taxid",
    "interactor_b_taxid",
    "interaction_types",
    "source_databases",
    "interaction_ids",
    "confidence_values",
    "expansion_methods",
    "interactor_a_biological_roles",
    "interactor_b_biological_roles",
    "interactor_a_experimental_roles",
    "interactor_b_experimental_roles",
    "interactor_a_types",
    "interactor_b_types",
    "interactor_a_xrefs",
    "interactor_b_xrefs",
    "interaction_xrefs",
    "interactor_a_annotations",
    "interactor_b_annotations",
    "interaction_annotations",
    "host_organisms",
    "interaction_parameters",
    "creation_date",
    "update_date",
    "interactor_a_checksums",
    "interactor_b_checksums",
    "interaction_checksums",
    "negative",
    "interactor_a_features",
    "interactor_b_features",
    "interactor_a_stoichiometry",
    "interactor_b_stoichiometry",
    "participant_a_identification_methods",
    "participant_b_identification_methods",
]

_NEGATIVE_MAP = {"true": True, "false": False}


def _mitab_schema() -> dict[str, ColumnSpec]:
    """PSI-MITAB 2.7 column contract shared by both interaction tables."""
    descriptions = {
        "interactor_a_id": "Primary identifier of interactor A (member-database CURIE), required",
        "interactor_b_id": "Primary identifier of interactor B (member-database CURIE), required",
        "interactor_a_alt_ids": "Pipe-delimited alternative identifiers of interactor A",
        "interactor_b_alt_ids": "Pipe-delimited alternative identifiers of interactor B",
        "interactor_a_aliases": "Pipe-delimited aliases of interactor A",
        "interactor_b_aliases": "Pipe-delimited aliases of interactor B",
        "detection_methods": "Interaction detection method(s) (psi-mi MI codes)",
        "publication_first_authors": "Publication first author(s)",
        "publication_ids": "Publication identifier(s), e.g. pubmed:...",
        "interactor_a_taxid": "NCBI taxid of interactor A (MITAB taxid:nnnn(label) form)",
        "interactor_b_taxid": "NCBI taxid of interactor B (MITAB taxid:nnnn(label) form)",
        "interaction_types": "Interaction type(s) (psi-mi MI codes)",
        "source_databases": "Source database(s) (psi-mi MI codes)",
        "interaction_ids": "Interaction identifier(s) in the source database",
        "confidence_values": "Confidence value(s)",
        "expansion_methods": "Complex-expansion method(s)",
        "interactor_a_biological_roles": "Biological role(s) of interactor A",
        "interactor_b_biological_roles": "Biological role(s) of interactor B",
        "interactor_a_experimental_roles": "Experimental role(s) of interactor A (e.g. bait/prey)",
        "interactor_b_experimental_roles": "Experimental role(s) of interactor B (e.g. bait/prey)",
        "interactor_a_types": "Molecular type(s) of interactor A (e.g. protein, gene)",
        "interactor_b_types": "Molecular type(s) of interactor B",
        "interactor_a_xrefs": "Cross-reference(s) for interactor A",
        "interactor_b_xrefs": "Cross-reference(s) for interactor B",
        "interaction_xrefs": "Cross-reference(s) for the interaction",
        "interactor_a_annotations": "Annotation(s) for interactor A",
        "interactor_b_annotations": "Annotation(s) for interactor B",
        "interaction_annotations": "Annotation(s) for the interaction",
        "host_organisms": "Host organism(s) the interaction was observed in",
        "interaction_parameters": "Interaction parameter(s)",
        "creation_date": "Creation date of the record",
        "update_date": "Last update date of the record",
        "interactor_a_checksums": "Checksum(s) for interactor A",
        "interactor_b_checksums": "Checksum(s) for interactor B",
        "interaction_checksums": "Checksum(s) for the interaction",
        "negative": "Whether this is a negative (non-interaction) result",
        "interactor_a_features": "Feature(s) of interactor A",
        "interactor_b_features": "Feature(s) of interactor B",
        "interactor_a_stoichiometry": "Stoichiometry of interactor A",
        "interactor_b_stoichiometry": "Stoichiometry of interactor B",
        "participant_a_identification_methods": "Participant identification method for A",
        "participant_b_identification_methods": "Participant identification method for B",
    }
    schema = {
        name: ColumnSpec(
            description=descriptions[name],
            required=name in ("interactor_a_id", "interactor_b_id"),
        )
        for name in _MITAB_NAMES
    }
    return schema


MITAB_SCHEMA = _mitab_schema()


def _count_comment_lines(path: Path) -> int:
    """Number of leading ``#``-prefixed comment lines (the Alliance header block)."""
    n = 0
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        for line in fh:
            if line.startswith("#"):
                n += 1
            else:
                break
    return n


def _read_alliance(
    path: Path,
    names: list[str] | None = None,
    quoting: int = csv.QUOTE_NONE,
) -> pd.DataFrame:
    """Read one Alliance gzipped TSV as all-string columns.

    Skips the leading ``#`` comment block. ``names`` is given for headerless
    products (and for PSI-MITAB, whose header lives inside the comment block);
    otherwise the first non-comment line is the header. Most products carry bare
    ``"`` characters as data (psi-mi CURIEs), so quoting is disabled by default;
    the variant report instead uses real CSV quoting to wrap the rare symbol that
    contains a literal tab, so it passes ``QUOTE_MINIMAL``.
    """
    skip = _count_comment_lines(path)
    return pd.read_csv(
        path,
        sep="\t",
        dtype=str,
        compression="gzip",
        skiprows=skip,
        names=names,
        header=None if names is not None else 0,
        quoting=quoting,
        keep_default_na=False,
        na_filter=False,
        skip_blank_lines=True,
        encoding="utf-8",
    )


def _strip_taxon(s: pd.Series) -> pd.Series:
    """``NCBITaxon:9606`` -> ``9606`` as nullable Int64 (via the NcbiTaxId type)."""
    stripped = s.str.strip().str.replace(r"^NCBITaxon:", "", regex=True)
    stripped = stripped.mask(stripped == "", pd.NA)
    return NcbiTaxId.cast(stripped)


def _to_int(s: pd.Series) -> pd.Series:
    cleaned = s.str.strip().mask(s.str.strip().isin(["", "-"]), pd.NA)
    return pd.to_numeric(cleaned, errors="raise").astype("Int64")


def _to_bool(
    s: pd.Series,
    mapping: dict[str, bool],
    *,
    casefold: bool = False,
    null_tokens: tuple[str, ...] = ("",),
) -> pd.Series:
    stripped = s.str.strip()
    if casefold:
        stripped = stripped.str.lower()
    is_null = stripped.isin(null_tokens)
    unexpected = stripped[~stripped.isin(mapping) & ~is_null].unique()
    if len(unexpected):
        raise ValueError(f"{s.name}: unexpected boolean values {list(unexpected[:10])}")
    out = stripped.map(mapping).mask(is_null, pd.NA)
    return out.astype("boolean")


def _null_strings(df: pd.DataFrame, skip: set[str]) -> None:
    """In place: strip whitespace and map ``""`` / ``-`` to null for string columns.

    ``skip`` lists columns already cast to a non-string dtype (taxon, ints,
    booleans), where ``-`` may be a real value, so they must not be touched.
    """
    for col in df.columns:
        if col in skip:
            continue
        s = df[col].str.strip()
        df[col] = s.mask(s.isin(["", "-"]), pd.NA)


@register
class AllianceGenome(DatasetPipeline):
    name = "alliancegenome"

    # (intermediate stem, output table, schema) for the generic load loop.
    _TABLES = [
        ("orthology", ORTHOLOGY_SCHEMA),
        ("disease_associations", DISEASE_SCHEMA),
        ("expression", EXPRESSION_SCHEMA),
        ("variant_alleles", VARIANT_SCHEMA),
        ("gene_descriptions", GENE_DESCRIPTION_SCHEMA),
        ("gene_cross_references", GENE_XREF_SCHEMA),
        ("uniprot_cross_references", UNIPROT_XREF_SCHEMA),
        ("genetic_interactions", MITAB_SCHEMA),
        ("molecular_interactions", MITAB_SCHEMA),
    ]

    def _combined(self, *parts: str) -> Path:
        """Resolve the single ``*.tsv.gz`` under ``<release>/<parts...>/COMBINED``."""
        base = self.raw_path().joinpath(*parts)
        files = sorted(base.glob("*.tsv.gz"))
        if len(files) != 1:
            raise FileNotFoundError(f"expected exactly one COMBINED file under {base}, found {len(files)}")
        return files[0]

    def extract(self) -> None:
        self._extract_orthology()
        self._extract_disease()
        self._extract_expression()
        self._extract_variant_alleles()
        self._extract_gene_descriptions()
        self._extract_gene_cross_references()
        self._extract_uniprot_cross_references()
        self._extract_interactions("INTERACTION-GEN", "genetic_interactions")
        self._extract_interactions("INTERACTION-MOL", "molecular_interactions")

    def transform(self) -> None:
        # Each table is a faithful one-row-per-source-line projection; all casting
        # and validation happens in extract via apply_schema. Nothing to reconcile.
        pass

    def load(self) -> None:
        for stem, schema in self._TABLES:
            df = self.load_parquet(self.intermediate_path() / f"{stem}.parquet")
            self.save_parquet(df, self.output_path() / f"{stem}.parquet")
            self.save_schema_yaml(schema, stem)

    # -- per-table extracts ------------------------------------------------

    def _extract_orthology(self) -> None:
        df = _read_alliance(self._combined("ORTHOLOGY-ALLIANCE", "COMBINED"))
        df = df.rename(columns=_ORTHOLOGY_RENAME)[list(ORTHOLOGY_SCHEMA)]
        special = {"gene1_tax_id", "gene2_tax_id", "algorithms_match", "out_of_algorithms"}
        _null_strings(df, special)
        df["gene1_tax_id"] = _strip_taxon(df["gene1_tax_id"])
        df["gene2_tax_id"] = _strip_taxon(df["gene2_tax_id"])
        df["algorithms_match"] = _to_int(df["algorithms_match"])
        df["out_of_algorithms"] = _to_int(df["out_of_algorithms"])
        df = self.apply_schema(df, ORTHOLOGY_SCHEMA)
        self.save_parquet(df, self.intermediate_path() / "orthology.parquet")

    def _extract_disease(self) -> None:
        df = _read_alliance(self._combined("DISEASE-ALLIANCE", "COMBINED"))
        df = df.rename(columns=_DISEASE_RENAME)[list(DISEASE_SCHEMA)]
        _null_strings(df, {"tax_id"})
        df["tax_id"] = _strip_taxon(df["tax_id"])
        df = self.apply_schema(df, DISEASE_SCHEMA)
        self.save_parquet(df, self.intermediate_path() / "disease_associations.parquet")

    def _extract_expression(self) -> None:
        df = _read_alliance(self._combined("EXPRESSION-ALLIANCE", "COMBINED"))
        df = df.rename(columns=_EXPRESSION_RENAME)[list(EXPRESSION_SCHEMA)]
        _null_strings(df, {"tax_id"})
        df["tax_id"] = _strip_taxon(df["tax_id"])
        df = self.apply_schema(df, EXPRESSION_SCHEMA)
        self.save_parquet(df, self.intermediate_path() / "expression.parquet")

    def _extract_variant_alleles(self) -> None:
        # The VARIANT-ALLELE COMBINED rollup lives under a nested legacy version dir.
        df = _read_alliance(self._combined("4.0.0", "VARIANT-ALLELE", "COMBINED"), quoting=csv.QUOTE_MINIMAL)
        df = df.rename(columns=_VARIANT_RENAME)[list(VARIANT_SCHEMA)]
        special = {
            "tax_id", "start_position", "end_position",
            "has_disease_annotations", "has_phenotype_annotations",
        }
        # Map the yes/`-` flags before the generic `-` -> null pass treats `-` as null.
        df["has_disease_annotations"] = _to_bool(df["has_disease_annotations"], _VARIANT_FLAG_MAP)
        df["has_phenotype_annotations"] = _to_bool(df["has_phenotype_annotations"], _VARIANT_FLAG_MAP)
        _null_strings(df, special)
        df["tax_id"] = _strip_taxon(df["tax_id"])
        df["start_position"] = _to_int(df["start_position"])
        df["end_position"] = _to_int(df["end_position"])
        df = self.apply_schema(df, VARIANT_SCHEMA)
        self.save_parquet(df, self.intermediate_path() / "variant_alleles.parquet")

    def _extract_gene_descriptions(self) -> None:
        # No COMBINED file: concatenate the per-species (headerless) shards.
        base = self.raw_path() / "GENE-DESCRIPTION-TSV"
        files = sorted(base.glob("*/*.tsv.gz"))
        if not files:
            raise FileNotFoundError(f"no GENE-DESCRIPTION-TSV shards under {base}")
        frames = [_read_alliance(f, names=_GENE_DESCRIPTION_NAMES) for f in files]
        df = pd.concat(frames, ignore_index=True)
        _null_strings(df, set())
        df = self.apply_schema(df, GENE_DESCRIPTION_SCHEMA)
        self.save_parquet(df, self.intermediate_path() / "gene_descriptions.parquet")

    def _extract_gene_cross_references(self) -> None:
        df = _read_alliance(self._combined("GENECROSSREFERENCE", "COMBINED"))
        df = df.rename(columns=_GENE_XREF_RENAME)[list(GENE_XREF_SCHEMA)]
        _null_strings(df, {"tax_id"})
        df["tax_id"] = _strip_taxon(df["tax_id"])
        df = self.apply_schema(df, GENE_XREF_SCHEMA)
        self.save_parquet(df, self.intermediate_path() / "gene_cross_references.parquet")

    def _extract_uniprot_cross_references(self) -> None:
        df = _read_alliance(self._combined("CROSSREFERENCEUNIPROT", "COMBINED"), names=_UNIPROT_XREF_NAMES)
        _null_strings(df, set())
        df = self.apply_schema(df, UNIPROT_XREF_SCHEMA)
        self.save_parquet(df, self.intermediate_path() / "uniprot_cross_references.parquet")

    def _extract_interactions(self, product: str, stem: str) -> None:
        df = _read_alliance(self._combined(product, "COMBINED"), names=_MITAB_NAMES)
        # MITAB Negative is true/false but the Alliance also emits `-` (unspecified)
        # and upper-case variants; treat `-` as null, normalize case.
        df["negative"] = _to_bool(df["negative"], _NEGATIVE_MAP, casefold=True, null_tokens=("", "-"))
        _null_strings(df, {"negative"})
        df = self.apply_schema(df, MITAB_SCHEMA)
        self.save_parquet(df, self.intermediate_path() / f"{stem}.parquet")
