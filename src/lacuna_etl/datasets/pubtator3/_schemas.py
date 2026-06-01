"""Polars dtypes and column documentation for the PubTator3 ETL outputs.

`POLARS_SCHEMAS` is used at shard-write time to keep each shard's dtypes
consistent even when a column is all-null for a given batch. `TABLES_DOC` is
what gets serialised to the side-car yaml files alongside each output parquet.

Three tables, all keyed by PMID:
  articles    one row per document (article-level metadata, no body text)
  annotations one row per tagged bio-entity mention (with section + offset)
  relations   one row per extracted concept-concept relation
"""

from __future__ import annotations

import polars as pl

from lacuna_etl.core.identifiers import Doi, PubmedId
from lacuna_etl.core.schema import ColumnSpec


POLARS_SCHEMAS: dict[str, dict[str, pl.DataType]] = {
    "articles": {
        "pmid":          pl.Int64,
        "pmc_id":        pl.String,
        "doi":           pl.String,
        "year":          pl.Int32,
        "volume":        pl.String,
        "issue":         pl.String,
        "first_page":    pl.String,
        "last_page":     pl.String,
        "license":       pl.String,
        "has_full_text": pl.Boolean,
        "n_passages":    pl.Int32,
    },
    "annotations": {
        "pmid":          pl.Int64,
        "annotation_id": pl.String,
        "section_type":  pl.String,
        "passage_type":  pl.String,
        "offset":        pl.Int64,
        "length":        pl.Int64,
        "mention":       pl.String,
        "entity_type":   pl.String,
        "identifier":    pl.String,
    },
    "relations": {
        "pmid":             pl.Int64,
        "relation_id":      pl.String,
        "relation_type":    pl.String,
        "score":            pl.Float64,
        "role1_type":       pl.String,
        "role1_identifier": pl.String,
        "role2_type":       pl.String,
        "role2_identifier": pl.String,
    },
}


# Tables other than `articles` that are keyed by PMID. `articles` is the parent
# (one row per PMID); these are the per-PMID child tables.
CHILD_TABLES: tuple[str, ...] = ("annotations", "relations")


TABLES_DOC: dict[str, dict[str, ColumnSpec]] = {
    "articles": {
        "pmid":          ColumnSpec(identifier=PubmedId, required=True, description="PubMed identifier (the BioC document id)"),
        "pmc_id":        ColumnSpec(description="PubMed Central ID from article-id_pmc, e.g. PMC1234567; null for abstract-only records"),
        "doi":           ColumnSpec(identifier=Doi, description="DOI from the article-id_doi infon (URL prefix stripped); null when PubTator carries no clean DOI key, e.g. most abstract-only records"),
        "year":          ColumnSpec(description="Publication year from the front/title passage"),
        "volume":        ColumnSpec(description="Journal volume from the front/title passage"),
        "issue":         ColumnSpec(description="Journal issue from the front/title passage"),
        "first_page":    ColumnSpec(description="First page (fpage) from the front/title passage"),
        "last_page":     ColumnSpec(description="Last page (lpage) from the front/title passage"),
        "license":       ColumnSpec(description="License statement, present mainly for full-text PMC records"),
        "has_full_text": ColumnSpec(description="True when the document contains body paragraphs (PMC full text), False for abstract-only records"),
        "n_passages":    ColumnSpec(description="Number of BioC <passage> elements in the document"),
    },
    "annotations": {
        "pmid":          ColumnSpec(identifier=PubmedId, required=True, description="PubMed identifier of the article the mention occurs in"),
        "annotation_id": ColumnSpec(description="BioC annotation id, unique within the document"),
        "section_type":  ColumnSpec(description="Passage section_type infon, e.g. TITLE, ABSTRACT, INTRO, METHODS, RESULTS, DISCUSS, CONCL, FIG, TABLE"),
        "passage_type":  ColumnSpec(description="Passage type infon, e.g. front, title, abstract, paragraph, fig_caption, table. Annotations in reference (type='ref') passages are excluded"),
        "offset":        ColumnSpec(description="Absolute character offset of the mention within the document text"),
        "length":        ColumnSpec(description="Length of the mention in characters"),
        "mention":       ColumnSpec(description="Surface text of the tagged mention as it appears in the article"),
        "entity_type":   ColumnSpec(description="Annotated entity type, e.g. Gene, Disease, Chemical, Species, CellLine, SNP, DNAMutation, ProteinMutation, Chromosome"),
        "identifier":    ColumnSpec(description="Normalised concept identifier as emitted by PubTator, verbatim. Heterogeneous by type: MESH:/OMIM: (Disease, Chemical), bare NCBI taxon id (Species), bare NCBI Gene id (Gene), CVCL: (CellLine), packed tmVar: composite (variants). PubTator's '-' (no normalisation) is stored as null"),
    },
    "relations": {
        "pmid":             ColumnSpec(identifier=PubmedId, required=True, description="PubMed identifier of the article the relation was extracted from"),
        "relation_id":      ColumnSpec(description="BioC relation id, unique within the document, e.g. R1"),
        "relation_type":    ColumnSpec(description="Relation type, e.g. Association, Positive_Correlation, Negative_Correlation, Comparison, Cotreatment, Bind"),
        "score":            ColumnSpec(description="Confidence score reported by PubTator's relation extractor"),
        "role1_type":       ColumnSpec(description="Entity type of the first concept in the relation, e.g. Chemical"),
        "role1_identifier": ColumnSpec(description="Concept identifier of the first role, verbatim (same heterogeneous forms as annotations.identifier)"),
        "role2_type":       ColumnSpec(description="Entity type of the second concept in the relation"),
        "role2_identifier": ColumnSpec(description="Concept identifier of the second role, verbatim"),
    },
}
