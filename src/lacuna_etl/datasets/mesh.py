"""Medical Subject Headings (MeSH), the NLM controlled vocabulary.

The NLM ships MeSH as four XML files per yearly release:

- ``desc<year>.gz``  - DescriptorRecordSet: main headings (the MeSH tree).
- ``qual<year>.xml`` - QualifierRecordSet: subheadings (e.g. ``/metabolism``).
- ``supp<year>.gz``  - SupplementalRecordSet: Supplementary Concept Records
  (chemicals, diseases, protocols, organisms) mapped onto descriptors.
- ``pa<year>.xml``   - PharmacologicalActionSet: descriptor -> substance lists.

All four fit in memory, so this is an in-memory pandas pipeline that walks each
file once with ``iterparse``. ``extract`` parses every record into per-table row
lists and writes one intermediate Parquet per table; ``transform`` is a no-op
(faithful projection); ``load`` validates against each ``SCHEMA`` and writes final
Parquet + sidecar.

Every record type (descriptor / qualifier / supplemental) carries the same
``ConceptList`` substructure - concepts, their entry terms (synonyms), concept
relations, and related registry numbers - so those four are emitted as **shared**
tables keyed by ``record_ui`` with a ``record_type`` discriminator, rather than
duplicated per record type. The record-type-specific lists (tree numbers,
allowable qualifiers, heading mappings, ...) are their own child tables keyed by
the parent UI. See docs/DESIGN.md for the full table inventory.
"""
from __future__ import annotations

import gzip
import xml.etree.ElementTree as ET
from pathlib import Path

import pandas as pd

from lacuna_etl.core.identifiers import (
    MeshConceptId,
    MeshDescriptorId,
    MeshQualifierId,
    MeshSupplementalId,
    MeshTermId,
)
from lacuna_etl.core.pipeline import DatasetPipeline
from lacuna_etl.core.schema import ColumnSpec
from lacuna_etl.datasets.registry import register

_RECORD_TYPES = {"descriptor", "qualifier", "supplemental"}

# --------------------------------------------------------------------------
# Shared concept substructure (present in descriptors, qualifiers, and SCRs).
# --------------------------------------------------------------------------
CONCEPTS_SCHEMA = {
    "record_ui": ColumnSpec(description="UI of the owning record (descriptor D…, qualifier Q…, or supplemental C…)", required=True),
    "record_type": ColumnSpec(allowed_values=_RECORD_TYPES, required=True, description="Type of the owning record: descriptor, qualifier, or supplemental"),
    "concept_ui": ColumnSpec(identifier=MeshConceptId, required=True, description="MeSH concept UI"),
    "is_preferred_concept": ColumnSpec(description="Whether this is the record's preferred concept"),
    "name": ColumnSpec(description="Concept name"),
    "scope_note": ColumnSpec(description="Free-text definition/scope note for the concept"),
    "registry_number": ColumnSpec(description="Registry number (CAS number or FDA UNII; '0' when none)"),
    "casn1_name": ColumnSpec(description="Chemical Abstracts Service Name (CASN1) when present"),
}

CONCEPT_TERMS_SCHEMA = {
    "record_ui": ColumnSpec(description="UI of the owning record", required=True),
    "concept_ui": ColumnSpec(identifier=MeshConceptId, required=True, description="MeSH concept UI the term belongs to"),
    "term_ui": ColumnSpec(identifier=MeshTermId, required=True, description="MeSH term UI"),
    "term": ColumnSpec(description="Term string (an entry term / synonym)"),
    "is_concept_preferred": ColumnSpec(description="Whether this term is the concept's preferred term"),
    "is_record_preferred": ColumnSpec(description="Whether this term is the whole record's preferred term (the heading name)"),
    "is_permuted": ColumnSpec(description="Whether this is a permuted (rotated) form of another term"),
    "lexical_tag": ColumnSpec(description="Lexical category tag, e.g. NON, ABB, ABX, ACR, EPO, LAB, NAM, TRD"),
    "entry_version": ColumnSpec(description="Abbreviated entry version of the term, when given"),
    "thesaurus_ids": ColumnSpec(description="Pipe-delimited source thesaurus IDs the term came from (e.g. 'NLM (1975)')"),
    "date_created": ColumnSpec(description="Date the term was created"),
}

CONCEPT_RELATIONS_SCHEMA = {
    "record_ui": ColumnSpec(description="UI of the owning record", required=True),
    "concept1_ui": ColumnSpec(identifier=MeshConceptId, required=True, description="First concept UI in the relation"),
    "concept2_ui": ColumnSpec(identifier=MeshConceptId, required=True, description="Second concept UI in the relation"),
    "relation_name": ColumnSpec(description="Relation type: NRW (narrower), BRD (broader), or REL (related)"),
}

CONCEPT_RELATED_REGISTRY_NUMBERS_SCHEMA = {
    "record_ui": ColumnSpec(description="UI of the owning record", required=True),
    "concept_ui": ColumnSpec(identifier=MeshConceptId, required=True, description="MeSH concept UI"),
    "related_registry_number": ColumnSpec(description="A related registry number for the concept"),
}

# --------------------------------------------------------------------------
# Descriptors (main headings).
# --------------------------------------------------------------------------
DESCRIPTORS_SCHEMA = {
    "descriptor_ui": ColumnSpec(identifier=MeshDescriptorId, required=True, description="MeSH descriptor UI"),
    "name": ColumnSpec(description="Descriptor (main heading) name"),
    "descriptor_class": ColumnSpec(description="Descriptor class: 1 (topical), 2 (publication type), 3 (check tag/geographic), 4 (geographic)"),
    "date_introduced": ColumnSpec(description="Date the descriptor was introduced into MeSH"),
    "last_updated": ColumnSpec(description="Date the descriptor record was last updated"),
    "annotation": ColumnSpec(description="Indexer annotation / usage note"),
    "history_note": ColumnSpec(description="History note (year introduced and prior treatment)"),
    "online_note": ColumnSpec(description="Online search note"),
    "public_mesh_note": ColumnSpec(description="Public MeSH note"),
    "nlm_classification_number": ColumnSpec(description="NLM Classification number"),
    "consider_also": ColumnSpec(description="'Consider also' cross-reference note"),
}

DESCRIPTOR_TREE_NUMBERS_SCHEMA = {
    "descriptor_ui": ColumnSpec(identifier=MeshDescriptorId, required=True, description="MeSH descriptor UI"),
    "tree_number": ColumnSpec(description="A position of the descriptor in the MeSH tree, e.g. 'D02.355.291.933.125'", required=True),
}

DESCRIPTOR_ALLOWABLE_QUALIFIERS_SCHEMA = {
    "descriptor_ui": ColumnSpec(identifier=MeshDescriptorId, required=True, description="MeSH descriptor UI"),
    "qualifier_ui": ColumnSpec(identifier=MeshQualifierId, required=True, description="UI of a qualifier allowable with this descriptor"),
    "abbreviation": ColumnSpec(description="Two-letter qualifier abbreviation"),
}

DESCRIPTOR_PHARMACOLOGICAL_ACTIONS_SCHEMA = {
    "descriptor_ui": ColumnSpec(identifier=MeshDescriptorId, required=True, description="MeSH descriptor UI (the substance)"),
    "action_descriptor_ui": ColumnSpec(identifier=MeshDescriptorId, required=True, description="Descriptor UI of a pharmacological action of this substance"),
}

DESCRIPTOR_PREVIOUS_INDEXING_SCHEMA = {
    "descriptor_ui": ColumnSpec(identifier=MeshDescriptorId, required=True, description="MeSH descriptor UI"),
    "previous_indexing": ColumnSpec(description="A heading under which the concept was previously indexed", required=True),
}

DESCRIPTOR_SEE_RELATED_SCHEMA = {
    "descriptor_ui": ColumnSpec(identifier=MeshDescriptorId, required=True, description="MeSH descriptor UI"),
    "see_related_descriptor_ui": ColumnSpec(identifier=MeshDescriptorId, required=True, description="UI of a 'see related' cross-referenced descriptor"),
}

DESCRIPTOR_ENTRY_COMBINATIONS_SCHEMA = {
    "descriptor_ui": ColumnSpec(identifier=MeshDescriptorId, required=True, description="MeSH descriptor UI"),
    "ecin_descriptor_ui": ColumnSpec(identifier=MeshDescriptorId, description="Entry-combination IN: descriptor the indexer types"),
    "ecin_qualifier_ui": ColumnSpec(identifier=MeshQualifierId, description="Entry-combination IN: qualifier the indexer types"),
    "ecout_descriptor_ui": ColumnSpec(identifier=MeshDescriptorId, description="Entry-combination OUT: descriptor it maps to"),
    "ecout_qualifier_ui": ColumnSpec(identifier=MeshQualifierId, description="Entry-combination OUT: qualifier it maps to"),
}

# --------------------------------------------------------------------------
# Qualifiers (subheadings).
# --------------------------------------------------------------------------
QUALIFIERS_SCHEMA = {
    "qualifier_ui": ColumnSpec(identifier=MeshQualifierId, required=True, description="MeSH qualifier UI"),
    "name": ColumnSpec(description="Qualifier (subheading) name"),
    "abbreviation": ColumnSpec(description="Two-letter qualifier abbreviation"),
    "date_introduced": ColumnSpec(description="Date the qualifier was introduced"),
    "last_updated": ColumnSpec(description="Date the qualifier record was last updated"),
    "annotation": ColumnSpec(description="Indexer annotation / usage note"),
    "history_note": ColumnSpec(description="History note"),
    "online_note": ColumnSpec(description="Online search note"),
}

QUALIFIER_TREE_NUMBERS_SCHEMA = {
    "qualifier_ui": ColumnSpec(identifier=MeshQualifierId, required=True, description="MeSH qualifier UI"),
    "tree_number": ColumnSpec(description="Position of the qualifier in the qualifier hierarchy", required=True),
}

# --------------------------------------------------------------------------
# Supplemental Concept Records (SCRs).
# --------------------------------------------------------------------------
SUPPLEMENTAL_RECORDS_SCHEMA = {
    "scr_ui": ColumnSpec(identifier=MeshSupplementalId, required=True, description="MeSH Supplementary Concept Record UI"),
    "name": ColumnSpec(description="SCR name (the substance / concept name)"),
    "scr_class": ColumnSpec(description="SCR class: 1 (chemical), 2 (protocol), 3 (rare disease), 4 (organism)"),
    "date_introduced": ColumnSpec(description="Date the SCR was introduced"),
    "last_updated": ColumnSpec(description="Date the SCR record was last updated"),
    "note": ColumnSpec(description="Free-text note"),
    "frequency": ColumnSpec(description="Number of times the SCR has been used for indexing"),
}

SUPPLEMENTAL_HEADING_MAPPED_TO_SCHEMA = {
    "scr_ui": ColumnSpec(identifier=MeshSupplementalId, required=True, description="MeSH SCR UI"),
    "descriptor_ui": ColumnSpec(identifier=MeshDescriptorId, required=True, description="Descriptor the SCR is mapped to for indexing"),
    "qualifier_ui": ColumnSpec(identifier=MeshQualifierId, description="Qualifier paired with the descriptor in the mapping, if any"),
    "is_primary": ColumnSpec(description="Whether this is the primary mapping (the source '*' marker)"),
}

SUPPLEMENTAL_INDEXING_INFORMATION_SCHEMA = {
    "scr_ui": ColumnSpec(identifier=MeshSupplementalId, required=True, description="MeSH SCR UI"),
    # Some MeSH IndexingInformation entries carry no DescriptorReferredTo, so this is nullable.
    "descriptor_ui": ColumnSpec(identifier=MeshDescriptorId, description="Descriptor carrying additional indexing information for the SCR (nullable: some entries reference no descriptor)"),
    "qualifier_ui": ColumnSpec(identifier=MeshQualifierId, description="Qualifier paired with the descriptor, if any"),
}

SUPPLEMENTAL_PHARMACOLOGICAL_ACTIONS_SCHEMA = {
    "scr_ui": ColumnSpec(identifier=MeshSupplementalId, required=True, description="MeSH SCR UI"),
    "action_descriptor_ui": ColumnSpec(identifier=MeshDescriptorId, required=True, description="Descriptor UI of a pharmacological action of this substance"),
}

SUPPLEMENTAL_PREVIOUS_INDEXING_SCHEMA = {
    "scr_ui": ColumnSpec(identifier=MeshSupplementalId, required=True, description="MeSH SCR UI"),
    "previous_indexing": ColumnSpec(description="A heading under which the concept was previously indexed", required=True),
}

SUPPLEMENTAL_SOURCES_SCHEMA = {
    "scr_ui": ColumnSpec(identifier=MeshSupplementalId, required=True, description="MeSH SCR UI"),
    "source": ColumnSpec(description="Citation/source where the substance was reported (nullable: a few MeSH Source entries are empty)"),
}

# --------------------------------------------------------------------------
# Pharmacological actions (descriptor -> substance lists).
# --------------------------------------------------------------------------
PHARMACOLOGICAL_ACTIONS_SCHEMA = {
    "action_descriptor_ui": ColumnSpec(identifier=MeshDescriptorId, required=True, description="Descriptor UI of the pharmacological action"),
    "substance_ui": ColumnSpec(description="UI of a substance with this action (descriptor D… or supplemental C…)", required=True),
    "substance_name": ColumnSpec(description="Substance name"),
}


def _text(node: ET.Element | None) -> str | None:
    if node is None or node.text is None:
        return None
    return node.text.strip() or None


def _date(node: ET.Element | None) -> pd.Timestamp:
    """Build a Timestamp from a MeSH <…><Year/Month/Day> date node, else NaT."""
    if node is None:
        return pd.NaT
    y = _text(node.find("Year"))
    if not y:
        return pd.NaT
    try:
        return pd.Timestamp(int(y), int(_text(node.find("Month")) or 1), int(_text(node.find("Day")) or 1))
    except (ValueError, TypeError):
        return pd.NaT


def _yn(el: ET.Element, attr: str) -> bool:
    return el.get(attr) == "Y"


def _strip_star(ui: str | None) -> tuple[str | None, bool]:
    """SCR heading targets prefix the primary mapping's UI with '*'."""
    if ui and ui.startswith("*"):
        return ui[1:], True
    return ui, False


def _open(path: Path):
    return gzip.open(path, "rb") if path.suffix == ".gz" else open(path, "rb")


@register
class Mesh(DatasetPipeline):
    name = "mesh"

    _TABLES = [
        ("descriptors", DESCRIPTORS_SCHEMA),
        ("descriptor_tree_numbers", DESCRIPTOR_TREE_NUMBERS_SCHEMA),
        ("descriptor_allowable_qualifiers", DESCRIPTOR_ALLOWABLE_QUALIFIERS_SCHEMA),
        ("descriptor_pharmacological_actions", DESCRIPTOR_PHARMACOLOGICAL_ACTIONS_SCHEMA),
        ("descriptor_previous_indexing", DESCRIPTOR_PREVIOUS_INDEXING_SCHEMA),
        ("descriptor_see_related", DESCRIPTOR_SEE_RELATED_SCHEMA),
        ("descriptor_entry_combinations", DESCRIPTOR_ENTRY_COMBINATIONS_SCHEMA),
        ("qualifiers", QUALIFIERS_SCHEMA),
        ("qualifier_tree_numbers", QUALIFIER_TREE_NUMBERS_SCHEMA),
        ("supplemental_records", SUPPLEMENTAL_RECORDS_SCHEMA),
        ("supplemental_heading_mapped_to", SUPPLEMENTAL_HEADING_MAPPED_TO_SCHEMA),
        ("supplemental_indexing_information", SUPPLEMENTAL_INDEXING_INFORMATION_SCHEMA),
        ("supplemental_pharmacological_actions", SUPPLEMENTAL_PHARMACOLOGICAL_ACTIONS_SCHEMA),
        ("supplemental_previous_indexing", SUPPLEMENTAL_PREVIOUS_INDEXING_SCHEMA),
        ("supplemental_sources", SUPPLEMENTAL_SOURCES_SCHEMA),
        ("pharmacological_actions", PHARMACOLOGICAL_ACTIONS_SCHEMA),
        ("concepts", CONCEPTS_SCHEMA),
        ("concept_terms", CONCEPT_TERMS_SCHEMA),
        ("concept_relations", CONCEPT_RELATIONS_SCHEMA),
        ("concept_related_registry_numbers", CONCEPT_RELATED_REGISTRY_NUMBERS_SCHEMA),
    ]

    def _file(self, prefix: str) -> Path:
        matches = sorted(self.raw_path().glob(f"{prefix}*"))
        matches = [m for m in matches if m.suffix in (".gz", ".xml")]
        if len(matches) != 1:
            raise FileNotFoundError(f"expected exactly one {prefix}* file under {self.raw_path()}, found {len(matches)}")
        return matches[0]

    # -- shared concept parsing --------------------------------------------

    def _parse_concepts(self, record: ET.Element, record_ui: str, record_type: str) -> None:
        cl = record.find("ConceptList")
        if cl is None:
            return
        for c in cl.findall("Concept"):
            cui = _text(c.find("ConceptUI"))
            self._concepts.append({
                "record_ui": record_ui,
                "record_type": record_type,
                "concept_ui": cui,
                "is_preferred_concept": _yn(c, "PreferredConceptYN"),
                "name": _text(c.find("ConceptName/String")),
                "scope_note": _text(c.find("ScopeNote")),
                "registry_number": _text(c.find("RegistryNumberList/RegistryNumber")),
                "casn1_name": _text(c.find("CASN1Name")),
            })
            for rrn in c.findall("RelatedRegistryNumberList/RelatedRegistryNumber"):
                self._related_rn.append({"record_ui": record_ui, "concept_ui": cui, "related_registry_number": _text(rrn)})
            for cr in c.findall("ConceptRelationList/ConceptRelation"):
                self._relations.append({
                    "record_ui": record_ui,
                    "concept1_ui": _text(cr.find("Concept1UI")),
                    "concept2_ui": _text(cr.find("Concept2UI")),
                    "relation_name": cr.get("RelationName"),
                })
            for t in c.findall("TermList/Term"):
                thes = [x for x in (_text(e) for e in t.findall("ThesaurusIDlist/ThesaurusID")) if x]
                self._terms.append({
                    "record_ui": record_ui,
                    "concept_ui": cui,
                    "term_ui": _text(t.find("TermUI")),
                    "term": _text(t.find("String")),
                    "is_concept_preferred": _yn(t, "ConceptPreferredTermYN"),
                    "is_record_preferred": _yn(t, "RecordPreferredTermYN"),
                    "is_permuted": _yn(t, "IsPermutedTermYN"),
                    "lexical_tag": t.get("LexicalTag"),
                    "entry_version": _text(t.find("EntryVersion")),
                    "thesaurus_ids": "|".join(thes) or None,
                    "date_created": _date(t.find("DateCreated")),
                })

    # -- per-file parsing ---------------------------------------------------

    def _parse_descriptors(self) -> None:
        rows, tree, allowq, dpa, prev, see, ecomb = [], [], [], [], [], [], []
        with _open(self._file("desc")) as fh:
            for _, el in ET.iterparse(fh, events=("end",)):
                if el.tag != "DescriptorRecord":
                    continue
                ui = _text(el.find("DescriptorUI"))
                rows.append({
                    "descriptor_ui": ui,
                    "name": _text(el.find("DescriptorName/String")),
                    "descriptor_class": el.get("DescriptorClass"),
                    "date_introduced": _date(el.find("DateIntroduced")),
                    "last_updated": _date(el.find("LastUpdated")),
                    "annotation": _text(el.find("Annotation")),
                    "history_note": _text(el.find("HistoryNote")),
                    "online_note": _text(el.find("OnlineNote")),
                    "public_mesh_note": _text(el.find("PublicMeSHNote")),
                    "nlm_classification_number": _text(el.find("NLMClassificationNumber")),
                    "consider_also": _text(el.find("ConsiderAlso")),
                })
                for tn in el.findall("TreeNumberList/TreeNumber"):
                    tree.append({"descriptor_ui": ui, "tree_number": _text(tn)})
                for aq in el.findall("AllowableQualifiersList/AllowableQualifier"):
                    allowq.append({
                        "descriptor_ui": ui,
                        "qualifier_ui": _text(aq.find("QualifierReferredTo/QualifierUI")),
                        "abbreviation": _text(aq.find("Abbreviation")),
                    })
                for pa in el.findall("PharmacologicalActionList/PharmacologicalAction"):
                    dpa.append({"descriptor_ui": ui, "action_descriptor_ui": _text(pa.find("DescriptorReferredTo/DescriptorUI"))})
                for pi in el.findall("PreviousIndexingList/PreviousIndexing"):
                    prev.append({"descriptor_ui": ui, "previous_indexing": _text(pi)})
                for sr in el.findall("SeeRelatedList/SeeRelatedDescriptor"):
                    see.append({"descriptor_ui": ui, "see_related_descriptor_ui": _text(sr.find("DescriptorReferredTo/DescriptorUI"))})
                for ec in el.findall("EntryCombinationList/EntryCombination"):
                    ecin, ecout = ec.find("ECIN"), ec.find("ECOUT")
                    ecomb.append({
                        "descriptor_ui": ui,
                        "ecin_descriptor_ui": _text(ecin.find("DescriptorReferredTo/DescriptorUI")) if ecin is not None else None,
                        "ecin_qualifier_ui": _text(ecin.find("QualifierReferredTo/QualifierUI")) if ecin is not None else None,
                        "ecout_descriptor_ui": _text(ecout.find("DescriptorReferredTo/DescriptorUI")) if ecout is not None else None,
                        "ecout_qualifier_ui": _text(ecout.find("QualifierReferredTo/QualifierUI")) if ecout is not None else None,
                    })
                self._parse_concepts(el, ui, "descriptor")
                el.clear()
        self.save_parquet(pd.DataFrame(rows), self.intermediate_path() / "descriptors.parquet")
        self.save_parquet(pd.DataFrame(tree), self.intermediate_path() / "descriptor_tree_numbers.parquet")
        self.save_parquet(pd.DataFrame(allowq), self.intermediate_path() / "descriptor_allowable_qualifiers.parquet")
        self.save_parquet(pd.DataFrame(dpa), self.intermediate_path() / "descriptor_pharmacological_actions.parquet")
        self.save_parquet(pd.DataFrame(prev), self.intermediate_path() / "descriptor_previous_indexing.parquet")
        self.save_parquet(pd.DataFrame(see), self.intermediate_path() / "descriptor_see_related.parquet")
        self.save_parquet(pd.DataFrame(ecomb), self.intermediate_path() / "descriptor_entry_combinations.parquet")

    def _parse_qualifiers(self) -> None:
        rows, tree = [], []
        with _open(self._file("qual")) as fh:
            for _, el in ET.iterparse(fh, events=("end",)):
                if el.tag != "QualifierRecord":
                    continue
                ui = _text(el.find("QualifierUI"))
                rows.append({
                    "qualifier_ui": ui,
                    "name": _text(el.find("QualifierName/String")),
                    "abbreviation": _text(el.find("Abbreviation")),
                    "date_introduced": _date(el.find("DateIntroduced")),
                    "last_updated": _date(el.find("LastUpdated")),
                    "annotation": _text(el.find("Annotation")),
                    "history_note": _text(el.find("HistoryNote")),
                    "online_note": _text(el.find("OnlineNote")),
                })
                for tn in el.findall("TreeNumberList/TreeNumber"):
                    tree.append({"qualifier_ui": ui, "tree_number": _text(tn)})
                self._parse_concepts(el, ui, "qualifier")
                el.clear()
        self.save_parquet(pd.DataFrame(rows), self.intermediate_path() / "qualifiers.parquet")
        self.save_parquet(pd.DataFrame(tree), self.intermediate_path() / "qualifier_tree_numbers.parquet")

    def _parse_supplemental(self) -> None:
        rows, hmap, iinfo, spa, prev, src = [], [], [], [], [], []
        with _open(self._file("supp")) as fh:
            for _, el in ET.iterparse(fh, events=("end",)):
                if el.tag != "SupplementalRecord":
                    continue
                ui = _text(el.find("SupplementalRecordUI"))
                rows.append({
                    "scr_ui": ui,
                    "name": _text(el.find("SupplementalRecordName/String")),
                    "scr_class": el.get("SCRClass"),
                    "date_introduced": _date(el.find("DateIntroduced")),
                    "last_updated": _date(el.find("LastUpdated")),
                    "note": _text(el.find("Note")),
                    "frequency": _text(el.find("Frequency")),
                })
                for hm in el.findall("HeadingMappedToList/HeadingMappedTo"):
                    d, primary = _strip_star(_text(hm.find("DescriptorReferredTo/DescriptorUI")))
                    q, _ = _strip_star(_text(hm.find("QualifierReferredTo/QualifierUI")))
                    hmap.append({"scr_ui": ui, "descriptor_ui": d, "qualifier_ui": q, "is_primary": primary})
                for ii in el.findall("IndexingInformationList/IndexingInformation"):
                    d, _ = _strip_star(_text(ii.find("DescriptorReferredTo/DescriptorUI")))
                    q, _ = _strip_star(_text(ii.find("QualifierReferredTo/QualifierUI")))
                    iinfo.append({"scr_ui": ui, "descriptor_ui": d, "qualifier_ui": q})
                for pa in el.findall("PharmacologicalActionList/PharmacologicalAction"):
                    spa.append({"scr_ui": ui, "action_descriptor_ui": _text(pa.find("DescriptorReferredTo/DescriptorUI"))})
                for pi in el.findall("PreviousIndexingList/PreviousIndexing"):
                    prev.append({"scr_ui": ui, "previous_indexing": _text(pi)})
                for s in el.findall("SourceList/Source"):
                    src.append({"scr_ui": ui, "source": _text(s)})
                self._parse_concepts(el, ui, "supplemental")
                el.clear()
        df = pd.DataFrame(rows)
        df["frequency"] = pd.to_numeric(df["frequency"], errors="coerce").astype("Int64")
        self.save_parquet(df, self.intermediate_path() / "supplemental_records.parquet")
        self.save_parquet(pd.DataFrame(hmap), self.intermediate_path() / "supplemental_heading_mapped_to.parquet")
        self.save_parquet(pd.DataFrame(iinfo), self.intermediate_path() / "supplemental_indexing_information.parquet")
        self.save_parquet(pd.DataFrame(spa), self.intermediate_path() / "supplemental_pharmacological_actions.parquet")
        self.save_parquet(pd.DataFrame(prev), self.intermediate_path() / "supplemental_previous_indexing.parquet")
        self.save_parquet(pd.DataFrame(src), self.intermediate_path() / "supplemental_sources.parquet")

    def _parse_pharmacological_actions(self) -> None:
        rows = []
        with _open(self._file("pa")) as fh:
            for _, el in ET.iterparse(fh, events=("end",)):
                if el.tag != "PharmacologicalAction":
                    continue
                ad = _text(el.find("DescriptorReferredTo/DescriptorUI"))
                for sub in el.findall("PharmacologicalActionSubstanceList/Substance"):
                    rows.append({
                        "action_descriptor_ui": ad,
                        "substance_ui": _text(sub.find("RecordUI")),
                        "substance_name": _text(sub.find("RecordName/String")),
                    })
                el.clear()
        self.save_parquet(pd.DataFrame(rows), self.intermediate_path() / "pharmacological_actions.parquet")

    # -- pipeline stages ----------------------------------------------------

    def extract(self) -> None:
        # Shared concept substructure accumulated across all three record types.
        self._concepts: list[dict] = []
        self._terms: list[dict] = []
        self._relations: list[dict] = []
        self._related_rn: list[dict] = []

        print(f"[{self.name}] parsing descriptors")
        self._parse_descriptors()
        print(f"[{self.name}] parsing qualifiers")
        self._parse_qualifiers()
        print(f"[{self.name}] parsing supplemental records")
        self._parse_supplemental()
        print(f"[{self.name}] parsing pharmacological actions")
        self._parse_pharmacological_actions()

        self.save_parquet(pd.DataFrame(self._concepts), self.intermediate_path() / "concepts.parquet")
        self.save_parquet(pd.DataFrame(self._terms), self.intermediate_path() / "concept_terms.parquet")
        self.save_parquet(pd.DataFrame(self._relations), self.intermediate_path() / "concept_relations.parquet")
        self.save_parquet(pd.DataFrame(self._related_rn), self.intermediate_path() / "concept_related_registry_numbers.parquet")
        print(
            f"[{self.name}] concepts: {len(self._concepts):,}, terms: {len(self._terms):,}, "
            f"relations: {len(self._relations):,}"
        )

    def transform(self) -> None:
        # Faithful projection; all parsing/typing happens in extract + load.
        pass

    def load(self) -> None:
        for stem, schema in self._TABLES:
            df = self.load_parquet(self.intermediate_path() / f"{stem}.parquet")
            df = df[list(schema)]
            df = self.apply_schema(df, schema)
            self.save_parquet(df, self.output_path() / f"{stem}.parquet")
            self.save_schema_yaml(schema, stem)
