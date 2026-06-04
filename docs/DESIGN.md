# lacuna-etl design and contract

This document is the authoritative description of what this repo produces and the
guarantees those outputs carry. The inspection and MCP repos that consume these
datasets should treat the contract sections below (Output layout, Identifier
contract, Invariants) as canonical and reference them rather than re-deriving the
shape of the data.

## Purpose

`lacuna-etl` turns raw scientific bibliographic datasets, as downloaded and
versioned by the sibling `data_downloader` repo, into cleaned, typed, validated
Parquet tables with machine-readable schema sidecars.

The split is deliberate. `data_downloader` owns acquisition and versioning and
nothing else; it never reshapes data. This repo owns transformation and the
output contract and never fetches anything. Keeping download and transform in
separate repos means a re-download cannot silently change cleaning logic, and a
cleaning change can be re-run against an already-downloaded snapshot without
touching the network.

## Inputs and outputs

There are three roots, resolved in this order: environment variable, then
`~/.config/lacuna_etl/config.toml` (written by `etl configure`), then a built-in
default where one exists. See `src/lacuna_etl/config.py`.

- **Data root** (`DL_DATA_ROOT`): where `data_downloader` writes raw snapshots.
  Required; no default. Each dataset lives at `<data_root>/<name>/<version>/...`
  and the pipeline always reads the lexicographically latest `<version>`
  directory, so the ETL operates on the newest downloaded snapshot.
- **Output root** (`ETL_OUTPUT_ROOT`): where cleaned datasets are written.
  Required; no default. Outputs go to `<output_root>/<name>/`.
- **Intermediate root** (`ETL_INTERMEDIATE_ROOT`): staging area between pipeline
  stages. Defaults to `./intermediate/` in this repo. Intermediates are
  disposable; they exist for restartability, not for consumption.

Outputs are Parquet (snappy-compressed) plus a YAML sidecar per table. Parquet
is chosen for typed, nullable, columnar, compressed storage that round-trips
pandas and polars dtypes; the sidecar carries the human- and machine-readable
column contract (identifier type, description, allowed values, required flag)
that bare Parquet cannot express. The sidecar is generated from the same
`ColumnSpec` objects used to validate the data, so documentation and enforcement
cannot drift.

## Architecture

Every dataset is a `DatasetPipeline` subclass (`src/lacuna_etl/core/pipeline.py`)
with three stages run in order by `run()`:

1. **extract** — read raw files, rename source columns to snake_case, cast to
   identifier dtypes, write intermediate Parquet (or, for streaming pipelines,
   final shards directly).
2. **transform** — apply cross-dataset corrections, explode multi-valued fields,
   deduplicate, and validate identifier columns.
3. **load** — write final Parquet to the output root and emit the schema sidecar.

Pipelines self-register with the `@register` decorator into `REGISTRY`
(`src/lacuna_etl/datasets/registry.py`); importing a dataset module is what
registers it, so every dataset is imported in `datasets/__init__.py`. The CLI
(`etl list`, `etl run <name>`) is a thin shell over the registry.

`depends_on` declares cross-dataset dependencies (for example, the gene tables
depend on `ncbi_gene_history`). **Dependencies are declared, not orchestrated:**
`run()` executes a single dataset, and a dependent reads its prerequisite's
*output*, so the operator must run prerequisites first. This keeps the runner
trivial and makes each stage independently restartable, at the cost of manual
ordering. `etl list` prints the dependencies to make ordering visible.

### Two execution shapes

- **In-memory (pandas).** Used for the NCBI gene tables and iCite, which fit in
  memory. Read whole, cast, validate, write.
- **Streaming, sharded, parallel (polars + `ProcessPoolExecutor`).** Used for
  OpenAlex, PubMed, and PubTator3, whose snapshots are far too large for memory.
  Each input file is processed independently into per-file Parquet shards, with a
  done-marker (PubMed, PubTator3) or shard-exists check (OpenAlex) so an
  interrupted run resumes instead of restarting. Peak memory is bounded by one
  batch/shard, not the dataset. For OpenAlex the snapshot is already immutable per
  version, so `extract` writes final shards directly, `transform` only validates
  across shards, and `load` only writes sidecars. PubTator3's unit of work is one
  whole `BioCXML.<n>.tar.gz` archive per worker; because a single archive holds
  millions of documents, the worker flushes per-table shards every N documents to
  keep its memory bounded.

### Filling in missing values

When a field is sometimes null in raw data and you want to populate it for more
rows, the right shape depends on where the substitute value comes from.

- **Same-record fallback is normal extract/transform.** Deriving a column from
  another field on the same record — falling back from `PubDate.Year` to
  `ArticleDate.Year`, parsing a year out of a `MedlineDate` free-text string,
  stripping URL prefixes off an identifier — stays inside that record's own
  authoritative source. Populate the column directly; the column's meaning is
  unchanged and no provenance column is needed.
- **Cross-dataset heuristic linkage gets its own table.** Inferring
  `openalex_works.pmid` for records OpenAlex lacks one for, by matching against
  PubMed on DOI / title / journal / year, is a different operation: the value
  comes from outside the source record, the match is heuristic, and a false
  match links an entirely wrong paper. Such linkages belong in their own dataset
  — a crosswalk table such as `(work_id, pmid, match_source)` with `depends_on`
  on both upstreams. The upstream tables stay unchanged; consumers join the
  crosswalk when they want the enriched view. This preserves the provenance of
  every value, lets the matching heuristic be re-tuned without rewriting the
  heavy upstream outputs, and keeps each artifact answering exactly one
  question.

The rule: same column, broader population is fine when the new values come from
within the same record's own authoritative source. It becomes a contract
problem when the new values come from outside that source or from heuristic
matching across datasets — those cases need their own table so provenance,
confidence, and iteration speed are preserved.

## Identifier contract

Identifier types (`src/lacuna_etl/core/identifiers.py`) centralize the canonical
form and validation of every cross-referenced ID, so that any dataset emitting a
DOI, PMID, ORCID, and so on enforces the *same* shape. This is the core of why
downstream repos can join across datasets safely. Canonical forms:

- **DOI** — URL prefix stripped, lowercase scheme removed; stored as `10.x/...`,
  never as a `https://doi.org/` URL. **Article-level**: a publisher version
  suffix is never present. A handful of publishers append an article *version* to
  the DOI — F1000 platform `10.12688/…` (`.N`, legacy `.vN`), Research Square
  `10.21203/…/vN`, Preprints.org `10.20944/…vN`, ChemRxiv `10.26434`, TechRxiv
  `10.36227`, Authorea/ESSOAr `10.22541/…/vN`, Qeios `10.32388/…N`, and
  bioRxiv/medRxiv `10.1101/…vN` (the version `vN` is appended directly to the
  article number with no separator; the rule is digit-led to exclude the Cold
  Spring Harbor Press journals on the same registrant) — so two records
  of the same article can carry DOIs differing only by version, which breaks DOI
  joins. Detection is a curated, registrant-anchored allowlist (a blanket
  "trailing `.N`/`vN`" rule is unsafe: most DOIs legitimately end in numbers, e.g.
  Elsevier page ids, journal *Viruses* `10.3390/v1010072`, arXiv/SSRN ids). A
  recognised versioned DOI is split into the article-level base `doi` plus a
  sibling `<col>_versioned` column.
- **DoiVersioned** — same canonical `10.x/...` shape as DOI but *may* carry a
  publisher version suffix (supports, does not require, versioning). This is the
  type of the sibling `<col>_versioned` column that preserves the original
  versioned DOI; it is null on rows whose DOI carries no recognised version.
- **PMID, NCBI Gene ID, NCBI Tax ID** — positive `Int64`, never zero or negative.
- **ORCID** — canonical hyphenated `XXXX-XXXX-XXXX-XXXX`, ISO 7064 MOD 11-2 check
  digit recovered or verified; values that fail the checksum become null rather
  than propagating a wrong ID.
- **ISSN-L** — canonical hyphenated `XXXX-XXXC`, checksum verified/recovered.
- **GO ID** — `GO:` followed by 7 digits.
- **Disease Ontology ID** — `DOID:` followed by digits (`DOID:0001816`). The Human
  Disease Ontology's native term ID; keys the `disease_ontology` tables and every
  edge that references a term. Enforced in pandas (the pipeline is pandas-backed).
  The ontology's external cross-references (MESH, UMLS_CUI, ORDO, MIM, SNOMEDCT, …)
  are heterogeneous CURIEs with no single canonical form and stay plain strings.
- **UniProt accession** — the canonical 6- or 10-character UniProtKB accession
  (`P12345`, `Q70XZ5`, `A0A804MTU9`), matched by UniProt's official accession
  regex; isoform suffixes (`-2`) are not part of the base accession. Enforced in
  pandas (used by `unknome`).
- **MeSH UIs** — the NLM Medical Subject Headings record identifiers, each a
  single-letter-prefixed UI enforced (in pandas) by a dedicated type:
  `MeshDescriptorId` (`D\d+`), `MeshQualifierId` (`Q\d+`), `MeshSupplementalId`
  (`C\d+`, Supplementary Concept Records), `MeshConceptId` (`M\d+`), `MeshTermId`
  (`T\d+`). PubMed's `mesh_headings.descriptor_ui`/`qualifier_ui` carry the same
  `D…`/`Q…` form (left as documented strings there).
- **Alliance gene ID** — the Alliance of Genome Resources canonical gene curie:
  one of the eight model-organism-database prefixes `HGNC`/`MGI`/`RGD`/`ZFIN`/
  `SGD`/`FB`/`WB`/`Xenbase` followed by that database's accession. No single
  accession shape spans the databases, so the prefix set is the anchor. Unlike
  most string identifiers (pattern enforced only in polars), this one also
  enforces its pattern in pandas, since the Alliance pipelines are pandas-backed
  and the `ncbi_gene2_alliance` join depends on the form.
- **ROR ID** — canonical full URL `https://ror.org/...` (the registry's own
  canonical form).
- **Wikidata ID** — bare `Q\d+`, URL forms stripped.
- **Country code** — ISO 3166-1 alpha-2 (and alpha-3 where noted).
- **OpenAlex IDs** — short form (`W2741809807`, `A...`, `I...`, etc.), URL prefix
  stripped, to normalize and to save space across hundreds of millions of rows.
- **Ensembl Gene ID** — unversioned `ENS…G\d+`; the species infix varies (`ENSG…`
  human, `ENSMUSG…` mouse), so the pattern is permissive across species. Open
  Targets' canonical key for a target and the `proteinatlas` gene key. Cross-species
  *homologue* gene IDs are not all Ensembl (worm/fly use `WBGene`/`FBgn`), so those
  columns stay plain strings. The pattern is enforced in pandas as well as polars
  (the `proteinatlas` pipeline is pandas-backed).
- **ChEMBL ID** — `CHEMBL\d+`; Open Targets' canonical drug-molecule identifier.
- **RefSeq accession** — `[A-Z]{2}_\d+` with an optional `.<version>` suffix
  (some sources, e.g. Ensembl's TSV dumps, drop the version): the curated/predicted
  transcript and protein accessions `NM/NR/XM/XR/NP/XP/YP`. WGS *genomic* RefSeq
  accessions interleave letters after the prefix (`NZ_MCBT01000001.1`) and so do
  *not* use this type — columns that can hold those stay plain strings.
- **RefSeq accession** — `[A-Z]{2}_\d+` with an optional `.<version>` suffix: the
  curated/predicted transcript and protein accessions `NM/NR/XM/XR/NP/XP/YP`. WGS
  *genomic* RefSeq accessions interleave letters after the prefix
  (`NZ_MCBT01000001.1`) and so do *not* use this type — columns that can hold those
  stay plain strings.

Some machine-generated identifiers are intentionally *not* given a canonical
identifier type and are stored as documented plain strings, following the
PubTator precedent: Open Targets disease/phenotype IDs are heterogeneous CURIEs
(`EFO_…`, `MONDO_…`, `HP_…`, `Orphanet_…`, `MP:…`) with no single canonical form,
so `disease_id` and the ontology-edge columns are plain strings (required where
they are a key) rather than a fragile catch-all pattern.

Patterns are stored unanchored on each identifier and anchored at validation
time. Empty and whitespace-only strings are normalized to null (an OpenAlex
convention applied everywhere). A pipeline that produces a value violating its
identifier's pattern fails loudly; it does not emit bad data.

## Output layout and table inventory

`<output_root>/<dataset>/<table>.parquet` with a sibling `<table>.yml`.
Column-level detail (names, identifier types, descriptions, allowed values) is
defined once in each dataset module's `SCHEMA` / `TABLES_DOC` dict and emitted
verbatim to the sidecar. **Those `*.yml` sidecars, generated from the module
`ColumnSpec`s, are the authoritative column-level schema.** This file documents
the table set and grain; it does not duplicate every column.

NCBI gene tables (pandas; all `depends_on = ncbi_gene_history`):

- `ncbi_gene_history` → `gene_history` (discontinued/replacement Gene ID map).
- `ncbi_gene_info` → `gene_info` (one row per gene).
- `ncbi_gene2go` → `gene2go` (one row per gene/GO/PubMed annotation; the source
  pipe-joined PubMed list is exploded to one PMID per row).
- `ncbi_gene2pubmed` → `gene2pubmed` (one row per gene/PubMed link).
- `ncbi_generifs` → `gene_rif` (Gene Reference into Function statements).
- `ncbi_gene2ensembl` → `gene2ensembl` (one row per NCBI↔Ensembl gene/transcript/
  protein mapping line). Restricted to the eight reference model organisms (worm
  and yeast are absent from the snapshot). `tax_id` + `entrez_id` typed; the paired
  `refseq_rna_accession` / `refseq_protein_accession` typed `RefSeqAccession`. The
  `ensembl_gene_id` / `ensembl_transcript_id` / `ensembl_protein_id` columns are
  plain strings (version-stripped to the unversioned canonical form) because they
  are species-native — vertebrates use `ENS…` but fly uses FlyBase `FBgn/FBtr/FBpp`,
  so no single Ensembl pattern holds.
- `ncbi_gene2accession` → `gene2accession` (one row per gene/accession-set line),
  restricted to the reference model organisms (both yeast taxa, 4932 and 559292,
  appear). The genome-wide ~4 GB source is read once with Polars and filtered to
  the model taxa into a restartable intermediate, then transformed in pandas.
  `tax_id` + `entrez_id` typed; `status` (RefSeq curation lifecycle, `-` for
  non-RefSeq rows) and `orientation` (`+`/`-`/`?`) carry `allowed_values`; the GI
  and position columns are `Int64`. The RNA / protein / genomic / mature-peptide
  `accession.version` columns are **plain strings**: each mixes RefSeq with GenBank
  (and the genomic column also WGS `NZ_…`) accessions, so no single canonical
  identifier form holds. Here `-` is the missing marker for the accession/GI/
  position/assembly/symbol columns but a *real value* for `status` (non-RefSeq) and
  `orientation` (minus strand), so it is nulled selectively. Unlike the current
  gene list, an accession dump can still reference a gene since discontinued
  *without* a replacement; those rows (2 in the current snapshot) are dropped and
  the count is logged, rather than hard-erroring the run.

Ensembl cross-references (`ensembl_tsv`, pandas; the per-species "Stable ID to
&lt;db&gt;" TSV dumps, restricted upstream to eight reference organisms — human,
mouse, rat, zebrafish, fly, worm, chicken, yeast — with a `tax_id` column added
from the species directory). One table per external-database dump, each a faithful
one-row-per-source-mapping projection:

- `entrez` — Ensembl gene/transcript/protein ↔ NCBI. `xref` is a plain string
  because the dump mixes Entrez Gene IDs (`db_name='EntrezGene'`) with Entrez
  transcript *names* (`db_name='EntrezGene_trans_name'`).
- `refseq` — Ensembl ↔ RefSeq; `refseq_accession` typed `RefSeqAccession`,
  `db_name` gives the molecule type (mRNA/peptide/ncRNA, curated or predicted).
- `uniprot` — Ensembl ↔ UniProt; `uniprot_accession` plain string (`db_name`
  separates SWISSPROT / SPTREMBL / isoform).
- `ena` — Ensembl ↔ ENA/INSDC; `primary_accession` / `secondary_accession` plain
  strings (the primary column also carries clone/contig/chromosome labels).

The `ensembl_gene_id` / `ensembl_transcript_id` / `ensembl_protein_id` columns are
**plain strings, not `EnsemblGeneId`-typed**: only the five vertebrates use `ENS…`
stable IDs, while yeast (SGD `YDL246C`), worm (WormBase `WBGene…`), and fly
(FlyBase `FBgn…`/`FBtr…`/`FBpp…`) use species-native IDs, so no single canonical
Ensembl pattern holds across the species set. The chromosome-length `karyotype`
dumps are not ingested (genome metadata, not a cross-reference).

NCBI Taxonomy (`ncbi_taxdump`, pandas; the full reference tree, no organism
filter): parsed from the pipe-delimited `.dmp` members read directly out of
`taxdump.tar.gz`. Three tables:

- `taxonomy_nodes` — one row per `tax_id`: `parent_tax_id` (the root node 1 is its
  own parent), `rank`, `division_id`, `genetic_code_id`.
- `taxonomy_names` — one row per `(tax_id, name, name_class)`: the `name`, its
  disambiguated `unique_name` (null when `name` is already unique), and the
  `name_class` (`scientific name`, `synonym`, `authority`, `genbank common name`,
  …; a documented free string — the vocabulary can grow between releases).
- `taxonomy_merged` — one row per `old_tax_id`: the `new_tax_id` a since-merged
  taxon now resolves to (analogous to `gene_history` for Gene IDs).

iCite (pandas):

- `icite` → `icite` (one row per PMID, citation metrics) and
  `open_citation_collection` (citing → referenced PMID pairs). `icite.doi` is
  article-level with a `doi_versioned` sibling (see the DOI identifier contract).

PubMed / MEDLINE (`ncbi_pubmed`, streaming): one parent table `articles` (one
current row per PMID) plus child tables keyed by PMID — `authors`,
`affiliations`, `mesh_headings`, `chemicals`, `publication_types`, `grants`,
`keywords`, `article_ids`, `references` — and `deleted_pmids`. `articles.doi` is
article-level with a `doi_versioned` sibling; `article_ids` is left as the
verbatim source id list (its plain-string `value` is not version-split).

OpenAlex (21 datasets, streaming): `openalex_works` produces `works`,
`works_authorships`, `works_topics`, and `works_refs`; the other 20 entities
(`openalex_authors`, `openalex_sources`, `openalex_institutions`, the topic
hierarchy, and the controlled-vocabulary lookups) each produce a single
similarly named table. `works.doi` is article-level with a `doi_versioned`
sibling.

PubTator3 (`ncbi_pubtator3`, streaming): the BioC-XML archives carry NCBI's
text-mined bio-entity annotations and concept-concept relations over PubMed and
the PMC full-text subset. Three tables, all keyed by PMID:

- `articles` — one row per PMID (document), with the article-level metadata
  PubTator carries (pmc_id, doi (+ `doi_versioned` sibling), year,
  volume/issue/pages, license) plus
  `has_full_text` and `n_passages`. Body text is intentionally not stored.
  Metadata is read only from the front/title passage, never from reference
  passages (which describe cited works). A small fraction of documents (~0.1%)
  are PMC full-text articles keyed by a `PMC…` id with no PMID; because the whole
  dataset is PMID-keyed, those are excluded and their count is reported at the end
  of `extract`.
- `annotations` — one row per tagged entity mention, with its `section_type`,
  `offset`, `length`, surface `mention`, `entity_type`, and the verbatim concept
  `identifier`. Mentions tagged inside reference-list passages are excluded.
- `relations` — one row per extracted relation, with `relation_type`, `score`,
  and the two roles split into `roleN_type` / `roleN_identifier`.

`entity_type`, `section_type`, and `relation_type` are documented free strings,
not `allowed_values`-constrained: they are machine-generated and the value set
can grow between PubTator releases, so a new value should not abort a run. The
concept `identifier` is stored exactly as PubTator emits it (heterogeneous by
type, and a packed composite for variants), so it is a plain string rather than a
single canonical identifier type.

NLM Catalog — journals reported to MEDLINE (`ncbi_nlmcatalog_reportedmedline`,
pandas; one large `NLMCatalogRecordSet` XML, ~15.5k serial records). Parsed in a
single `iterparse` pass. The parent key is `nlm_unique_id`
(`NlmUniqueId`: the NLM Catalog `NlmUniqueID`, an opaque catalog key stored as a
string — almost all digits, some older IDs carry a trailing check letter such as
`2984730R`, so it is not an integer). Eight tables, the parent plus seven
children keyed by `nlm_unique_id`:

- `journals` — one row per record: titles (`title_main`, `medline_ta`),
  convenience `issn_print` / `issn_electronic` (first of each medium; full set in
  `journal_issns`), country/publisher, `publication_first_year` /
  `publication_end_year` (`Int64`; the `9999` "continuing" sentinel becomes null
  and sets `is_ongoing_publication`), the four catalog dates, `record_status`,
  and `currently_indexed_medline` (any MEDLINE-lineage source with status
  `Currently-indexed`).
- `journal_issns` — one row per `(nlm_unique_id, issn, issn_type)`; ISSNs
  normalized to canonical `IssnL` form, `issn_type` ∈ {`Print`, `Electronic`,
  `Undetermined`}.
- `journal_alternate_titles`, `journal_languages`, `journal_mesh_headings` —
  the exploded `TitleAlternate`, `Language`, and MeSH `DescriptorName` lists.
- `journal_relations` — one row per `TitleRelated` link, the journal
  split/merge/continuation lineage: `relation_type` (`Preceding`, `Succeeding`,
  `MergedTo`, `MergerOf`, `SplitTo`, `SplitFrom`, `Absorbed`, `Supersedes`,
  `Translated`, … — a documented free string, the vocabulary can grow) and
  `related_nlm_unique_id`, which joins back to `journals.nlm_unique_id` when NLM
  recorded one (else null; non-conforming related IDs are nulled, not dropped).
- `indexing_coverage` — one row per parsed indexing *frame*, keyed by
  `(nlm_unique_id, source_index, frame_index)`: the `indexing_source` (MEDLINE,
  OLDMEDLINE, Index medicus, PubMed, PMC, …; case variants of MEDLINE/PubMed
  normalized, otherwise verbatim — a documented free string), `is_medline_indexing`
  (the MEDLINE / Index Medicus indexing lineage, vs PubMed deposit / PMC /
  reference works), `indexing_treatment`, `indexing_status`, the verbatim
  `coverage_raw`, and `start_year` / `end_year` / `ongoing` recovered from it.
- `indexed_years` — one row per `(nlm_unique_id, indexing_source, year)`: the
  coverage frames exploded to the individual years a journal was indexed, the
  table this dataset exists to produce.

  The `Coverage` strings are historically grown free text with inconsistent
  human formatting and can encode disjoint frames (a journal de-indexed then
  re-indexed). Rather than parse every volume/issue/month token, the parser
  recovers only the years: it splits disjoint frames on `;`, truncates appended
  notes ("selected citations only before this date"), and reads the first and
  last four-digit year of each frame as its span — robust against the hyphens
  *inside* dates (`Jan.-Feb.`, `v25n3-4`, `1980-81`) because it never splits on
  `-`. A frame ending in an open `-` is flagged `ongoing`, and in `indexed_years`
  such frames are expanded through the snapshot's year (so the open end is
  recoverable from `indexing_coverage.ongoing` but materialized to "present" in
  the per-year explosion). `indexed_years` covers **all** indexing sources;
  consumers filter on `is_medline_indexing` for true MEDLINE indexing.
  
Open Targets (6 datasets, streaming per-Parquet-file; one registered pipeline per
Open Targets (55 datasets, streaming per-Parquet-file; one registered pipeline per
downloaded product, named `opentargets_<product>`): the Platform ships each
product as a directory of Parquet part files, so the unit of work is one part file
processed into per-table shards (restartable via a shard-exists check), and the
transform is columnar Polars (unnest structs, explode lists) rather than row-wise.
Outputs follow the OpenAlex sharded layout (`<dataset>/<table>/<part>.parquet`
plus `<table>.yml`). The default shape is fully exploded — a parent table per
product plus a child table per repeated/nested field, keyed by the parent ID.
List-of-scalar fields are kept as list columns on the parent (matching how `target`
keeps `transcript_ids` etc.); `List(Struct)` fields become child tables. Two
products whose source rows have no stable key (`interaction_evidence`,
`pharmacogenomics`) cannot key child tables, so their nested structs are instead
flattened in place (parallel list columns, or denormalised onto one row per nested
element); this exception is noted on each below.

**Core annotated entities.**

- `opentargets_target` (key Ensembl gene ID): `targets` plus `targets_transcripts`,
  `targets_go`, `targets_synonyms` (current and obsolete symbol/name synonyms,
  tagged by `synonym_type`), `targets_subcellular_locations`, `targets_classes`,
  `targets_constraints`, `targets_protein_ids`, `targets_db_xrefs`,
  `targets_pathways`, `targets_tractability`, `targets_homologues`,
  `targets_chemical_probes` (+ `targets_chemical_probe_urls`),
  `targets_safety_liabilities` (+ `targets_safety_effects`,
  `targets_safety_biosamples`, `targets_safety_studies`, linked by
  `liability_index`), `targets_hallmark_attributes`, and `targets_cancer_hallmarks`.
- `opentargets_target_essentiality` (key Ensembl gene ID): `target_essentiality`
  (per-gene essentiality) plus `target_essentiality_screens` (one row per DepMap
  cell-line screen, flattened across `geneEssentiality → depMapEssentiality →
  screens`).
- `opentargets_target_prioritisation` (key Ensembl gene ID): `target_prioritisation`,
  one row per gene of per-property prioritisation flags (0/1/-1 integer indicators)
  and continuous scores.
- `opentargets_expression` (key Ensembl gene ID): `expression_tissues` (one row per
  gene × tissue, RNA + protein summary levels) plus `expression_protein_cell_types`
  (per gene × tissue, protein level by cell type).
- `opentargets_disease` (key disease/EFO ID): `diseases` plus `diseases_synonyms`
  (tagged by `synonym_scope`), the ontology-edge tables `diseases_parents`,
  `diseases_children`, `diseases_ancestors`, `diseases_descendants`,
  `diseases_therapeutic_areas`, `diseases_xrefs`, `diseases_obsolete_terms`,
  `diseases_obsolete_xrefs`, and `diseases_ontology_sources`.
- `opentargets_disease_hpo` (key HPO term ID): `hpo_terms` plus `hpo_terms_xrefs`,
  `hpo_terms_parents`, `hpo_terms_obsolete_terms`.
- `opentargets_disease_phenotype` (key disease+phenotype): `disease_phenotypes`
  plus `disease_phenotype_evidence` (one row per supporting HPOA evidence record).
- `opentargets_biosample` (key biosample ontology ID): `biosamples` plus
  `biosamples_synonyms`, `biosamples_xrefs`, and the edge tables
  `biosamples_parents`/`_children`/`_ancestors`/`_descendants`.
- `opentargets_go` (key GO ID): `go_terms` plus `go_terms_alt_ids` and the relation
  edge tables `go_terms_is_a`, `go_terms_part_of`, `go_terms_regulates`,
  `go_terms_negatively_regulates`, `go_terms_positively_regulates`.
- `opentargets_so` (key SO ID): `so_terms` (Sequence Ontology ID + label).

**Drugs.**

- `opentargets_drug_molecule` (key ChEMBL ID): `drugs` (with trade-name, synonym,
  and child-ChEMBL-ID list columns) plus `drugs_cross_references`.
- `opentargets_drug_mechanism_of_action` (key ChEMBL ID; source rows are keyless so
  the mechanism is exploded by the molecules it applies to): `drug_mechanisms` plus
  `drug_mechanism_references`.
- `opentargets_drug_warning` (key warning ID): `drug_warnings` plus
  `drug_warnings_chembl_ids` and `drug_warnings_references`.
- `opentargets_openfda_significant_adverse_drug_reactions` (key ChEMBL ID + event):
  `drug_adverse_reactions` (FAERS disproportionality LLR signals).
- `opentargets_pharmacogenomics` (keyless): a single `pharmacogenomics` table
  denormalised to one row per (evidence, variant annotation); the associated drugs
  are flattened into parallel list columns.

**Clinical.**

- `opentargets_clinical_indication` (key indication ID): `clinical_indications`
  (drug → disease at a max clinical stage).
- `opentargets_clinical_target` (key record ID): `clinical_targets` plus
  `clinical_targets_diseases`.
- `opentargets_clinical_report` (key report ID): `clinical_reports` plus
  `clinical_reports_drugs`, `clinical_reports_diseases`,
  `clinical_reports_side_effects`.

**Target–disease associations.** Six products of identical shape — `overall`,
`by_datatype`, `by_datasource`, each in a `direct` and `indirect` flavour
(`indirect` propagates evidence up the disease ontology). Each emits `associations`
(keyed target+disease, with the datatype/datasource in `aggregation_value` for the
non-overall variants) plus `associations_timeseries` (per-year score/novelty/
evidence points): `opentargets_association_overall_direct`,
`opentargets_association_overall_indirect`,
`opentargets_association_by_datatype_direct`,
`opentargets_association_by_datatype_indirect`,
`opentargets_association_by_datasource_direct`,
`opentargets_association_by_datasource_indirect`.

**Evidence.** Twenty `opentargets_evidence_<source>` products, each keyed by the
evidence record `id`, sharing a common `evidence` parent table (provenance envelope
plus the source-specific scalar / list-scalar columns) and adding child tables only
where the source carries `List(Struct)` fields: `cancer_biomarkers` (+ `evidence_urls`,
`evidence_biomarkers_gene_expression`, `evidence_biomarkers_genetic_variation`),
`cancer_gene_census` (+ `evidence_mutated_samples`), `clingen` (+ `evidence_urls`),
`clinical_precedence`, `crispr` (+ `evidence_disease_cell_lines`), `crispr_screen`,
`europepmc` (+ `evidence_sentences`), `eva`, `eva_somatic`, `expression_atlas`,
`gene2phenotype`, `gene_burden` (+ `evidence_urls`), `genomics_england`,
`gwas_credible_sets`, `impc` (+ `evidence_model_phenotypes`,
`evidence_human_phenotypes`), `intogen` (+ `evidence_mutated_samples`), `orphanet`,
`reactome` (+ `evidence_pathways`), `uniprot_literature`, `uniprot_variants`.

**Genetics (GWAS / variants / QTLs).**

- `opentargets_study` (key study ID): `studies` plus `studies_discovery_samples`,
  `studies_replication_samples`, `studies_ld_populations`, `studies_sumstat_qc`.
- `opentargets_variant` (key variant ID, `chrom_pos_ref_alt`): `variants` plus
  `variants_effects`, `variants_transcript_consequences`,
  `variants_allele_frequencies`, `variants_db_xrefs`.
- `opentargets_credible_set` (key study-locus ID): `credible_sets` plus
  `credible_sets_locus` (member variants with posterior probabilities) and
  `credible_sets_ld` (LD tag variants).
- `opentargets_colocalisation` (key left+right study-locus): `colocalisations`
  (pairwise COLOC/eCAVIAR results between credible sets).
- `opentargets_l2g_prediction` (key study-locus + gene): `l2g_predictions` plus
  `l2g_prediction_features` (per-prediction SHAP feature contributions).
- `opentargets_enhancer_to_gene` (key interval ID): `enhancer_gene_predictions`
  plus `enhancer_gene_prediction_scores`.

**Interactions and literature.**

- `opentargets_interaction` (key A+B+source): `interactions` (aggregated molecular
  interactions; species structs unnested into prefixed columns).
- `opentargets_interaction_evidence` (keyless): a single `interaction_evidences`
  table; species/resource/tissue structs are unnested into prefixed columns and the
  participant-detection-method struct lists are flattened into parallel list columns.
- `opentargets_literature` (key publication + keyword): `literature`. `pmid` is a
  documented plain string, not a `PubmedId`, because Europe PMC contributes non-PubMed
  `IND...` identifiers.
- `opentargets_literature_vector` (key category + word): `literature_vectors`
  (word2vec embeddings; the embedding stays a list-of-float column).

As with PubTator, the many machine-generated category columns (biotype, GO aspect
and evidence, homology type, tractability modality, drug type, aggregation type,
datasource/datatype IDs, clinical stage, etc.) are documented free strings rather
than `allowed_values`-constrained, so a new value in a future release does not abort
a run.

MeSH — Medical Subject Headings (`mesh`, pandas; the four NLM yearly XML files:
`desc` descriptors, `qual` qualifiers, `supp` Supplementary Concept Records,
`pa` pharmacological actions). All fit in memory; `extract` walks each file once
with `iterparse`, `transform` is a no-op, `load` validates and writes. Twenty
tables — a parent per record type plus its record-type-specific children, and
**four shared concept tables** (every record type carries the same `ConceptList`
substructure, so concepts/terms/relations/registry-numbers are emitted once,
keyed by `record_ui` with a `record_type` ∈ {descriptor, qualifier, supplemental}
discriminator, rather than duplicated three times). UIs are typed (see the
identifier contract); `record_ui` stays a plain string because it mixes `D…`/`Q…`/
`C…`. Dates are `DateIntroduced`/`LastUpdated` (other MeSH dates are term-level).
Per-term source thesaurus IDs are kept as a pipe-delimited string; the SCR
heading-mapping `*` primary marker is parsed into an `is_primary` flag with the
`*` stripped off the descriptor UI.

- **Descriptors** (main headings): `descriptors` (UI, name, class, dates, notes)
  plus `descriptor_tree_numbers`, `descriptor_allowable_qualifiers`,
  `descriptor_pharmacological_actions`, `descriptor_previous_indexing`,
  `descriptor_see_related`, and `descriptor_entry_combinations` (ECIN→ECOUT
  descriptor+qualifier mappings).
- **Qualifiers** (subheadings): `qualifiers` plus `qualifier_tree_numbers`.
- **Supplementary Concept Records**: `supplemental_records` plus
  `supplemental_heading_mapped_to` (descriptor/qualifier the SCR maps to, with
  `is_primary`), `supplemental_indexing_information`,
  `supplemental_pharmacological_actions`, `supplemental_previous_indexing`,
  `supplemental_sources`.
- **Pharmacological actions**: `pharmacological_actions` (one row per action
  descriptor × substance; `substance_ui` mixes `D…`/`C…` so it is a plain string).
- **Shared concept substructure**: `concepts`, `concept_terms` (entry
  terms/synonyms), `concept_relations` (NRW/BRD/REL between concepts), and
  `concept_related_registry_numbers`.

Alliance of Genome Resources (`alliancegenome`, pandas; one release directory
holding many products, each a directory of gzipped TSVs split by member database
/ species plus an all-species `COMBINED` rollup). One module, nine tables — the
`COMBINED` rollup per product, except `gene_descriptions`, which has no `COMBINED`
file and so concatenates the per-species shards. Each table is a faithful
one-row-per-source-line projection. The canonical **gene** columns are typed
`AllianceGeneId` (the eight-prefix Alliance gene curie; see the identifier
contract): `orthology.gene1_id`/`gene2_id`, `expression.gene_id`,
`gene_descriptions.gene_id`, and `variant_alleles.allele_associated_gene_id`/
`variant_affected_gene_id`. Other identifier columns are heterogeneous CURIEs
(`DOID:`, `UniProtKB:`, allele/model IDs, the PSI-MITAB `entrez gene/locuslink`/
`flybase`/`wormbase` interactor forms, and `gene_cross_references.gene_id` /
`disease_associations.db_object_id`, which also hold non-gene IDs) with no single
canonical form, so they stay documented **plain strings** (the Open Targets
disease-ID precedent), `required` where they are a grain key. Species taxon
columns, written uniformly as `NCBITaxon:<id>`, are stripped of the prefix and
typed `NcbiTaxId` (`tax_id` / `gene1_tax_id` / `gene2_tax_id`). Multi-valued attribute fields (synonyms,
qualifier-ID lists, HGVS names, references) are kept as their **raw delimited
strings** (`|`- or `,`-separated) rather than exploded into child tables, so the
grain stays one row per source line. `-` and empty strings are the source null
markers and become null, except where the source overloads `-` as a real value
(handled before that pass): the variant annotation-presence flags use `yes`/`-`,
mapped to `boolean` with `-` = False.

- `orthology` (ORTHOLOGY-ALLIANCE) — cross-species ortholog gene pairs: the two
  genes (id/symbol/`tax_id`/species), the supporting `algorithms` (pipe list),
  `algorithms_match` / `out_of_algorithms` counts (`Int64`), and the
  `is_best_score` / `is_best_reverse_score` labels (`allowed_values` ∈ {`Yes`,
  `No`, `Yes_Adjusted`}).
- `disease_associations` (DISEASE-ALLIANCE) — gene / allele / affected-genomic-model
  → Disease Ontology (`do_id`) associations, with `association_type`, ECO
  `evidence_code`, `reference`, and provenance. The single-valued `reference` is
  mirrored into a typed `pubmed_id` (`PubmedId`) when it is a `PMID:` citation.
  (`expression.reference` is *not* given a `pubmed_id`: it is multi-valued — up to
  ~140 PMIDs per row — so its PMIDs stay in the raw delimited `reference` string.)
- `expression` (EXPRESSION-ALLIANCE) — gene expression annotations (assay, stage,
  anatomy / cellular-component / substructure terms with their qualifier lists).
- `variant_alleles` (VARIANT-ALLELE, from the nested `4.0.0/…/COMBINED` rollup) —
  alleles and variants per gene: identifiers/symbols/synonyms, associated and
  affected genes, SO variant type, assembly coordinates (`start_position` /
  `end_position` `Int64`), HGVS names, and the `has_disease_annotations` /
  `has_phenotype_annotations` boolean flags.
- `gene_descriptions` (GENE-DESCRIPTION-TSV, per-species shards concatenated;
  headerless) — one automated Alliance gene description per gene.
- `gene_cross_references` (GENECROSSREFERENCE) — Alliance gene ID → external
  identifier (`cross_reference_id`, URL, resource page, `tax_id`).
- `uniprot_cross_references` (CROSSREFERENCEUNIPROT; headerless two-column) —
  UniProtKB identifier → external cross-reference.
- `genetic_interactions` (INTERACTION-GEN) and `molecular_interactions`
  (INTERACTION-MOL) — PSI-MITAB 2.7 (42 raw columns; the column header lives in the
  comment block, the data is headerless). Fields are heterogeneous
  controlled-vocabulary CURIEs (`psi-mi:"MI:nnnn"(label)`, pipe-delimited lists)
  kept as documented plain strings; only `negative` is a boolean. Because MITAB
  carries bare `"` as data, these (and the other reports) are read with quoting
  disabled — except `variant_alleles`, whose source uses real CSV quoting to wrap
  the rare symbol containing a literal tab. The raw interactor IDs are not Alliance
  curies (they are `entrez gene/locuslink:`, `flybase:`, `wormbase:`, plus
  `uniprotkb:`/`refseq:`/… in the molecular table), so four typed columns are
  derived alongside the raw ones: `interactor_a_gene_id`/`interactor_b_gene_id`
  (`AllianceGeneId`, resolving `flybase:`→`FB:`, `wormbase:`→`WB:`, and `entrez
  gene/locuslink:`→the curie via gene_info's `AllianceGenome:` dbXref; null for
  non-gene IDs — populated for 100% of genetic and 88% of molecular interactors),
  the MITAB `taxid:nnnn(label)` strings parsed in place to `interactor_a_taxid`/
  `interactor_b_taxid` (`NcbiTaxId`; `host_organisms` keeps its raw string because
  it carries MITAB's negative in-vitro/chemical taxids), and `pubmed_id`
  (`PubmedId`, the first `pubmed:` token of `publication_ids`). Resolving the
  entrez interactors is why `alliancegenome` `depends_on` `ncbi_gene_info`.

OLS — Ontology Lookup Service (`ols`, streaming; the EMBL-EBI OLS4 ontology dumps,
one ~10 GB tarball of 342 "linked" JSON files, ~269 GB uncompressed, one file
(NCBITaxon) 71 GB). Each ontology JSON is read once straight out of the tarball
with an `ijson` event router that reconstructs each entity (class / property /
individual) on the fly plus the ontology's scalar metadata — never loading a whole
file into memory. Output uses the OpenAlex sharded layout
(`<dataset>/<table>/<ontology>_<part>.parquet` + one `<table>.yml`); each ontology
is written as immutable shards with a done-marker, so an interrupted run resumes.
OLS4 wraps every annotation value as `{"type": [...], "value": "…"}` (or a list, or
a bare IRI string); these are unwrapped to their string value(s). IRIs, CURIEs and
short forms are heterogeneous across 342 ontologies, so they are documented plain
strings (the PubTator precedent). Eight tables:

- `ontologies` — one row per ontology (id, iri, title, description, version IRI,
  declared class/property/individual counts).
- `terms` — one row per class (ontology_id, iri, curie, short_form, label,
  definition, `is_obsolete`, `is_defining_ontology`). Each file also carries
  *imported* classes referenced from other ontologies, so a term can appear in
  several ontologies' shards; `is_defining_ontology` flags the home ontology.
- `term_parents` — one row per direct subClassOf edge (term_iri → parent_iri).
- `term_synonyms` — one row per (term, synonym, `synonym_type` ∈ exact/related/
  narrow/broad), from the oboInOwl synonym properties.
- `term_xrefs` — one row per term database cross-reference (oboInOwl hasDbXref).
- `properties` — one row per property (with `property_type` ∈ objectProperty/
  datatypeProperty/annotationProperty).
- `individuals` — one row per named individual.

Human Disease Ontology (`disease_ontology`, pandas; the single OBO-format
`doid.obo` release, ~14.7k `[Term]` stanzas). Small enough for memory, so it is an
in-memory pandas pipeline: `extract` walks the file once into per-table row lists,
`transform` is a no-op (faithful projection), `load` validates and writes. The
native term ID is the `DOID:` curie (`DiseaseOntologyId`), which keys the parent and
every term-referencing edge; external cross-references stay plain strings (see the
identifier contract). DO uses only `is_a` for its term graph (no `relationship`
stanzas), so `term_parents` is the single hierarchy edge table; ancestors/children
are derivable and not materialised. The definition's provenance refs and the
miscellaneous header annotations are dropped. Ten tables — the parent plus nine
child tables keyed by `doid`:

- `terms` — one row per term: `name`, `definition` (null where absent),
  `is_obsolete`, `comment`, `created_by`, `creation_date`.
- `term_alt_ids` — one row per `(doid, alt_id)`: a secondary/merged DOID that
  resolves to the term (analogous to `gene_history`/`taxonomy_merged`).
- `term_parents` — one row per direct `is_a` edge (`doid` → `parent_id`).
- `term_synonyms` — one row per `(doid, synonym)`: the `scope` (`EXACT`, `BROAD`,
  `NARROW`, `RELATED`, `allowed_values`-constrained) and the optional `synonym_type`
  curie (e.g. `OMO:0003012` = acronym, else null).
- `term_xrefs` — one row per `(doid, xref)`: an external cross-reference CURIE,
  kept verbatim.
- `term_subsets` — one row per `(doid, subset)`: a DO slim the term belongs to.
- `term_skos_matches` — one row per `(doid, match_type, match_id)`: the SKOS
  `property_value` mappings, `match_type` ∈ {`exactMatch`, `broadMatch`,
  `closeMatch`, `narrowMatch`, `relatedMatch`}, `match_id` the matched external
  CURIE (verbatim).
- `term_disjoint_from` — one row per `disjoint_from` edge (`doid` → `disjoint_from_id`).
- `term_replaced_by` — one row per obsolete term's definitive replacement
  (`doid` → `replaced_by_id`).
- `term_consider` — one row per obsolete term's suggested alternative
  (`doid` → `consider_id`).

Human Protein Atlas (`proteinatlas`, pandas; the single wide zipped TSV
`proteinatlas.tsv.zip`, ~20.2k rows × 119 columns, one row per protein-coding gene).
It fits in memory, so it is an in-memory pandas pipeline: `extract` reads the TSV
and reshapes it, `transform` is a no-op, `load` validates and writes. The grain key
is the Ensembl gene ID (`EnsemblGeneId`; human-only, so always `ENSG…`). The parent
`genes` table keeps the gene-level scalars; the wide column families are reshaped
into tidy child tables keyed by `ensembl_gene_id`. External identifiers other than
the Ensembl key and UniProt accession (antibody IDs, RRIDs, expression-cluster
labels) are heterogeneous and stay documented plain strings. Twelve tables — the
parent plus eleven children:

- `genes` — one row per gene: symbol, description, chromosome, `position_start` /
  `position_end` (parsed from the `start-end` range), the four evidence levels,
  secretome location/function, `ccd_protein` / `ccd_transcript` (booleans; the
  source `NA` → null), the two blood-concentration columns (pg/L, `Int64`), the five
  RNA expression-cluster labels, `rna_tissue_cell_type_enrichment` (kept verbatim —
  the `tissue - cell type` tokens are not split, as either side may contain ` - `),
  `n_interactions`, and the three reliability scores.
- `gene_synonyms`, `gene_uniprot` (`uniprot_accession` typed `UniprotAccession`),
  `gene_protein_classes`, `gene_biological_processes`, `gene_molecular_functions`,
  `gene_disease_involvement` — the comma-separated list fields, one value per row.
- `gene_subcellular_locations` — one row per immunofluorescence location, with
  `location_class` ∈ {`main`, `additional`} (from the two source columns; the
  redundant union column is not re-ingested).
- `gene_antibodies` — one row per `(gene, antibody)`, with the antibody `rrid` when
  the release assigns one (blank in v25.1).
- `expression_specificity` — the `specificity` / `distribution` / `specificity_score`
  triple that the source repeats across 11 RNA contexts (tissue, single cell, single
  cell type group, single nuclei brain, cancer, brain regional, blood cell, blood
  lineage, cell line, mouse/pig brain regional) and 2 protein contexts (cell type,
  tissue), unified into one long table with a `modality` ∈ {`RNA`, `protein`} +
  `context` discriminator rather than ~52 parallel parent columns. A row is emitted
  per gene × context only where the source carries any of the three values.
- `specific_expression` — the matching `specific <unit>` maps (`sample: value;…`, the
  per-sample elevated expression) exploded to one row per `(gene, modality, context,
  sample)`, with `value` (`Float64`) and `unit` ∈ {`nTPM`, `nCPM`, `pTPM`,
  `Intensity`}.
- `cancer_prognostics` — the 31 per-cancer prognostics columns reshaped long: one row
  per `(gene, cancer, dataset)` with `dataset` ∈ {`TCGA`, `validation`} (from the
  column header), `prognostic_type` (the call, e.g. `unprognostic`, `validated
  prognostic favorable` — a documented free string) and the parsed `p_value`
  (`Float64`).

`specificity`, `distribution`, `prognostic_type`, and the evidence/reliability/
cluster strings are documented free strings rather than `allowed_values`-constrained:
they are curated HPA category labels whose value set can grow between releases, so a
new value should not abort a run (the PubTator/Open Targets precedent). The
discriminators this pipeline itself generates — `modality`, `unit`, `location_class`,
`dataset` — are `allowed_values`-constrained.

Unknome (`unknome`, pandas; the Unknome database — Rocha et al., PLoS Biol 2023 —
which clusters eukaryotic proteins into PANTHER-based ortholog groups and scores
each by "knownness", 0 = completely uncharacterised, to surface conserved-but-
unstudied proteins). Two gzipped TSVs whose snake_case headers map straight
through, into two tables joined on `cluster_id`:

- `proteins` — one row per UniProt entry × accession (the source's `;`-delimited
  accession list is exploded so each `uniprot_accession` — typed
  `UniprotAccession` — is its own row; an entry with N accessions yields N rows):
  `uniprot_name`, `knownness`, `gene_name`/`protein_name`, `taxon_id` (`NcbiTaxId`),
  `species`, and the `cluster_id` / `panther_group` it belongs to.
- `clusters` — one row per cluster (`cluster_id`): `panther_id`, `cluster_name`,
  `knownness`, `num_proteins` / `num_species`, the best-known member
  (`best_known_protein_id`/`_gene`/`_name`), and `key_protein_ids` /
  `key_protein_xrefs`. `cluster_id` / `panther_group` are documented plain strings.

Crosswalks (pandas; derived from already-produced ETL outputs, not from raw
snapshots):

- `pmid_openalex` → `pmid_openalex` (one row per `(pmid, work_id)` link, with
  `match_source` recording which rule produced it: `pmid`, `doi`,
  `doi_versioned`, `pmcid`, or `title_year`). Both upstreams now store the
  article-level DOI, so a single exact-DOI match links versioned variants of the
  same article directly; that match is tagged `doi_versioned` rather than `doi`
  when either side carried a publisher version suffix (read from the upstream
  `doi_versioned` columns), else `doi`. `depends_on` both `ncbi_pubmed` and
  `openalex_works`; unmatched PMIDs are not in the table, so consumers left-join
  from `articles` when they need them.
- `ncbi_gene2_alliance` → `ncbi_gene2_alliance` (one row per `(entrez_id,
  alliance_gene_id)`, with `tax_id`). Unlike `pmid_openalex`, the link is
  authoritative, not heuristic: NCBI's `gene_info` cross-references the Alliance
  gene directly in `db_xrefs` as `AllianceGenome:<curie>`, so the crosswalk strips
  that prefix to recover the `AllianceGeneId`. `depends_on` both `ncbi_gene_info`
  (the link source, Entrez IDs already current via gene_history) and
  `alliancegenome` (every curie is confirmed against the union of the Alliance
  outputs' gene columns; links to a curie absent from the release are dropped and
  the count logged). Genes without an `AllianceGenome:` xref are not in the table,
  so consumers left-join from `gene_info`.

Research-integrity lists (pandas; small in-memory CSV sources):

- `predatory_journals` → `predatory_journals` and `predatory_publishers` →
  `predatory_publishers` (Beall's-list-style name lists). The source is a
  headerless two-column CSV (`row number, name`); the cleaned output is a single
  `title` column, one row per distinct name.
- `retractionwatch_hijackedjournals` → `hijacked_journals` (one row per
  hijacked/clone record: `record_id`, hijacked vs. original titles and URLs) and
  `hijacked_journal_issns` (one row per `(record_id, role, issn)`; the source's
  comma-separated ISSN fields are split and normalized to canonical ISSN form,
  with `role` ∈ {`hijacked`, `original`}). The source CSV's first record is a
  donation banner, so the real header is its second record.
- `retractionwatch_retractiondatabase` → parent `retractions` (one row per
  Retraction Watch `record_id`: titles, journal/publisher, dates, retraction and
  original-paper DOIs/PMIDs, `retraction_nature`, `article_type`, `paywalled`,
  `notes`) plus child tables that explode the source's `;`-delimited fields, each
  keyed by `record_id`: `retraction_reasons`, `retraction_subjects`,
  `retraction_authors`, `retraction_countries`, `retraction_institutions`,
  `retraction_urls`. PubMed IDs use `0`/blank for "missing" (mapped to null);
  DOIs use `unavailable`/blank for "missing" and are repaired to canonical form,
  with unrecoverable values set to null; `retraction_doi` and `original_paper_doi`
  are article-level, each with a `*_doi_versioned` sibling. `record_id` is a positive `Int64`
  (`RetractionWatchId`), a dataset-internal key not cross-referenced elsewhere;
  a small fraction of source rows (~0.4%) carry no Record ID and, since it is the
  parent key and every child's join key, those rows are excluded with the count
  reported at transform time rather than dropped silently or given a fabricated id.

## Invariants the ETL guarantees

A consumer may rely on all of the following for any successfully produced table:

- **Identifier columns conform to their canonical form.** Validation runs before
  load; a violation aborts the run, so malformed identifiers never reach output.
- **`required` columns contain no nulls.**
- **Numeric IDs are positive `Int64`.**
- **Entrez Gene IDs are current.** Discontinued IDs are remapped to their
  replacement via `ncbi_gene_history`; an ID discontinued *without* a replacement
  is a hard error, never a silent drop. So a Gene ID in any gene output is a live
  ID, and prerequisites must be run first.
- **DOIs carry no URL prefix and no version suffix; ORCIDs are checksum-valid or
  null; OpenAlex IDs are short form.** A `doi` column is always the article-level
  DOI (validation rejects a value carrying a recognised publisher version suffix);
  the versioned form, when present, is in the sibling `<col>_versioned` column
  (`DoiVersioned`). (See the identifier contract.)
- **iCite PMIDs are unique**, and the output column set matches `SCHEMA` exactly
  (an unexpected source column aborts the run rather than passing through).
- **PubMed reflects "latest wins."** NLM's load order is encoded in the filename
  integer `NNNN`; every row is tagged with that `file_seq`, and transform keeps
  only each PMID's max-`file_seq` version. Child-table rows are versioned with
  their parent, so a revised article fully replaces its prior children. PMIDs
  listed in any `DeleteCitation` are anti-joined out of every table.
- **Categorical columns honor their `allowed_values`** (for example GO aspect,
  gene nomenclature status, OpenAlex authorship position).
- **Every output table has a sidecar `.yml`** describing its columns.

## Assumptions

- The latest `<version>` directory under each dataset is the one to process;
  versions sort lexicographically into chronological order (as `data_downloader`
  names them).
- Source layouts are stable per provider: NCBI tab files use `-` for null;
  OpenAlex is gzipped JSONL under `data/<entity>/updated_date=*/part_*.gz`;
  PubMed is gzipped MEDLINE XML in `baseline/` then `updatefiles/`.
- Validation is inline in the pipelines, by design: a run that completes is a
  run whose contract held. There is no separate unit-test suite asserting the
  contract; the pipelines assert it on every run.

## Changing the contract

The schema is the product. Adding, removing, renaming, or retyping an output
column, changing an identifier's canonical form, or changing a table's grain is a
**contract change** that downstream repos depend on. Update the relevant module's
`ColumnSpec`/`SCHEMA`/`TABLES_DOC` (which regenerates the sidecar) *and* this
document in the same change, and treat it as a breaking change for consumers.
