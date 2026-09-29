# lacuna-etl

Extract, transform, and load pipeline for versioned scientific datasets downloaded by [data_downloader](../data_downloader).

`data_downloader` acquires and versions raw snapshots; `lacuna-etl` turns the latest snapshot of each dataset into cleaned, typed, validated Parquet tables, each with a YAML schema sidecar. Identifier columns share one canonical form across all datasets, so the outputs can be joined safely.

[docs/DESIGN.md](docs/DESIGN.md) is the authoritative contract: the table inventory and grain of every dataset, the canonical form of every identifier, and the invariants the ETL guarantees. The `<table>.yml` sidecars written next to each output are the authoritative column-level schema.

## Setup

**1. Create a Python 3.13 virtual environment and install:**

```bash
uv venv --python 3.13 .venv
uv pip install -e .
```

**2. Configure the data root (where data_downloader stores its files) and the output root:**

```bash
.venv/bin/etl configure /path/to/your/data --output-root /path/to/cleaned/output
```

This is saved to `~/.config/lacuna_etl/config.toml`. You can also set `DL_DATA_ROOT` and `ETL_OUTPUT_ROOT` as environment variables instead.

Intermediate files default to `./intermediate/` in this repo. Override with `ETL_INTERMEDIATE_ROOT` or pass `--intermediate-root` to configure.

## Usage

```bash
# List registered datasets and their dependencies
.venv/bin/etl list

# Run a dataset pipeline (extract → transform → load)
.venv/bin/etl run <dataset>
```

Each dataset is written to `<output_root>/<dataset>/<table>.parquet` with a sibling `<table>.yml`. The large streaming datasets (OpenAlex, Open Targets, InterPro protein matches, UniProt ID mapping, OLS) write sharded tables as `<dataset>/<table>/<part>.parquet` instead.

Dependencies are declared but not orchestrated: `etl run` runs a single dataset, so run its prerequisites first (`etl list` shows them). Most gene tables depend on `ncbi_gene_history`, which remaps discontinued Entrez Gene IDs to their current replacements.

## Datasets

Registered dataset names, grouped by domain. Dataset names ending in `*` stand for a family of pipelines. See [docs/DESIGN.md](docs/DESIGN.md#output-layout-and-table-inventory) for every table and its grain.

### Literature and bibliometrics

| Dataset | Source | Contents |
|---|---|---|
| `ncbi_pubmed` | PubMed / MEDLINE baseline + updates | One current row per PMID (`articles`), plus authors, affiliations, MeSH headings, chemicals, publication types, grants, keywords, article IDs, references, deleted PMIDs |
| `ncbi_pubtator3` | NCBI PubTator3 BioC-XML | Text-mined entity annotations and concept relations over PubMed and PMC full text |
| `ncbi_nlmcatalog_reportedmedline` | NLM Catalog | MEDLINE journals: ISSNs, titles, title lineage, per-year indexing coverage |
| `icite` | NIH iCite | Per-PMID citation metrics and the Open Citation Collection (PMID → PMID) |
| `openalex_*` (21 datasets) | OpenAlex snapshot | Works (with authorships, topics, references, funders, awards), authors, sources, institutions, publishers, funders, awards, the topic hierarchy, and controlled vocabularies |
| `orcid` | ORCID Public Data File | Researcher records, affiliations, fundings, works and their external IDs |
| `ror` | Research Organization Registry | Organizations, names, types, external IDs, relationships, locations |
| `pmid_openalex` | derived (crosswalk) | PMID ↔ OpenAlex work links, tagged by match rule; needs `ncbi_pubmed` and `openalex_works` |

### Research funding

| Dataset | Source | Contents |
|---|---|---|
| `nih_exporter` | NIH ExPORTER | Project-years, abstracts, project ↔ PMID links, publications, patents, clinical studies |
| `nsf_awards` | NSF Awards | Awards, investigators, program elements and references, obligations, funding lines |

OpenAlex funding (`openalex_funders`, `openalex_awards`, and the `works_funders` / `works_awards` tables of `openalex_works`) is listed under literature.

### Research integrity

| Dataset | Source | Contents |
|---|---|---|
| `retractionwatch_retractiondatabase` | Retraction Watch | Retractions with reasons, subjects, authors, countries, institutions, URLs |
| `retractionwatch_hijackedjournals` | Retraction Watch | Hijacked/clone journals and their ISSNs |
| `predatory_journals`, `predatory_publishers` | Beall's-list-style lists | Journal and publisher names |

### Genes and genomes

| Dataset | Source | Contents |
|---|---|---|
| `ncbi_gene_history` | NCBI Gene | Discontinued → replacement Gene ID map (prerequisite for most gene tables) |
| `ncbi_gene_info` | NCBI Gene | One row per gene |
| `ncbi_gene2go`, `ncbi_gene2pubmed`, `ncbi_generifs` | NCBI Gene | Gene ↔ GO, gene ↔ PMID, Gene References into Function |
| `ncbi_gene2ensembl`, `ncbi_gene2accession` | NCBI Gene | Gene ↔ Ensembl and gene ↔ RefSeq/GenBank accessions (reference model organisms) |
| `ncbi_taxdump` | NCBI Taxonomy | Taxonomy nodes, names, merged tax IDs |
| `hgnc` | HGNC | Approved human genes, nomenclature history, gene groups, cross-references |
| `alliancegenome` | Alliance of Genome Resources | Orthology, disease associations, expression, variants/alleles, gene descriptions, cross-references, genetic/molecular interactions |
| `ncbi_gene2_alliance` | derived (crosswalk) | Entrez Gene ID ↔ Alliance gene curie |
| `ensembl_gtf` | Ensembl GTF (release 116) | Genes, transcripts, and exon/CDS/UTR features for five vertebrates |
| `ensembl_tsv` | Ensembl xref TSV dumps | Ensembl ↔ Entrez, RefSeq, UniProt, and ENA mappings for eight reference organisms |
| `omim` | OMIM `mim2gene.txt` | MIM number ↔ Entrez / HGNC symbol / Ensembl |
| `gtex` | GTEx v10 | Median TPM per gene × tissue, and a sample catalogue |
| `proteinatlas` | Human Protein Atlas | Gene-level protein and RNA annotations, subcellular location, expression specificity, cancer prognostics |
| `harmonizome` | Harmonizome | Gene → attribute edges per collection, with gene and attribute universes and a processing report |
| `unknome` | Unknome | Ortholog clusters and proteins scored by "knownness" |

### Proteins, pathways, and interactions

| Dataset | Source | Contents |
|---|---|---|
| `uniprot_fasta` | UniProt Swiss-Prot | Reviewed protein entries with sequences |
| `uniprot_idmapping` | UniProt `idmapping.dat` | UniProt accession ↔ ~100 external databases |
| `interpro` | InterPro 108.0 | Entries, hierarchy, InterPro ↔ GO, protein ↔ entry matches |
| `reactome` | Reactome | Pathways, hierarchy, and Ensembl/UniProt/NCBI → pathway mappings |
| `biogrid_interactions` | BioGRID | Curated genetic and physical interactions |
| `intact` | IntAct | Molecular interactions (PSI-MITAB), including negative results |

### Ontologies and vocabularies

| Dataset | Source | Contents |
|---|---|---|
| `mesh` | NLM MeSH | Descriptors, qualifiers, supplementary concept records, pharmacological actions, concepts and terms |
| `geneontology_basic` | Gene Ontology `go-basic.obo` | Terms, hierarchy, relationships, synonyms, cross-references |
| `disease_ontology` | Human Disease Ontology | Terms, hierarchy, synonyms, cross-references |
| `ols` | EMBL-EBI OLS4 (342 ontologies) | Ontologies, terms, parents, synonyms, xrefs, properties, individuals |

### Disease genetics and drug targets

| Dataset | Source | Contents |
|---|---|---|
| `gwas_catalog` | NHGRI-EBI GWAS Catalog | Studies, ancestry, EFO trait mappings, associations |
| `opentargets_*` (55 datasets) | Open Targets Platform | Targets, diseases, drugs, clinical data, target–disease associations, 20 evidence sources, GWAS studies, variants, credible sets, L2G, interactions, literature |

## Typed identifiers

Every identifier that links datasets is cast through a type in [src/lacuna_etl/core/identifiers.py](src/lacuna_etl/core/identifiers.py). The type fixes the canonical form and validates it. A value that does not conform fails the run; it is never written. Empty strings become null. The main types:

| Type | Canonical form | Example | Main datasets |
|---|---|---|---|
| `PubmedId` | positive integer | `31452104` | PubMed, iCite, PubTator3, NCBI Gene, NIH ExPORTER, Retraction Watch |
| `Doi` | article-level `10.x/...`, no URL, no version suffix | `10.1038/nature12373` | PubMed, OpenAlex, iCite, PubTator3, Retraction Watch |
| `DoiVersioned` | as `Doi`, but may carry a publisher version suffix; sibling `<col>_versioned` column | `10.1101/2020.01.01.123456v2` | same as `Doi` |
| `NcbiGeneId` | positive integer, current (history-remapped) | `7157` | NCBI Gene, HGNC, OMIM, BioGRID, Harmonizome |
| `NcbiTaxId` | positive integer | `9606` | NCBI Gene, taxdump, UniProt, Alliance, BioGRID |
| `HgncId` | `HGNC:\d+` | `HGNC:11998` | HGNC |
| `AllianceGeneId` | `HGNC`/`MGI`/`RGD`/`ZFIN`/`SGD`/`FB`/`WB`/`Xenbase` + `:` + accession | `MGI:98834` | Alliance, `ncbi_gene2_alliance`, HGNC |
| `EnsemblGeneId` / `EnsemblTranscriptId` / `EnsemblProteinId` | unversioned `ENS[A-Z]*G/T/P\d+` | `ENSG00000141510` | Ensembl GTF, Open Targets, Protein Atlas, GTEx, HGNC, OMIM |
| `RefSeqAccession` | `[A-Z]{2}_\d+` with optional `.version` | `NM_000546.6` | NCBI Gene, Ensembl TSV, HGNC |
| `UniprotAccession` | 6- or 10-character UniProtKB accession, no isoform suffix | `P04637` | UniProt, HGNC, Protein Atlas, Unknome |
| `OpenAlexWorkId`, `OpenAlexAuthorId`, … | short form, URL stripped (`W…`, `A…`, `I…`, `S…`, `F…`, `P…`, `T…`, `G…`, …) | `W2741809807` | OpenAlex, `pmid_openalex` |
| `Orcid` | `XXXX-XXXX-XXXX-XXXX`, checksum-verified (invalid → null) | `0000-0002-1825-0097` | ORCID, OpenAlex, PubMed |
| `RorId` | full URL `https://ror.org/...` | `https://ror.org/01an7q238` | ROR, OpenAlex |
| `CrossrefFunderDoi` | `10.13039/\d+` (a funder, never joined to a work DOI) | `10.13039/100000002` | OpenAlex funders |
| `IssnL` | `XXXX-XXXC`, checksum-verified | `0028-0836` | OpenAlex, NLM Catalog, Retraction Watch |
| `NlmUniqueId` | NLM Catalog ID, digits with optional check letter | `2984730R` | NLM Catalog, PubMed |
| `MeshDescriptorId`, `MeshQualifierId`, `MeshSupplementalId`, … | `D\d+`, `Q\d+`, `C\d+`, `M\d+`, `T\d+` | `D009369` | MeSH, PubMed |
| `GoId` | `GO:\d{7}` | `GO:0008150` | Gene Ontology, NCBI gene2go, InterPro, Open Targets |
| `DiseaseOntologyId` | `DOID:\d+` | `DOID:162` | Disease Ontology |
| `EfoId` | `EFO:\d+` | `EFO:0000270` | GWAS Catalog |
| `ChemblId` | `CHEMBL\d+` | `CHEMBL25` | Open Targets |
| `ReactomePathwayId` | `R-[A-Z]{3}-\d+` | `R-HSA-109582` | Reactome, Open Targets |
| `InterProId` | `IPR\d{6}` | `IPR000126` | InterPro |
| `MimNumber` | 6 digits, stored as a string | `191170` | OMIM |
| `GwasStudyAccession` | `GCST\d+` | `GCST000001` | GWAS Catalog |
| `NihCoreProjectNum`, `NsfAwardId` | grant IDs | `R01GM123456` | NIH ExPORTER, NSF Awards |
| `WikidataId` | `Q\d+`, URL stripped | `Q42` | ROR, OpenAlex |
| `CountryCode` / `CountryCodeAlpha3` | ISO 3166-1 alpha-2 / alpha-3 | `US` | ROR, ORCID, OpenAlex |

Identifiers with no single canonical form are kept as documented plain strings rather than forced into a type. Examples are Open Targets disease IDs (`EFO_…`, `MONDO_…`, `HP_…`), PubTator concept IDs, OLS IRIs, and multi-species cross-reference columns.

The full rules, including which publisher DOI version suffixes are recognised, are in the [identifier contract](docs/DESIGN.md#identifier-contract).

## Changing the schema

The output schema is the product that downstream repos consume. Adding, removing, renaming, or retyping a column, changing an identifier's canonical form, or changing a table's grain is a breaking contract change. Such a change must update [docs/DESIGN.md](docs/DESIGN.md) in the same commit.
