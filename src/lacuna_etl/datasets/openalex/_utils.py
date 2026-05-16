"""Shared helpers for OpenAlex transforms."""

from lacuna_etl.core.identifiers import Orcid


def short_id(url: str | None) -> str | None:
    """Strip 'https://openalex.org/' prefix, e.g. 'https://openalex.org/W123' -> 'W123'."""
    if url is None or not isinstance(url, str):
        return None
    return url.removeprefix("https://openalex.org/")


def short_orcid(url: str | None) -> str | None:
    """Normalize OpenAlex ORCID to canonical 'XXXX-XXXX-XXXX-XXXX' form.

    Recovers the check digit if missing (15-digit input) and rejects values that
    fail the ORCID checksum.
    """
    return Orcid.normalize(url)


def short_pmid(url: str | None) -> str | None:
    """Return bare numeric PMID from 'https://pubmed.ncbi.nlm.nih.gov/12345'."""
    if url is None:
        return None
    return url.rsplit("/", 1)[-1] or None
