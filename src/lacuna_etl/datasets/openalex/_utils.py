"""Shared helpers for OpenAlex transforms."""


def short_id(url: str | None) -> str | None:
    """Strip 'https://openalex.org/' prefix, e.g. 'https://openalex.org/W123' -> 'W123'."""
    if url is None or not isinstance(url, str):
        return None
    return url.removeprefix("https://openalex.org/")


def short_orcid(url: str | None) -> str | None:
    """Return bare ORCID from 'https://orcid.org/0000-0001-2345-6789'."""
    if url is None:
        return None
    return url.removeprefix("https://orcid.org/") or None


def short_pmid(url: str | None) -> str | None:
    """Return bare numeric PMID from 'https://pubmed.ncbi.nlm.nih.gov/12345'."""
    if url is None:
        return None
    return url.rsplit("/", 1)[-1] or None
