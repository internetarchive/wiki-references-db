from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Connection

from ..models import PageHeader
from . import NotFound

TYPE_LABELS = {0: "other", 1: "inline", 2: "endnote"}


def page_url(domain: str, page_id: int) -> str:
    return f"https://{domain}/w/index.php?curid={page_id}"


def source_label(row) -> str:
    """UVA ``_build_source_label``: title, else DOI, ISBN, URL, else the id."""
    title = row.get("title")
    if title and title.strip():
        return title.strip()
    for key, prefix in (("extracted_doi", "DOI "), ("extracted_isbn", "ISBN "), ("extracted_url", "")):
        v = row.get(key)
        if v and v.strip():
            return prefix + v.strip()
    return f"Source {row['global_source_id']}"


def require_domain_id(db, con: Connection, domain: str) -> int:
    did = db.domain_id(con, domain)
    if did is None:
        raise NotFound(f"Unknown domain {domain!r}")
    return did


def page_header(con: Connection, domain: str, domain_id: int, page_id: int) -> PageHeader:
    """page_metadata + the revisions aggregate (count, first/latest timestamp).
    A page the database does not know is a 404."""
    meta = con.execute(text(
        "SELECT page_title, page_namespace FROM page_metadata WHERE domain_id = :d AND page_id = :p"),
        {"d": domain_id, "p": page_id}).first()
    agg = con.execute(text(
        "SELECT count(*), min(revision_timestamp), max(revision_timestamp) FROM revisions "
        "WHERE domain_id = :d AND page_id = :p"), {"d": domain_id, "p": page_id}).first()
    if meta is None and (agg is None or agg[0] == 0):
        raise NotFound(f"Page {page_id} on {domain} not found")
    return PageHeader(
        domain=domain, page_id=page_id, url=page_url(domain, page_id),
        page_title=meta[0] if meta else None, page_namespace=meta[1] if meta else None,
        revision_count=int(agg[0] or 0), first_revision_timestamp=agg[1], latest_revision_timestamp=agg[2],
    )


def latest_revision(con: Connection, domain_id: int, page_id: int):
    """(revision_id, timestamp) of the page's latest revision by timestamp, or None."""
    return con.execute(text(
        "SELECT revision_id, revision_timestamp FROM revisions WHERE domain_id = :d AND page_id = :p "
        "ORDER BY revision_timestamp DESC, revision_id DESC LIMIT 1"), {"d": domain_id, "p": page_id}).first()
