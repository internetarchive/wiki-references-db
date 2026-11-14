"""Group 1: ``/article?url=`` — any article URL to ``(domain, page_id)`` + metadata."""
from __future__ import annotations

import re
from urllib.parse import parse_qs, unquote, urlparse

import httpx
from sqlalchemy import text

from ..db import Database
from ..models import ArticleResponse
from . import BadRequest, NotFound, Unavailable
from .common import page_url

_WIKI_PATH = re.compile(r"^/wiki/(.+)$")


def parse_article_url(url: str) -> tuple[str, int | None, str | None]:
    """Returns ``(domain, page_id, title)``: a curid URL gives the id, a
    ``/wiki/Title`` or ``?title=`` URL gives the title (resolved via the site API)."""
    try:
        u = urlparse(url if "://" in url else "https://" + url)
    except ValueError as e:
        raise BadRequest(f"Invalid URL: {e}") from e
    if not u.netloc:
        raise BadRequest("URL has no host")
    domain = u.netloc.lower()
    qs = parse_qs(u.query)
    if "curid" in qs:
        try:
            return domain, int(qs["curid"][0]), None
        except ValueError as e:
            raise BadRequest("curid is not an integer") from e
    if "title" in qs:
        return domain, None, unquote(qs["title"][0]).replace("_", " ")
    m = _WIKI_PATH.match(u.path)
    if m:
        return domain, None, unquote(m.group(1)).replace("_", " ")
    raise BadRequest("URL is neither a curid URL nor a /wiki/Title URL")


def resolve_title(domain: str, title: str, user_agent: str, contact_email: str | None, timeout: float = 10.0) -> int | None:
    """Title → page id through ``https://{domain}/w/api.php`` (follows redirects)."""
    ua = f"{user_agent} ({contact_email})" if contact_email else user_agent
    r = httpx.get(f"https://{domain}/w/api.php",
                  params={"action": "query", "titles": title, "redirects": 1, "format": "json", "formatversion": 2},
                  headers={"User-Agent": ua}, timeout=timeout)
    r.raise_for_status()
    pages = (r.json().get("query") or {}).get("pages") or []
    for p in pages:
        if p.get("pageid") and not p.get("missing"):
            return int(p["pageid"])
    return None


def get_article(db: Database, url: str, resolver=resolve_title) -> ArticleResponse:
    domain, page_id, title = parse_article_url(url)
    if page_id is None:
        try:
            page_id = resolver(domain, title, db.settings.user_agent, db.settings.contact_email)
        except (httpx.HTTPError, ValueError) as e:
            raise Unavailable(f"Could not resolve the title through {domain}: {e}") from e
        if page_id is None:
            raise NotFound(f"No page titled {title!r} on {domain}")
    with db.connect() as con:
        domain_id = db.domain_id(con, domain)
        if domain_id is None:
            raise NotFound(f"Unknown domain {domain!r}")
        meta = con.execute(text("SELECT page_title, page_namespace FROM page_metadata WHERE domain_id = :d AND page_id = :p"),
                           {"d": domain_id, "p": page_id}).first()
        agg = con.execute(text("SELECT count(*), max(revision_id) FROM revisions WHERE domain_id = :d AND page_id = :p"),
                          {"d": domain_id, "p": page_id}).first()
        if meta is None and not agg[0]:
            raise NotFound(f"Page {page_id} on {domain} is not in the database")
        return ArticleResponse(domain=domain, page_id=page_id, url=page_url(domain, page_id),
                               page_title=meta[0] if meta else None, page_namespace=meta[1] if meta else None,
                               revision_count=int(agg[0] or 0), latest_revision_id=agg[1])
