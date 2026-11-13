"""Redirects from the retired v1 site (the Flask ``/explorer/`` UI and its
``/api/v1/docs``) to the dashboard's report pages and FastAPI's docs.

Page ids and ``normalized_sha1`` values are the same in both databases, so those
links map directly. Template ids are not: the v1 database numbered its
``wiki_templates`` differently. An old template link is translated through the
v1 id → name table when one is configured (``API_LEGACY_TEMPLATE_IDS``, a TSV
``id<TAB>name`` exported from the v1 database); otherwise it lands on a report
page that says the link cannot be followed.

The redirects are 302s on purpose: while the cutover settles, a wrong mapping
can be fixed without browsers having cached it.
"""
from __future__ import annotations

import logging
from pathlib import Path
from urllib.parse import urlencode

from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse
from sqlalchemy import text

log = logging.getLogger("wcd_api")

router = APIRouter(include_in_schema=False)


def load_template_names(path: str | None) -> dict[int, str]:
    """``id<TAB>name`` lines (psql ``\\copy … TO`` text format) → {id: name}."""
    if not path:
        return {}
    names: dict[int, str] = {}
    with open(Path(path), encoding="utf-8") as fh:
        for line in fh:
            old_id, sep, name = line.rstrip("\n").partition("\t")
            if sep and old_id.strip().isdigit():
                # undo psql text-format escapes for the characters template names can contain
                names[int(old_id)] = name.replace("\\t", "\t").replace("\\\\", "\\")
    log.info("wcd_api: %d legacy template ids from %s", len(names), path)
    return names


def _to(path: str, **params) -> RedirectResponse:
    query = urlencode({k: v for k, v in params.items() if v not in (None, "")})
    return RedirectResponse(f"{path}?{query}" if query else path, status_code=302)


@router.get("/explorer")
@router.get("/explorer/")
def explorer_index():
    return _to("/")


@router.get("/explorer/article")
def explorer_article(url: str | None = None):
    return _to("/", url=url)


@router.get("/explorer/citation/{normalized_sha1}/report")
def explorer_citation_report(request: Request, normalized_sha1: str, page_id: int | None = None):
    domain = request.app.state.settings.default_domain if page_id is not None else None
    return _to(f"/citations/{normalized_sha1}", domain=domain, page_id=page_id)


@router.get("/explorer/citation/{normalized_sha1}/other-articles")
def explorer_citation_pages(request: Request, normalized_sha1: str, page_id: int | None = None):
    domain = request.app.state.settings.default_domain if page_id is not None else None
    return _to(f"/citations/{normalized_sha1}/pages", domain=domain, page_id=page_id)


@router.get("/explorer/template/{old_template_id}/report")
def explorer_template_report(request: Request, old_template_id: int, parameter_key: str = "", parameter_value: str = ""):
    key, value = parameter_key.strip(), parameter_value.strip()
    name = request.app.state.legacy_template_names.get(old_template_id)
    new_id = None
    if name is not None:
        with request.app.state.db.connect() as con:
            new_id = con.execute(text(
                "SELECT w.id FROM wiki_templates w JOIN domains d ON d.id = w.domain WHERE d.value = :d AND w.name = :n "
                "ORDER BY w.id LIMIT 1"), {"d": request.app.state.settings.default_domain, "n": name}).scalar()
    if new_id is not None:
        return _to(f"/templates/{new_id}/report", parameter_key=key, parameter_value=value)
    return _to("/templates/report", legacy_id=old_template_id, template_name=name, parameter_key=key, parameter_value=value)


@router.get("/api/v1/docs")
def v1_docs():
    return _to("/docs")


@router.get("/api/v1/openapi.json")
def v1_openapi():
    return _to("/openapi.json")
