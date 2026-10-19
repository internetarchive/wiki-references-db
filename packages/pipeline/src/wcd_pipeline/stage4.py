"""Stage 4: semantic normalization — ``normalized_citations`` → ``normalized_citation_parts``.

For each normalized citation (Postgres), the driver

4.1 takes the citation's *content*: the inside of the ``<ref>`` wrapper for
    ``ref_tag`` / ``template`` / ``bare_url``, the text after the list marker
    for ``endnote``, the text itself for ``shorthand`` / ``external_link``;
4.2 splits a bullet block of several citations into parts
    (``wcd_semantic.content.iter_citations``), else one part;
4.3 types each part: ``template`` when the UVA parser recognises a citation
    template, ``external_link`` for an ``external_link`` citation, else
    ``ref_tag``; a part the footnote filter rejects is kept as a ``ref_tag``
    part with ``norm_subtype = 'noise'`` so every citation has ≥ 1 part;
4.4 runs the matching normalizer (``wcd_semantic.template.extract_template``
    or ``wcd_semantic.reftag.classify``);
4.5 writes the parts (``part_sha1 = sha1(content)``) — one DELETE + INSERT
    per batch of citations, so a re-run replaces its output.

``wcd parts [--domain D] [--resume] [--batch N]``. Pure-Python work per
citation; the unique-content cache keeps repeated contents cheap.
"""
from __future__ import annotations

import argparse
import re
import time
from functools import lru_cache

import psycopg2
import psycopg2.extras
from refs_extractor.article import strip_ref_wrapper
from wcd_schema.ids import part_sha1
from wcd_semantic.content import iter_citations
from wcd_semantic.reftag import classify
from wcd_semantic.template import extract_template

from .config import load_dotenv, psycopg_dsn  # noqa: F401

_LIST_MARKER_RE = re.compile(r"^[ \t]*[*#:;]+[ \t]*")
_URL_RE = re.compile(r"(?:git|https?|ftps?)://[^\s\]\|\}<]+")

PART_COLUMNS = (
    "normalized_id", "part_index", "part_sha1", "content", "part_type",
    "norm_subtype", "norm_content", "anchor_key", "anchor_label", "url_anchor_text", "page_ref",
    "template_name", "template_subtype", "params_json",
    "author_last", "author_first", "journal", "date", "publisher", "location", "edition", "volume", "issue", "pages",
    "author_key", "title", "year", "year_reliable",
    "extracted_doi", "extracted_isbn", "extracted_pmid", "extracted_pmc", "extracted_arxiv",
    "extracted_wikidata_qid", "extracted_librarybase_id", "extracted_url", "is_archive_url",
    "id_tier", "id_tier_label",
)


def log(msg: str) -> None:
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} [parts] {msg}", flush=True)


# ---------------------------------------------------------------------------
# 4.1 content extraction
# ---------------------------------------------------------------------------

def content_of(reference_normalized: str, citation_type: str) -> str:
    if citation_type in ("ref_tag", "template", "bare_url"):
        return strip_ref_wrapper(reference_normalized).strip()
    if citation_type == "endnote":
        return _LIST_MARKER_RE.sub("", reference_normalized, count=1).strip()
    return reference_normalized.strip()


# ---------------------------------------------------------------------------
# 4.2–4.4 parts
# ---------------------------------------------------------------------------

def _template_part(parsed: dict) -> dict:
    raw = parsed["raw"]
    t = extract_template(raw, parsed["template_name"], parsed["params"])
    return {
        "content": raw, "part_type": "template",
        "template_name": t["template_name"], "template_subtype": t["template_subtype"],
        "params_json": parsed["params"],
        "author_last": t["author_last"], "author_first": t["author_first"], "journal": t["journal"],
        "date": t["date"], "publisher": t["publisher"], "location": t["location"], "edition": t["edition"],
        "volume": t["volume"], "issue": t["issue"], "pages": t["pages"],
        "author_key": t["author_key"], "title": t["title"], "year": t["year"], "year_reliable": None,
        "extracted_doi": t["extracted_doi"], "extracted_isbn": t["extracted_isbn"], "extracted_pmid": t["extracted_pmid"],
        "extracted_pmc": t["extracted_pmc"], "extracted_arxiv": t["extracted_arxiv"],
        "extracted_wikidata_qid": t["extracted_wikidata_qid"], "extracted_url": t["extracted_url"],
        "is_archive_url": t["is_archive_url"], "id_tier": t["id_tier"], "id_tier_label": t["id_tier_label"],
    }


def _reftag_part(content: str, part_type: str, noise: bool = False) -> dict:
    r = classify(content)
    url = r.get("extracted_url")
    if url is None and part_type == "external_link":
        m = _URL_RE.search(content)
        url = m.group(0) if m else None
    return {
        "content": content, "part_type": part_type,
        "norm_subtype": "noise" if noise else r["norm_subtype"], "norm_content": r["norm_content"],
        "anchor_key": r.get("anchor_key"), "anchor_label": r.get("anchor_label"),
        "url_anchor_text": r.get("url_anchor_text"), "page_ref": r.get("page_ref"),
        "author_key": r.get("author_key"), "title": r.get("title"), "year": r.get("year"),
        "year_reliable": r.get("year_reliable"),
        "extracted_doi": r.get("extracted_doi"), "extracted_isbn": r.get("extracted_isbn"),
        "extracted_pmid": r.get("extracted_pmid"), "extracted_pmc": r.get("extracted_pmc"),
        "extracted_arxiv": r.get("extracted_arxiv"), "extracted_wikidata_qid": r.get("extracted_wikidata_qid"),
        "extracted_url": url, "is_archive_url": r.get("is_archive_url"),
        "id_tier": r.get("id_tier"), "id_tier_label": r.get("id_tier_label"),
    }


@lru_cache(maxsize=200_000)
def _parts_for_content(content: str, citation_type: str) -> tuple:
    """The parts of one content string, as a tuple of dicts (cached: the same
    content recurs across pages)."""
    ref_part_type = "external_link" if citation_type == "external_link" else "ref_tag"
    parts = []
    for parsed in iter_citations(None, content):
        if parsed["citation_type"] == "template":
            parts.append(_template_part(parsed))
        else:
            parts.append(_reftag_part(parsed["content"], ref_part_type))
    if not parts:
        # footnote filter rejected it (or empty content): keep it, typed as noise
        parts.append(_reftag_part(content, ref_part_type, noise=True))
    return tuple(parts)


def parts_of(normalized_id: int, reference_normalized: str, citation_type: str) -> list[dict]:
    content = content_of(reference_normalized, citation_type)
    out = []
    for i, p in enumerate(_parts_for_content(content, citation_type)):
        row = {c: None for c in PART_COLUMNS}
        row.update(p)
        row["normalized_id"] = normalized_id
        row["part_index"] = i
        row["part_sha1"] = part_sha1(p["content"])
        if row["params_json"] is not None:
            row["params_json"] = psycopg2.extras.Json(row["params_json"])
        out.append(row)
    return out


# ---------------------------------------------------------------------------
# 4.5 writer
# ---------------------------------------------------------------------------

_INSERT = f"INSERT INTO normalized_citation_parts ({', '.join(PART_COLUMNS)}) VALUES %s"
_TEMPLATE = "(" + ", ".join(f"%({c})s" for c in PART_COLUMNS) + ")"


def write_batch(cur, rows: list[dict], normalized_ids: list[int]) -> None:
    cur.execute("DELETE FROM normalized_citation_parts WHERE normalized_id = ANY(%s)", (normalized_ids,))
    if rows:
        psycopg2.extras.execute_values(cur, _INSERT, rows, template=_TEMPLATE, page_size=1000)


def _select(domain: str | None, resume: bool) -> tuple[str, dict]:
    sql = "SELECT nc.id, nc.reference_normalized, nc.citation_type FROM normalized_citations nc"
    params: dict = {}
    where = []
    if domain:
        sql += " JOIN domains d ON d.id = nc.appears_on_domain_id"
        where.append("d.value = %(domain)s")
        params["domain"] = domain
    if resume:
        where.append("NOT EXISTS (SELECT 1 FROM normalized_citation_parts p WHERE p.normalized_id = nc.id)")
    if where:
        sql += " WHERE " + " AND ".join(where)
    return sql + " ORDER BY nc.id", params


def run(dsn: str, domain: str | None = None, resume: bool = False, batch: int = 2000) -> dict:
    """Build parts for the selected normalized citations. Returns counts."""
    read = psycopg2.connect(dsn)
    write = psycopg2.connect(dsn)
    n_cit = n_parts = 0
    t0 = time.time()
    try:
        sql, params = _select(domain, resume)
        with read.cursor(name="stage4_read") as rc, write.cursor() as wc:
            rc.itersize = batch
            rc.execute(sql, params)
            while True:
                chunk = rc.fetchmany(batch)
                if not chunk:
                    break
                rows, ids = [], []
                for nid, text, ctype in chunk:
                    ids.append(nid)
                    rows.extend(parts_of(nid, text, ctype or "ref_tag"))
                write_batch(wc, rows, ids)
                write.commit()
                n_cit += len(ids)
                n_parts += len(rows)
                if n_cit % (batch * 25) == 0:
                    log(f"{n_cit} citations → {n_parts} parts ({n_cit / (time.time() - t0):.0f}/s)")
    except Exception:
        write.rollback()
        raise
    finally:
        read.close()
        write.close()
    return {"citations": n_cit, "parts": n_parts, "seconds": round(time.time() - t0, 1)}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Stage 4: split normalized citations into semantic parts (normalized_citation_parts)")
    ap.add_argument("--domain", help="only citations first seen on this domain (default: all)")
    ap.add_argument("--resume", action="store_true", help="skip citations that already have parts")
    ap.add_argument("--batch", type=int, default=2000, help="citations per transaction (default: 2000)")
    ap.add_argument("--no-gate", action="store_true", help="skip the Stage-4 gate afterwards (gate needs --domain)")
    args = ap.parse_args(argv)
    dsn = psycopg_dsn()
    counts = run(dsn, args.domain, args.resume, args.batch)
    log(f"done: {counts}")
    if not args.no_gate and args.domain:
        from .gates import make_session, report, stage4
        session = make_session()
        try:
            return report("4", stage4(session, args.domain))
        finally:
            session.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
