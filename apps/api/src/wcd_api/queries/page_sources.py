"""The article page's sources: ``/api/v2/pages/{domain}/{page_id}/sources``.

Each global source cited on the page, with when it was on the page (intervals
over the page's revisions, and the added / removed / restored / form-change
events), every wikitext form it was cited in there, and its uses in the latest
revision.

The sources are chosen and paged in Postgres (``page_source_summary``); then one
DuckDB pass over the page's bucket partition computes, for those sources only:

* presence runs: ``citation_history`` ⋈ ``citation_instances_denorm`` (instance →
  global source) ⋈ the page's ``revisions`` in timestamp order, consecutive
  revision positions grouped into runs;
* forms: per (source, normalized citation), first and last position and the
  instances present in the latest revision;
* named reuses (``named_ref_uses``) in the latest revision, by ref name.

Absences shorter than ``ABSENCE_MERGE_DAYS`` are merged into the surrounding
interval: vandalism blanking a section and its revert minutes later should not
read as "removed, restored".
"""
from __future__ import annotations

from datetime import datetime, timedelta

from sqlalchemy import text

from wcd_semantic.profile import _classify_source_kind

from ..db import Database
from ..models import (
    PageSource, PageSourcesResponse, SourceCounts, SourceEvent, SourceForm, SourceInterval, TemplateUse,
)
from . import BadRequest
from .citations import _templates
from .common import require_domain_id, source_key

ABSENCE_MERGE_DAYS = 7
STATUSES = ("all", "current", "removed")
#: sort → ORDER BY on page_source_summary (the gap-free span stands in for time on page)
SORTS = {
    "longest": "(p.last_seen - p.first_seen) DESC NULLS LAST",
    "uses": "p.n_citations DESC NULLS LAST",
    "recent": "p.first_seen DESC NULLS LAST",
}
KIND_LABELS = {"Books": "Book", "Journals": "Journal article", "News": "News", "Websites": "Web page"}
REFERENCE_TYPE_OTHER = 0     # citation_instances.reference_type: 0 outside <ref>, 1 inline, 2 endnote


def display_wikitext(normalized: str | None) -> str:
    """The stored normalized text, repaired for display: the reftag normalizer drops a
    ref's leading ``<r`` (``ef name="x">…``) and can keep the text that follows the
    closing tag up to the next ``<``. The stored text (and its sha1) is unchanged."""
    s = (normalized or "").strip()
    if s.startswith(("ef ", "ef>")):
        s = "<r" + s
    if s.startswith("<ref") and "</ref>" in s:
        s = s[: s.rindex("</ref>") + len("</ref>")]
    return s


MAX_TEMPLATES_PER_FORM = 5


def _distinct_templates(uses) -> list[TemplateUse]:
    """Template uses of one form, each distinct (name, parameters) once (template_params
    has a row set per offset, so a text repeated in a long list repeats them), at most a few."""
    out, seen = [], set()
    for t in uses:
        key = (t.template_name, tuple(sorted((k, str(v)) for k, v in t.parameters.items())))
        if key not in seen:
            seen.add(key)
            out.append(TemplateUse(template_name=t.template_name, parameters=t.parameters))
        if len(out) == MAX_TEMPLATES_PER_FORM:
            break
    return out


def _form_label(citation_type: str | None, reference_type: int | None, current: bool, source_has_short: bool) -> str:
    if not current:
        return "Earlier form"
    if citation_type == "shorthand":
        return "Short form"
    if source_has_short and reference_type == REFERENCE_TYPE_OTHER:
        return "Bibliography"
    return "Full citation"


def _duck_facts(db: Database, domain: str, page_id: int, gids: list[str]):
    """(revisions [(id, ts)], runs {gid: [(lo, hi)]}, forms rows, reuse rows) for ``gids`` on the page.
    Positions are 0-based indexes into the revisions list."""
    duck = db.duck()
    try:
        db.bind(duck, domain, page_id, ("revisions", "citation_history", "citation_instances_denorm", "named_ref_uses"))
        revs = duck.execute("SELECT revision_id, revision_timestamp FROM revisions WHERE page_id = ? "
                            "ORDER BY revision_timestamp, revision_id", [page_id]).fetchall()
        if not revs or not gids:
            return revs, {}, [], []
        duck.execute("CREATE TEMP TABLE r AS SELECT revision_id, row_number() OVER (ORDER BY revision_timestamp, revision_id) - 1 AS pos "
                     "FROM revisions WHERE page_id = ?", [page_id])
        duck.execute("""CREATE TEMP TABLE i AS
            SELECT DISTINCT citation_instance_id, global_source_id AS g, normalized_id, normalized_sha1, citation_type,
                   reference_type, reference_name
            FROM citation_instances_denorm WHERE page_id = ? AND global_source_id IN (SELECT unnest(?::VARCHAR[]))""",
                     [page_id, gids])
        duck.execute("""CREATE TEMP TABLE pr AS
            SELECT h.citation_instance_id, r.pos FROM citation_history h JOIN r USING (revision_id)
            WHERE h.page_id = ? AND h.citation_instance_id IN (SELECT citation_instance_id FROM i)""", [page_id])
        latest = len(revs) - 1
        runs: dict[str, list[tuple[int, int]]] = {}
        for g, lo, hi in duck.execute("""
                WITH p AS (SELECT DISTINCT i.g, pr.pos FROM pr JOIN i USING (citation_instance_id))
                SELECT g, min(pos), max(pos) FROM (SELECT g, pos, pos - row_number() OVER (PARTITION BY g ORDER BY pos) AS k FROM p)
                GROUP BY g, k ORDER BY g, 2""").fetchall():
            runs.setdefault(g, []).append((int(lo), int(hi)))
        forms = duck.execute("""
            SELECT i.g, i.normalized_sha1, any_value(i.normalized_id), any_value(i.citation_type),
                   min(i.reference_type), any_value(i.reference_name), min(pr.pos), max(pr.pos),
                   count(DISTINCT i.citation_instance_id) FILTER (WHERE pr.pos = ?)
            FROM pr JOIN i USING (citation_instance_id) GROUP BY 1, 2 ORDER BY 1, 7""", [latest]).fetchall()
        # named reuses: <ref name="x" /> in the latest revision, attributed to the source whose instance
        # present there carries that name; first seen = the name's first reuse on the page
        reuses = duck.execute("""
            WITH names AS (SELECT DISTINCT i.g, i.reference_name AS name FROM pr JOIN i USING (citation_instance_id)
                           WHERE pr.pos = ? AND i.reference_name IS NOT NULL AND i.reference_name <> ''),
                 used AS (SELECT n.ref_name AS name, r.pos FROM named_ref_uses n JOIN r USING (revision_id)
                          WHERE n.page_id = ? AND n.ref_name IN (SELECT name FROM names))
            SELECT names.g, names.name, min(used.pos), count(*) FILTER (WHERE used.pos = ?)
            FROM names JOIN used USING (name) GROUP BY 1, 2 ORDER BY 1, 2""", [latest, page_id, latest]).fetchall()
        return revs, runs, forms, reuses
    finally:
        duck.close()


def _intervals(runs: list[tuple[int, int]], revs, current: bool):
    """Merge runs separated by short absences; return (intervals, events, days on page)."""
    ts = lambda pos: revs[pos][1] if 0 <= pos < len(revs) else None   # noqa: E731
    merged: list[list[int]] = []
    for lo, hi in runs:
        if merged:
            gap_start, gap_end = ts(merged[-1][1] + 1), ts(lo)
            if gap_start and gap_end and gap_end - gap_start < timedelta(days=ABSENCE_MERGE_DAYS):
                merged[-1][1] = hi
                continue
        merged.append([lo, hi])
    last = len(revs) - 1
    intervals, events, days = [], [], 0.0
    for n, (lo, hi) in enumerate(merged):
        still = hi == last
        removed_pos = None if still else hi + 1
        intervals.append(SourceInterval(start_revision_id=revs[lo][0], start=ts(lo), end_revision_id=revs[hi][0], end=ts(hi),
                                        removed_revision_id=revs[removed_pos][0] if removed_pos is not None else None,
                                        removed_at=ts(removed_pos) if removed_pos is not None else None))
        events.append(SourceEvent(kind="added" if n == 0 else "restored", revision_id=revs[lo][0], timestamp=ts(lo)))
        if removed_pos is not None:
            events.append(SourceEvent(kind="removed", revision_id=revs[removed_pos][0], timestamp=ts(removed_pos)))
        end_ts = ts(last) if still else ts(removed_pos)
        if ts(lo) and end_ts:
            days += (end_ts - ts(lo)).total_seconds() / 86400
    return intervals, events, days


def _canonical_parts(con, shas: list[str]) -> dict[str, dict]:
    if not shas:
        return {}
    rows = con.execute(text("""
        SELECT DISTINCT ON (part_sha1) part_sha1, author_last, journal, publisher, template_name, template_subtype,
               norm_subtype, part_type
        FROM normalized_citation_parts WHERE part_sha1 = ANY(CAST(:s AS bpchar[])) ORDER BY part_sha1, normalized_id"""),
        {"s": shas}).mappings().all()
    return {r["part_sha1"]: dict(r) for r in rows}


def get_page_sources(db: Database, domain: str, page_id: int, status: str = "all", sort: str = "longest",
                     limit: int = 10, offset: int = 0) -> PageSourcesResponse:
    if status not in STATUSES:
        raise BadRequest(f"status must be one of {', '.join(STATUSES)}")
    if sort not in SORTS:
        raise BadRequest(f"sort must be one of {', '.join(SORTS)}")
    where = {"all": "", "current": "AND p.is_current", "removed": "AND NOT p.is_current"}[status]
    with db.connect() as con:
        domain_id = require_domain_id(db, con, domain)
        n_all, n_cur = con.execute(text(
            "SELECT count(*), count(*) FILTER (WHERE is_current) FROM page_source_summary WHERE domain_id = :d AND page_id = :p"),
            {"d": domain_id, "p": page_id}).one()
        rows = con.execute(text(f"""
            SELECT p.global_source_id, p.author_key, p.year, p.title, p.extracted_doi, p.extracted_isbn, p.extracted_pmid,
                   p.extracted_pmc, p.extracted_arxiv, p.extracted_wikidata_qid, p.extracted_librarybase_id, p.extracted_url,
                   p.source_type, p.match_type, p.first_seen, p.last_seen, p.is_current, gs.n_pages, gs.canonical_part_sha1
            FROM page_source_summary p JOIN global_sources gs USING (global_source_id)
            WHERE p.domain_id = :d AND p.page_id = :p {where}
            ORDER BY {SORTS[sort]}, p.global_source_id LIMIT :l OFFSET :o"""),
            {"d": domain_id, "p": page_id, "l": limit, "o": offset}).mappings().all()
        canon = _canonical_parts(con, sorted({r["canonical_part_sha1"] for r in rows if r["canonical_part_sha1"]}))

    gids = [r["global_source_id"] for r in rows]
    revs, runs, form_rows, reuse_rows = _duck_facts(db, domain, page_id, gids)
    last = len(revs) - 1
    ts = lambda pos: revs[pos][1] if revs and 0 <= pos < len(revs) else None   # noqa: E731
    rid = lambda pos: revs[pos][0] if revs and 0 <= pos < len(revs) else None  # noqa: E731

    # wikitext of every form, and template parameters (as cited on this page)
    shas = sorted({f[1] for f in form_rows if f[1]})
    texts: dict[str, str] = {}
    if shas:
        with db.connect() as con:
            texts = dict(con.execute(text(
                "SELECT normalized_sha1, reference_normalized FROM normalized_citations "
                "WHERE normalized_sha1 = ANY(CAST(:s AS bpchar[]))"), {"s": shas}).all())
    sha_by_id = {f[2]: f[1] for f in form_rows if f[1] and f[2] is not None}
    templates = _templates(db, domain, page_id, sha_by_id) if sha_by_id else {}

    by_source_forms: dict[str, list] = {}
    for g, sha, nid, ctype, rtype, rname, lo, hi, uses in form_rows:
        by_source_forms.setdefault(g, []).append((sha, nid, ctype, rtype, rname, int(lo), int(hi), int(uses or 0)))
    by_source_reuses: dict[str, list] = {}
    for g, name, first, uses in reuse_rows:
        by_source_reuses.setdefault(g, []).append((name, int(first), int(uses or 0)))

    sources = []
    for r in rows:
        g = r["global_source_id"]
        intervals, events, days = _intervals(runs.get(g, []), revs, bool(r["is_current"])) if revs else ([], [], 0.0)
        start_ts = intervals[0].start if intervals else None
        raw_forms = by_source_forms.get(g, [])
        has_short = any(f[2] == "shorthand" for f in raw_forms)
        forms: list[SourceForm] = []
        for sha, nid, ctype, rtype, rname, lo, hi, uses in raw_forms:
            current = hi == last
            forms.append(SourceForm(
                label=_form_label(ctype, rtype, current, has_short), wikitext=display_wikitext(texts.get(sha)), normalized_sha1=sha,
                citation_type=ctype, reference_name=rname, first_revision_id=rid(lo), first_seen=ts(lo),
                last_revision_id=rid(hi), last_seen=ts(hi), current=current, uses_latest=uses,
                templates=_distinct_templates(templates.get(nid, []))))
            # short-form citations arriving well after the source did mark a conversion; other text edits
            # (every edit makes a new normalized form) are shown as earlier forms, not as events
            if (ctype == "shorthand" and start_ts and ts(lo)
                    and ts(lo) - start_ts >= timedelta(days=ABSENCE_MERGE_DAYS)):
                events.append(SourceEvent(kind="form_changed", revision_id=rid(lo), timestamp=ts(lo),
                                          label="converted to short-form citations"))
        for name, first, uses in by_source_reuses.get(g, []):
            forms.append(SourceForm(label="Named reuse", wikitext=f'<ref name="{name}" />', reference_name=name,
                                    first_revision_id=rid(first), first_seen=ts(first), current=True, uses_latest=uses))
        # current forms first (full citations, bibliography, short forms, reuses), then earlier ones; each by first seen
        order = {"Full citation": 0, "Bibliography": 1, "Short form": 2, "Named reuse": 3, "Earlier form": 4}
        forms.sort(key=lambda f: (order.get(f.label, 5), f.first_seen or datetime.min))
        events.sort(key=lambda e: (e.timestamp or datetime.min, e.revision_id))
        # keep one form-change event per date
        seen, deduped = set(), []
        for e in events:
            k = (e.kind, e.timestamp.date() if e.timestamp else None)
            if e.kind == "form_changed" and k in seen:
                continue
            seen.add(k)
            deduped.append(e)
        c = canon.get(r["canonical_part_sha1"]) or {}
        kind = KIND_LABELS.get(_classify_source_kind(
            template_name=c.get("template_name"), template_subtype=c.get("template_subtype"), norm_subtype=c.get("norm_subtype"),
            extracted_isbn=r["extracted_isbn"], extracted_doi=r["extracted_doi"], extracted_pmid=r["extracted_pmid"],
            extracted_pmc=r["extracted_pmc"], extracted_url=r["extracted_url"]))
        author = (c.get("author_last") or r["author_key"] or "").strip() or None
        sources.append(PageSource(
            **{k: r[k] for k in ("global_source_id", "year", "title", "extracted_doi", "extracted_isbn", "extracted_pmid",
                                 "extracted_pmc", "extracted_arxiv", "extracted_wikidata_qid", "extracted_librarybase_id",
                                 "extracted_url", "source_type", "match_type", "first_seen", "last_seen")},
            source_key=source_key(r), author=author.title() if author and author.islower() else author,
            publication=(c.get("journal") or c.get("publisher") or None), kind=kind, is_current=bool(r["is_current"]),
            days_on_page=round(days, 1), uses_latest=sum(f.uses_latest for f in forms if f.current),
            other_pages=max(0, int(r["n_pages"] or 1) - 1), intervals=intervals, events=deduped, forms=forms))

    return PageSourcesResponse(
        domain=domain, page_id=page_id, status=status, sort=sort,
        first_revision_timestamp=ts(0), latest_revision_id=rid(last), latest_revision_timestamp=ts(last),
        counts=SourceCounts(all=int(n_all), current=int(n_cur), removed=int(n_all) - int(n_cur)),
        total={"all": int(n_all), "current": int(n_cur), "removed": int(n_all) - int(n_cur)}[status],
        sources=sources)
