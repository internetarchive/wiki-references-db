"""Group 4: template parameter report and web-resource lookup (v1 shapes, extended)."""
from __future__ import annotations

from sqlalchemy import text

from wcd_schema import parquet as layout
from wcd_schema.ids import template_search_bucket
from wcd_schema.postgres import WebResource

from ..db import Database
from ..models import (
    TemplateReportCitation, TemplateReportResponse, WebResourceReference, WebResourceResponse,
)
from . import BadRequest, NotFound
from .common import page_titles, require_domain_id


def normalize_template_name(name: str) -> str:
    """As the extractor stores template names (``wcd_pipeline.extract._normalize_template_name``):
    underscores to spaces, trimmed, first letter capitalized — so ``cite_journal`` finds ``Cite journal``."""
    norm = (name or "").replace("_", " ").strip()
    return norm[:1].upper() + norm[1:]


def get_template_report(db: Database, domain: str, template_name: str, parameter_key: str, parameter_value: str,
                        limit: int = 100, offset: int = 0) -> TemplateReportResponse:
    """Normalized citations whose template ``template_name`` (on ``domain``) has
    ``parameter_key = parameter_value``. Templates are addressed by name: the
    ``wiki_templates.id`` row id depends on load order and is not part of the API.
    The matches come from the one ``template_param_index`` partition the
    (template, key, value) hashes to; the total from ``template_value_counts``
    when precomputed (heavy values), else counted there. Citations are listed in
    ``normalized_sha1`` order."""
    import glob

    name = normalize_template_name(template_name)
    if not name:
        raise BadRequest("template is required")
    # template values are stored trimmed (as the v1 explorer compared them: btrim on both sides)
    parameter_key, parameter_value = parameter_key.strip(), parameter_value.strip()
    with db.connect() as con:
        require_domain_id(db, con, domain)
        # the row id is only an internal join key for template_value_counts
        tmpl_id = con.execute(text(
            "SELECT w.id FROM wiki_templates w JOIN domains d ON d.id = w.domain WHERE d.value = :d AND w.name = :n "
            "ORDER BY w.id LIMIT 1"), {"d": domain, "n": name}).scalar()
        part = layout.search_partition_dir(db.parquet_dir, "template_param_index", domain,
                                           template_search_bucket(name, parameter_key, parameter_value))
        files = glob.glob(str(part / layout.FILE_GLOB))
        total, shas = None, []
        if files:
            src = f"read_parquet({files!r})"
            where = "template_name = ? AND parameter_key = ? AND parameter_value = ?"
            args = [name, parameter_key, parameter_value]
            if tmpl_id is not None:
                total = con.execute(text(
                    "SELECT n_citations FROM template_value_counts WHERE wiki_template_id = :w AND parameter_key = :k "
                    "AND parameter_value_md5 = CAST(md5(:v) AS bpchar)"), {"w": tmpl_id, "k": parameter_key, "v": parameter_value}).scalar()
            duck = db.duck()
            try:
                if total is None:
                    total = duck.execute(f"SELECT count(*) FROM {src} WHERE {where}", args).fetchone()[0]
                shas = [r[0] for r in duck.execute(
                    f"SELECT normalized_sha1 FROM {src} WHERE {where} ORDER BY normalized_sha1 LIMIT ? OFFSET ?",
                    args + [limit, offset]).fetchall()]
            finally:
                duck.close()
        if tmpl_id is None and not total:
            raise NotFound(f"Template {name!r} not found on {domain}")
        rows = con.execute(text(
            "SELECT nc.normalized_sha1, nc.reference_normalized, d.value, nc.appears_on_page_id FROM normalized_citations nc "
            "JOIN domains d ON d.id = nc.appears_on_domain_id "
            # normalized_sha1 is CHAR(40): a text[] parameter would cast the column and skip its index
            # (a parallel scan of the whole table, ~1 minute on enwiki)
            "WHERE nc.normalized_sha1 = ANY(CAST(:s AS bpchar[])) ORDER BY nc.normalized_sha1"),
            {"s": shas}).all() if shas else []
        titles = page_titles(con, {(r[2], r[3]) for r in rows if r[3] is not None})
    return TemplateReportResponse(
        template_name=name, domain=domain, parameter_key=parameter_key, parameter_value=parameter_value, total=int(total or 0),
        citations=[TemplateReportCitation(normalized_sha1=r[0], reference_normalized=r[1], appears_on_domain=r[2],
                                          appears_on_page_id=r[3], appears_on_article=r[3],
                                          appears_on_page_title=titles.get((r[2], r[3]))) for r in rows])


def get_web_resource(db: Database, url: str) -> WebResourceResponse:
    with db.connect() as con:
        wr = con.execute(text("SELECT w.id, w.url, d.value, w.numeric_page_id FROM web_resources w LEFT JOIN domains d ON d.id = w.domain_id "
                              "WHERE w.url_hash = :h"), {"h": WebResource.compute_url_hash(url)}).first()
        if wr is None:
            raise NotFound("Web resource not found")
        rows = con.execute(text("""
            SELECT nc.normalized_sha1, d.value, nc.appears_on_page_id, r.global_source_id, gs.reliability_score, gs.verdict
            FROM normalized_citation_web_resources l
            JOIN normalized_citations nc ON nc.id = l.normalized_id
            JOIN domains d ON d.id = nc.appears_on_domain_id
            LEFT JOIN citation_source_resolution r ON r.normalized_id = nc.id
                 AND r.domain_id = nc.appears_on_domain_id AND r.page_id = nc.appears_on_page_id
            LEFT JOIN global_sources gs ON gs.global_source_id = r.global_source_id
            WHERE l.web_resource_id = :w
            ORDER BY nc.id, r.part_index"""), {"w": wr[0]}).all()
    # web_resources.id is a row id: used to join, never returned (the URL is the key)
    return WebResourceResponse(
        url=wr[1], domain=wr[2], numeric_page_id=wr[3],
        referenced_by=[WebResourceReference(normalized_sha1=r[0], appears_on_domain=r[1], appears_on_page_id=r[2],
                                            appears_on_article=r[2], global_source_id=r[3], reliability_score=r[4], verdict=r[5])
                       for r in rows])
