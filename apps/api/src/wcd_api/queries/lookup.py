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
from . import NotFound


def get_template_report(db: Database, wiki_template_id: int, parameter_key: str, parameter_value: str,
                        limit: int = 100, offset: int = 0) -> TemplateReportResponse:
    """Normalized citations whose template ``wiki_template_id`` has
    ``parameter_key = parameter_value``. The matches come from the one
    ``template_param_index`` partition the (template, key, value) hashes to; the
    total from ``template_value_counts`` when precomputed (heavy values), else
    counted there. Citations are listed in ``normalized_sha1`` order."""
    import glob

    with db.connect() as con:
        tmpl = con.execute(text("SELECT w.name, d.value FROM wiki_templates w LEFT JOIN domains d ON d.id = w.domain WHERE w.id = :i"),
                           {"i": wiki_template_id}).first()
        if tmpl is None:
            raise NotFound("Template not found")
        name, domain = tmpl
        part = layout.search_partition_dir(db.parquet_dir, "template_param_index", domain,
                                           template_search_bucket(name, parameter_key, parameter_value))
        files = glob.glob(str(part / layout.FILE_GLOB))
        total, shas = 0, []
        if files:
            src = f"read_parquet({files!r})"
            where = "template_name = ? AND parameter_key = ? AND parameter_value = ?"
            args = [name, parameter_key, parameter_value]
            duck = db.duck()
            try:
                total = con.execute(text(
                    "SELECT n_citations FROM template_value_counts WHERE wiki_template_id = :w AND parameter_key = :k "
                    "AND parameter_value_md5 = md5(:v)"), {"w": wiki_template_id, "k": parameter_key, "v": parameter_value}).scalar()
                if total is None:
                    total = duck.execute(f"SELECT count(*) FROM {src} WHERE {where}", args).fetchone()[0]
                shas = [r[0] for r in duck.execute(
                    f"SELECT normalized_sha1 FROM {src} WHERE {where} ORDER BY normalized_sha1 LIMIT ? OFFSET ?",
                    args + [limit, offset]).fetchall()]
            finally:
                duck.close()
        rows = con.execute(text(
            "SELECT nc.normalized_sha1, nc.reference_normalized, d.value, nc.appears_on_page_id FROM normalized_citations nc "
            "JOIN domains d ON d.id = nc.appears_on_domain_id WHERE nc.normalized_sha1 = ANY(:s) ORDER BY nc.normalized_sha1"),
            {"s": shas}).all() if shas else []
    return TemplateReportResponse(
        wiki_template_id=wiki_template_id, template_name=name, domain=domain,
        parameter_key=parameter_key, parameter_value=parameter_value, total=int(total),
        citations=[TemplateReportCitation(normalized_sha1=r[0], reference_normalized=r[1], appears_on_domain=r[2],
                                          appears_on_page_id=r[3], appears_on_article=r[3]) for r in rows])


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
    return WebResourceResponse(
        web_resource_id=wr[0], url=wr[1], domain=wr[2], numeric_page_id=wr[3],
        referenced_by=[WebResourceReference(normalized_sha1=r[0], appears_on_domain=r[1], appears_on_page_id=r[2],
                                            appears_on_article=r[2], global_source_id=r[3], reliability_score=r[4], verdict=r[5])
                       for r in rows])
