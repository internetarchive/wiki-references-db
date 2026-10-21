"""Group 4: template parameter report and web-resource lookup (v1 shapes, extended)."""
from __future__ import annotations

from sqlalchemy import text

from wcd_schema.postgres import WebResource

from ..db import Database
from ..models import (
    TemplateReportCitation, TemplateReportResponse, WebResourceReference, WebResourceResponse,
)
from . import NotFound


def get_template_report(db: Database, wiki_template_id: int, parameter_key: str, parameter_value: str,
                        limit: int = 100, offset: int = 0) -> TemplateReportResponse:
    with db.connect() as con:
        tmpl = con.execute(text("SELECT w.name, d.value FROM wiki_templates w LEFT JOIN domains d ON d.id = w.domain WHERE w.id = :i"),
                           {"i": wiki_template_id}).first()
        if tmpl is None:
            raise NotFound("Template not found")
        base = """FROM (SELECT DISTINCT nc.id FROM normalized_citations nc JOIN template_data t ON t.normalized_id = nc.id
                        WHERE t.wiki_template_id = :w AND t.parameter_key = :k AND t.parameter_value = :v) m
                  JOIN normalized_citations nc ON nc.id = m.id
                  JOIN domains d ON d.id = nc.appears_on_domain_id"""
        params = {"w": wiki_template_id, "k": parameter_key, "v": parameter_value}
        total = con.execute(text(f"SELECT count(*) {base}"), params).scalar() or 0
        rows = con.execute(text(f"SELECT nc.normalized_sha1, nc.reference_normalized, d.value, nc.appears_on_page_id {base} "
                                "ORDER BY nc.id LIMIT :l OFFSET :o"), {**params, "l": limit, "o": offset}).all()
    return TemplateReportResponse(
        wiki_template_id=wiki_template_id, template_name=tmpl[0], domain=tmpl[1],
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
