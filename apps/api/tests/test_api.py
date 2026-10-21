"""Every /api/v2 endpoint (and the v1 aliases) on the synthetic database."""
from __future__ import annotations

import os

import pytest
from fastapi.testclient import TestClient

from wcd_api.main import create_app
from wcd_api.settings import Settings

DSN = os.environ.get("WCD_TEST_DATABASE_URL")

D = "en.wikipedia.org"
PAGE_A, PAGE_B = 101, 202
ISBN = "9780143036555"


def get(client, path, **params):
    r = client.get(path, params=params)
    return r.status_code, r.json()


# Group 1 -------------------------------------------------------------------

def test_article_resolution(client):
    s, j = get(client, "/api/v2/article", url=f"https://{D}/w/index.php?curid={PAGE_A}")
    assert s == 200 and j["domain"] == D and j["page_id"] == PAGE_A and j["page_title"] == "Example Island"
    assert j["revision_count"] == 3 and j["latest_revision_id"] == 1003 and "document_id" not in j
    s, j = get(client, "/api/v2/article", url=f"https://{D}/wiki/Second_Page")
    assert s == 200 and j["page_id"] == PAGE_B
    s, j = get(client, "/api/v2/article", url=f"{D}/w/index.php?title=Second%20Page")
    assert s == 200 and j["page_id"] == PAGE_B
    assert get(client, "/api/v2/article", url=f"https://{D}/wiki/No_Such_Page")[0] == 404
    assert get(client, "/api/v2/article", url=f"https://{D}/w/index.php?curid=999999")[0] == 404
    assert get(client, "/api/v2/article", url="https://xx.wikipedia.org/wiki/X")[0] == 404
    s, j = get(client, "/api/v2/article", url="not a url at all")
    assert s == 400 and j["code"] == 400


# Group 2 -------------------------------------------------------------------

def test_page_summary(client):
    s, j = get(client, f"/api/v2/pages/{D}/{PAGE_B}/summary")
    assert s == 200
    assert {k: j[k] for k in ("domain", "page_id", "page_title", "page_namespace", "revision_count")} == \
        {"domain": D, "page_id": PAGE_B, "page_title": "Second Page", "page_namespace": 0, "revision_count": 2}
    assert j["url"] == f"https://{D}/w/index.php?curid={PAGE_B}"
    assert j["first_revision_timestamp"].startswith("2006-02-02") and j["latest_revision_timestamp"].startswith("2007-02-02")
    lr = j["latest_revision"]
    assert lr["rev_id"] == 2202 and lr["reference_count"] == 2
    assert lr["citation_type_breakdown"] == [{"citation_type": "template", "count": 2}]
    assert [t["template_name"] for t in lr["top_templates"]] == ["cite book", "cite web"]
    sp = j["source_profile"]
    assert sp["total_citation_uses"] == 2 and len(sp["format_breakdown"]) == 3 and len(sp["source_kind_breakdown"]) == 5
    assert sp["summary"].startswith("This page mostly uses")
    assert get(client, f"/api/v2/pages/{D}/424242/summary")[0] == 404
    assert get(client, "/api/v2/pages/xx.wikipedia.org/101/summary")[0] == 404


def test_random_summary(client):
    s, j = get(client, "/api/v2/pages/random/summary")
    assert s == 200 and j["page_id"] in (PAGE_A, PAGE_B) and j["domain"] == D
    s, j = get(client, "/api/v2/pages/random/summary", domain=D)
    assert s == 200
    assert get(client, "/api/v2/pages/random/summary", domain="xx.wikipedia.org")[0] == 404


def test_timeline(client):
    s, j = get(client, f"/api/v2/pages/{D}/{PAGE_A}/timeline")
    assert s == 200 and j["page_id"] == PAGE_A and j["revision_count"] == 3
    pts = j["points"]
    assert [p["rev_id"] for p in pts] == [1001, 1002, 1003]
    assert [p["reference_count"] for p in pts] == [6, 8, 6]
    assert [p["named_ref_count"] for p in pts] == [1, 2, 2]          # self-closing reuses
    assert [p["template_count"] for p in pts] == [1, 2, 2]
    assert [p["high_confidence_count"] for p in pts] == [1, 1, 1]   # the ISBN source resolves at page level
    assert {b["citation_type"]: b["count"] for b in pts[0]["citation_type_breakdown"]} == \
        {"template": 1, "ref_tag": 1, "shorthand": 1, "endnote": 3}
    assert pts[0]["timestamp"].startswith("2004-01-01")


def test_top_sources_and_source_endpoints(client):
    s, j = get(client, f"/api/v2/pages/{D}/{PAGE_A}/top-sources", limit=3)
    assert s == 200 and j["total_sources"] == 8 and len(j["sources"]) == 3
    top = j["sources"][0]
    assert top["page_citation_count"] == 2 and top["cross_page_count"] == 2 and top["extracted_isbn"] == ISBN
    assert top["source_label"] == "Collapse" and top["verdict"] == "high_confidence" and top["reliability_score"] == 85
    assert top["match_type"] == "isbn" and top["confidence"] == 0.95 and top["n_self_closing_uses"] == 5
    assert top["is_current"] is True and top["first_seen"].startswith("2004-01-01")
    gid = top["global_source_id"]

    s, j = get(client, f"/api/v2/sources/{gid}")
    assert s == 200 and j["n_pages"] == 2 and j["n_total_citations"] == 3 and j["source_type"] == "template"
    assert j["canonical"]["page_id"] in (PAGE_A, PAGE_B) and len(j["canonical"]["normalized_sha1"]) == 40
    assert j["source_label"] == "Collapse"

    s, j = get(client, f"/api/v2/sources/{gid}/pages")
    assert s == 200 and j["total_pages"] == 2 and [p["page_id"] for p in j["pages"]] == [PAGE_A, PAGE_B]
    assert j["pages"][0]["citation_count"] == 2 and j["pages"][1]["is_current"] is True
    assert j["pages"][0]["url"] == f"https://{D}/w/index.php?curid={PAGE_A}" and j["pages"][0]["page_title"] == "Example Island"
    s, j = get(client, f"/api/v2/sources/{gid}/pages", exclude_domain=D, exclude_page_id=PAGE_A)
    assert [p["page_id"] for p in j["pages"]] == [PAGE_B] and j["total_pages"] == 2
    assert get(client, "/api/v2/sources/nope")[0] == 404
    assert get(client, "/api/v2/sources/nope/pages")[0] == 404


# Group 3 -------------------------------------------------------------------

def test_revisions(client):
    s, j = get(client, f"/api/v2/pages/{D}/{PAGE_A}/revisions")
    assert s == 200 and j["total"] == 3
    assert [(r["revision_id"], r["parent_revision_id"], r["citation_count"]) for r in j["revisions"]] == \
        [(1001, None, 6), (1002, 1001, 8), (1003, 1002, 6)]
    s, j = get(client, f"/api/v2/pages/{D}/{PAGE_A}/revisions", limit=1, offset=1)
    assert [r["revision_id"] for r in j["revisions"]] == [1002] and j["total"] == 3
    assert get(client, f"/api/v2/pages/{D}/424242/revisions")[0] == 404


def test_citations_at_revision(client):
    s, j = get(client, f"/api/v2/pages/{D}/{PAGE_A}/citations", revision_id=1002)
    assert s == 200 and j["citation_count"] == 8 and len(j["citations"]) == 8 and j["revision_timestamp"].startswith("2005-06-15")
    by_norm = {c["reference_normalized"]: c for c in j["citations"]}
    fischer = next(c for k, c in by_norm.items() if k.startswith("<ref>Fischer"))
    assert fischer["removed_at"]["revision_id"] == 1003 and fischer["currently_visible"] is False
    assert fischer["first_seen"]["revision_id"] == 1001 and fischer["appearance_count"] == 2
    assert fischer["citation_type"] == "ref_tag" and fischer["reference_type"] == "inline"
    assert fischer["parts"][0]["norm_subtype"] == "prose_citation" and fischer["parts"][0]["global_source_id"]
    assert fischer["parts"][0]["verdict"] == "medium_confidence"
    diamond = next(c for c in j["citations"] if c["reference_name"] == "diamond")
    assert diamond["removed_at"]["revision_id"] == 1003   # redefined in 1003 → this raw text disappears
    assert diamond["parts"][0]["extracted_isbn"] == ISBN and diamond["parts"][0]["match_type"] == "isbn"
    assert diamond["templates"][0]["template_name"] == "Cite book" and diamond["templates"][0]["parameters"]["isbn"]
    assert diamond["other_articles"] == []   # page B cites the book without a ref name: a different normalized text
    visits = next(c for c in j["citations"] if c["citation_type"] == "bare_url")
    assert visits["extracted_links"][0]["url"] == "https://example.org/visits"
    block = next(c for c in j["citations"] if len(c["parts"]) == 2)
    assert [p["part_index"] for p in block["parts"]] == [0, 1]

    # latest revision by default; raw mode is per-instance without the normalized detail
    s, j = get(client, f"/api/v2/pages/{D}/{PAGE_A}/citations")
    assert j["revision_id"] == 1003 and j["citation_count"] == 6 and all(c["currently_visible"] for c in j["citations"])
    s, j = get(client, f"/api/v2/pages/{D}/{PAGE_A}/citations", raw="true", limit=2, offset=1)
    assert s == 200 and j["citation_count"] == 6 and len(j["citations"]) == 2
    assert j["citations"][0]["raw_sha1"] and j["citations"][0]["reference_normalized"] is None and j["citations"][0]["parts"] == []
    assert get(client, f"/api/v2/pages/{D}/{PAGE_A}/citations", revision_id=5)[0] == 404


def test_citation_detail_and_history(client):
    s, j = get(client, f"/api/v2/pages/{D}/{PAGE_B}/citations")
    sha = next(c["normalized_sha1"] for c in j["citations"] if c["parts"][0]["extracted_isbn"] == ISBN)
    s, j = get(client, f"/api/v2/citations/{sha}")
    assert s == 200 and j["citation_type"] == "template"
    assert j["appears_on_articles"] == [{"domain": D, "page_id": PAGE_B}]
    assert len(j["instances"]) == 1 and j["instances"][0]["page_id"] == PAGE_B and j["instances"][0]["reference_type"] == "inline"
    assert j["parts"][0]["extracted_isbn"] == ISBN and j["parts"][0]["global_source_id"] is None
    res = j["parts"][0]["resolutions"]
    assert [(r["domain"], r["page_id"], r["match_type"]) for r in res] == [(D, PAGE_B, "singleton")]
    assert res[0]["verdict"] == "high_confidence" and res[0]["global_source_id"]
    assert [(h["page_id"], h["revision_id"]) for h in j["history"]] == [(PAGE_B, 2201), (PAGE_B, 2202)]
    assert j["templates"][0]["parameters"]["last"].strip() == "Diamond" and j["extracted_links"] == []
    # the named definition on page A: two normalized texts (two editions), each its own citation
    s, j = get(client, f"/api/v2/pages/{D}/{PAGE_A}/citations", revision_id=1001)
    sha_a = next(c["normalized_sha1"] for c in j["citations"] if c["reference_name"] == "diamond")
    s, j = get(client, f"/api/v2/citations/{sha_a}")
    assert [(h["page_id"], h["revision_id"]) for h in j["history"]] == [(PAGE_A, 1001), (PAGE_A, 1002)]
    assert j["parts"][0]["resolutions"][0]["match_type"] == "isbn"

    s, j = get(client, f"/api/v2/citations/{sha}/history", domain=D, page_id=PAGE_B)
    assert s == 200 and [h["revision_id"] for h in j["revisions"]] == [2201, 2202]
    assert j["revisions"][0]["revision_timestamp"].startswith("2006-02-02")
    assert get(client, "/api/v2/citations/" + "0" * 40)[0] == 404
    assert get(client, "/api/v2/citations/short")[0] == 422


# Group 4 -------------------------------------------------------------------

def test_template_report_and_web_resources(client):
    s, j = get(client, f"/api/v2/pages/{D}/{PAGE_B}/citations")
    tpl = next(t for c in j["citations"] for t in c["templates"] if t["template_name"] == "Cite book")
    # parameter values are stored as written in the wikitext (here with the trailing space before the next pipe)
    s, j = get(client, f"/api/v2/templates/{tpl['wiki_template_id']}/report", parameter_key="last", parameter_value="Diamond ")
    assert s == 200 and j["template_name"] == "Cite book" and j["domain"] == D and j["total"] == 3
    assert all(c["appears_on_domain"] == D and c["appears_on_article"] == c["appears_on_page_id"] for c in j["citations"])
    assert get(client, "/api/v2/templates/999999/report", parameter_key="x", parameter_value="y")[0] == 404

    s, j = get(client, "/api/v2/web-resources", url="https://example.org/page")
    assert s == 200 and j["domain"] == "example.org" and len(j["referenced_by"]) == 1
    ref = j["referenced_by"][0]
    assert ref["appears_on_page_id"] == PAGE_B and ref["global_source_id"] and ref["verdict"] == "high_confidence"
    assert get(client, "/api/v2/web-resources", url="https://nowhere.example/")[0] == 404


# v1 aliases and meta ---------------------------------------------------------

def test_v1_aliases(client):
    assert client.get("/healthz").json() == {"status": "ok"}
    s, j = get(client, "/api/v1/article", url=f"https://{D}/w/index.php?curid={PAGE_A}")
    assert s == 200 and j["page_id"] == PAGE_A
    s, j = get(client, f"/api/v1/article/{PAGE_A}/revisions")
    assert s == 200 and j["total"] == 3 and j["domain"] == D
    s, j = get(client, f"/api/v1/article/{PAGE_A}/citations", revision_id=1001)
    assert s == 200 and j["citation_count"] == 6
    sha = j["citations"][0]["normalized_sha1"]
    assert get(client, f"/api/v1/citation/{sha}")[0] == 200
    assert get(client, f"/api/v1/citation/{sha}/history", page_id=PAGE_A)[0] == 200
    assert get(client, "/api/v1/web_resource", url="https://example.org/page")[0] == 200
    spec = client.get("/openapi.json").json()
    assert "/api/v2/pages/{domain}/{page_id}/summary" in spec["paths"] and "/api/v1/article" in spec["paths"]


@pytest.mark.parametrize("path", ["/api/v2/pages/en.wikipedia.org/0/summary", "/api/v2/pages/en.wikipedia.org/101/top-sources?limit=0"])
def test_validation(client, path):
    assert client.get(path).status_code == 422


def test_article_title_resolution_failure_is_503(synthetic):
    import httpx

    def failing(domain, title, user_agent, contact_email):
        raise httpx.ConnectError("blocked")

    settings = Settings(database_url="postgresql+psycopg2://" + DSN.split("://", 1)[1], parquet_dir=synthetic["parquet_dir"])
    with TestClient(create_app(settings, title_resolver=failing, frontend_dir=None)) as c:
        r = c.get("/api/v2/article", params={"url": f"https://{D}/wiki/Example_Island"})
        assert r.status_code == 503 and "Could not resolve" in r.json()["error"]
        # curid URLs never touch the site API
        assert c.get("/api/v2/article", params={"url": f"https://{D}/?curid={PAGE_A}"}).status_code == 200


def test_frontend_is_served(synthetic):
    from wcd_api.main import FRONTEND_DIR

    assert (FRONTEND_DIR / "index.html").exists()
    settings = Settings(database_url="postgresql+psycopg2://" + DSN.split("://", 1)[1], parquet_dir=synthetic["parquet_dir"])
    with TestClient(create_app(settings, title_resolver=lambda *a: None)) as c:
        index = c.get("/")
        assert index.status_code == 200 and "Wikipedia Citations Database" in index.text and "UVA" not in index.text
        assert c.get("/static/app.js").status_code == 200 and c.get("/static/styles.css").status_code == 200
        assert 'API_BASE = "/api/v2"' in c.get("/static/app.js").text
