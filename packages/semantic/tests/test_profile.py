"""profile.py against upstream page_summary.py on fixed rows (the classifiers are
verbatim; this pins their behaviour and the added assembly)."""
from wcd_semantic.profile import build_source_profile, classify_source_format, classify_source_kind


def test_classifiers():
    assert classify_source_format(citation_type="template", has_ref_tag=None, has_cite_template=None, has_citation_template=None) == "Citation templates"
    assert classify_source_format(citation_type="ref_tag", has_ref_tag=None, has_cite_template=None, has_citation_template=None) == "Plain-text refs"
    assert classify_source_format(citation_type=None, has_ref_tag=True, has_cite_template=None, has_citation_template=None) == "Plain-text refs"
    assert classify_source_format(citation_type=None, has_ref_tag=None, has_cite_template=None, has_citation_template=None) == "Other / mixed"
    k = lambda **kw: classify_source_kind(**{"template_name": None, "template_subtype": None, "norm_subtype": None,
                                              "extracted_isbn": None, "extracted_doi": None, "extracted_pmid": None,
                                              "extracted_pmc": None, "extracted_url": None, **kw})
    assert k(template_subtype="cite_book") == "Books"
    assert k(template_name="Cite journal") == "Journals"
    assert k(template_subtype="cite_other") == "Other / unknown"
    assert k(extracted_isbn="9780143036555") == "Books"
    assert k(norm_subtype="prose_citation") == "Journals"
    assert k(extracted_url="https://e.org") == "Websites"
    assert k(norm_subtype="bare_url") == "Websites"
    assert k() == "Other / unknown"


def test_build_source_profile():
    rows = [
        {"citation_type": "template", "template_subtype": "cite_book"},
        {"citation_type": "template", "template_subtype": "cite_web"},
        {"citation_type": "ref_tag", "norm_subtype": "prose_citation"},
        {"citation_type": "bare_url", "norm_subtype": "bare_url", "extracted_url": "https://x"},
    ]
    p = build_source_profile(rows, 4)
    assert p["total_citation_uses"] == 4
    assert {b["label"]: b["count"] for b in p["format_breakdown"]} == {"Citation templates": 2, "Plain-text refs": 2, "Other / mixed": 0}
    assert {b["label"]: b["count"] for b in p["source_kind_breakdown"]} == {"Websites": 2, "Journals": 1, "Books": 1, "News": 0, "Other / unknown": 0}
    assert p["format_breakdown"][0]["percentage"] == 50.0
    assert p["summary"] == "This page mostly uses websites and journals, and most references appear as citation templates."
    assert build_source_profile([], 0)["summary"].startswith("This page does not yet have enough")
