"""Stage 4 content extraction and part building, without a database."""
from __future__ import annotations

from wcd_pipeline.stage4 import content_of, parts_of
from wcd_schema.ids import part_sha1


def test_content_of_strips_wrappers_by_citation_type():
    assert content_of('<ref name="a">Smith 2001</ref>', "ref_tag") == "Smith 2001"
    assert content_of("<ref>{{Cite book|title=T}}</ref>", "template") == "{{Cite book|title=T}}"
    assert content_of("<ref>https://x.org/1</ref>", "bare_url") == "https://x.org/1"
    assert content_of("* Doe, J. (1990). ''Endnote''.", "endnote") == "Doe, J. (1990). ''Endnote''."
    assert content_of("** nested item", "endnote") == "nested item"
    assert content_of("{{Sfn|Smith|2001|p=3}}", "shorthand") == "{{Sfn|Smith|2001|p=3}}"
    assert content_of("[https://x.org/2 label]", "external_link") == "[https://x.org/2 label]"


def test_parts_template_ref_tag_and_bullet_block():
    tpl = parts_of(1, "<ref>{{Cite book|first=Jared|isbn=978-0-14-303655-5|last=Diamond|title=Collapse|year=2005}}</ref>", "template")
    assert len(tpl) == 1 and tpl[0]["part_type"] == "template"
    assert tpl[0]["template_name"] == "cite book" and tpl[0]["extracted_isbn"] == "9780143036555"
    assert tpl[0]["author_key"] == "diamond" and tpl[0]["year"] == "2005"
    assert tpl[0]["part_sha1"] == part_sha1(tpl[0]["content"]) and tpl[0]["content"].startswith("{{Cite book")
    assert tpl[0]["params_json"] is not None

    prose = parts_of(2, '<ref>Fischer, Steven Roger (1995). "Preliminary Evidence". \'\'Journal\'\' 104: 303–21.</ref>', "ref_tag")
    assert len(prose) == 1 and prose[0]["part_type"] == "ref_tag"
    assert prose[0]["author_key"] == "fischer" and prose[0]["year"] == "1995" and prose[0]["params_json"] is None

    block = parts_of(3, "<ref>\n* {{Cite journal|first=A|journal=J1|last=Lee|title=Trade A|year=2001}}\n"
                        "* {{Cite journal|first=B|journal=J2|last=Lee|title=Trade B|year=2002}}\n</ref>", "template")
    assert [(p["part_index"], p["part_type"], p["year"]) for p in block] == [(0, "template", "2001"), (1, "template", "2002")]
    assert block[0]["normalized_id"] == 3 and block[0]["content"] != block[1]["content"]


def test_parts_urls_shorthand_and_noise():
    bare = parts_of(4, "<ref>[https://example.org/visits Visits]</ref>", "bare_url")
    assert bare[0]["extracted_url"] == "https://example.org/visits" and bare[0]["norm_subtype"] == "bare_url"
    ext = parts_of(5, "https://example.org/5", "external_link")
    assert ext[0]["part_type"] == "external_link" and ext[0]["extracted_url"] == "https://example.org/5"
    sfn = parts_of(6, "{{Sfn|Routledge|1919|p=12}}", "shorthand")
    assert sfn[0]["norm_subtype"] == "shorthand_template" and sfn[0]["author_key"] == "routledge" and sfn[0]["year"] == "1919"
    # the footnote filter rejects an editorial note; it still gets one part, typed noise
    noise = parts_of(7, "<ref>&nbsp;see above</ref>", "ref_tag")
    assert len(noise) == 1 and noise[0]["norm_subtype"] == "noise" and noise[0]["part_index"] == 0
    assert parts_of(8, "<ref></ref>", "ref_tag")[0]["norm_subtype"] == "noise"


def test_long_letter_run_is_classified_quickly_and_whole():
    """A vandalised ref of one 50,000-letter 'word' (enwiki has a 227,137-char one)
    is classified in linear time (wcd_semantic NOTICE deviation 2), and a long
    letters-only URL parameter reaches the normalizer uncut."""
    import time

    content = "[http://example.org/x Music Trust – More" + "a" * 50_000 + "]"
    t = time.process_time()
    part = parts_of(9, f"<ref>{content}</ref>", "ref_tag")[0]
    assert time.process_time() - t < 5     # upstream regex: ~4 CPU-min (quadratic backtracking)
    assert part["content"] == content and part["part_sha1"] == part_sha1(content)
    assert part["extracted_url"] == "https://example.org/x"   # the normalizer upgrades the scheme
    gs = "https://www.google.com/search?q=x&gs_lp=" + "Egxnd" * 60
    assert parts_of(10, gs, "external_link")[0]["extracted_url"] == gs


def test_ftp_url_citations_keep_their_url():
    """The ref-tag normalizer extracts only http(s); a bare-URL or external-link
    citation of an ftp:// URL still gets extracted_url (gate 4)."""
    bare = parts_of(11, "<ref>[ftp://ftp.example.org/pub/a.pdf A manual]</ref>", "bare_url")[0]
    assert bare["extracted_url"] == "ftp://ftp.example.org/pub/a.pdf"
    plain = parts_of(12, "<ref>ftp://ftp.example.org/pub/b.txt</ref>", "bare_url")[0]
    assert plain["extracted_url"] == "ftp://ftp.example.org/pub/b.txt"
    # a prose ref_tag citation is not a URL citation: no fallback
    assert parts_of(13, "<ref>Text, see ftp://ftp.example.org/c</ref>", "ref_tag")[0]["extracted_url"] is None


def test_url_citation_keeps_its_url_when_only_a_template_part_is_split_out():
    """`[http://… Title{{Citation needed}}]`: the splitter keeps only the template;
    the URL becomes a part of its own (templates and wiki quotes removed)."""
    parts = parts_of(14, "<ref>[http://ellen.warne{{Citation needed}}rbros.com/2010/a.html Title{{Citation needed}}]</ref>",
                     "bare_url")
    if parts[0]["part_type"] == "template":     # the vendored splitter's behaviour this guards against
        assert [p["part_index"] for p in parts] == [0, 1]
        url_part = parts[1]
        assert url_part["content"] == "http://ellen.warnerbros.com/2010/a.html"
        assert url_part["part_sha1"] == part_sha1(url_part["content"])
    assert any(p["extracted_url"] and "ellen.warnerbros.com/2010/a.html" in p["extracted_url"] for p in parts)
    ext = parts_of(15, "[https://2cm.es/1g4SC In 2015 he signed.{{Citation needed|date=October 2024}}]", "external_link")
    assert any(p["extracted_url"] for p in ext)


def test_fallback_url_drops_wiki_quotes_and_bare_schemes():
    assert parts_of(16, "<ref>http://'''Www.usa.com/weather.htm'''</ref>", "bare_url")[0]["extracted_url"] \
        == "http://Www.usa.com/weather.htm"
    assert parts_of(17, "<ref>[http://''www.radiotimes.com/episode/x'']</ref>", "bare_url")[0]["extracted_url"] \
        == "http://www.radiotimes.com/episode/x"
    assert all(p["extracted_url"] is None for p in parts_of(18, "<ref>https://'</ref>", "bare_url"))
