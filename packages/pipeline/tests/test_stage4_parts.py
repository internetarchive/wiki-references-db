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
