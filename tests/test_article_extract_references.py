from refs_extractor.article import extract_references


def _by_raw(results):
    return {r["raw_reference"]: r for r in results}


def test_extract_references_types_and_offsets_and_dedupe_external_links():
    wikitext = (
        "Lead text. <!-- <ref>https://comment.invalid</ref> -->\n"
        "Inline <ref name=foo>https://example.com/a</ref> then more.\n"
        "And a short footnote {{Sfn|Smith|2020|p=12}} here.\n"
        "\n"
        "==External links==\n"
        "* [https://archive.org Internet Archive]\n"
        "\n"
        "==Other section==\n"
        "* No links here\n"
        "* Has a bare url https://example.com/b\n"
        "\n"
        "Standalone https://example.com/c at end.\n"
    )

    results = extract_references(wikitext, include_offsets=True)
    by_raw = _by_raw(results)

    ref_raw = "<ref name=foo>https://example.com/a</ref>"
    assert ref_raw in by_raw
    assert by_raw[ref_raw]["reference_name"] == "foo"
    assert by_raw[ref_raw]["offset_start"] == wikitext.find(ref_raw)
    assert by_raw[ref_raw]["length"] == len(ref_raw)

    sfn_raw = "{{Sfn|Smith|2020|p=12}}"
    assert sfn_raw in by_raw
    assert by_raw[sfn_raw]["reference_name"] is None
    assert by_raw[sfn_raw]["offset_start"] == wikitext.find(sfn_raw)

    list_raw = "* [https://archive.org Internet Archive]"
    assert list_raw in by_raw
    assert by_raw[list_raw]["offset_start"] == wikitext.find(list_raw)

    # Bare URL list item in non-reference section should be included
    item_raw = "* Has a bare url https://example.com/b"
    assert item_raw in by_raw

    # Standalone URL should be included, but URL inside <ref> should not be emitted as standalone
    assert "https://example.com/c" in by_raw
    assert "https://example.com/a" not in by_raw


def test_extract_references_self_closing_ref_name():
    wikitext = "X <ref name=bar /> Y"
    results = extract_references(wikitext)
    by_raw = _by_raw(results)
    raw = "<ref name=bar />"
    assert raw in by_raw
    assert by_raw[raw]["reference_name"] == "bar"
    assert by_raw[raw]["offset_start"] == wikitext.find(raw)
