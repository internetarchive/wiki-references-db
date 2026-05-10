import json


def test_cli_uses_current_timestamp_when_omitted_and_prints_refs_by_default(
    monkeypatch, capsys
):
    from refs_extractor import article, cli, wikiapi

    def fake_now():
        return "2020-01-02T03:04:05Z"

    called = {}

    def fake_extract(title, domain="en.wikipedia.org", as_of=None):
        called["title"] = title
        called["domain"] = domain
        called["as_of"] = as_of
        return 123, 456, "2019-12-31T00:00:00Z", [
            {"raw_reference": "<ref>a</ref>"},
            {"raw_reference": "<ref>b</ref>"},
        ]

    monkeypatch.setattr(wikiapi, "get_current_timestamp", fake_now)
    monkeypatch.setattr(article, "extract_references_from_page", fake_extract)

    rc = cli.main(["Easter Island"])
    assert rc == 0
    assert called["title"] == "Easter Island"
    assert called["as_of"] == "2020-01-02T03:04:05Z"

    out = capsys.readouterr().out
    assert out == "<ref>a</ref>\n\n<ref>b</ref>\n"


def test_cli_accepts_explicit_timestamp(monkeypatch, capsys):
    from refs_extractor import article, cli

    called = {}

    def fake_extract(title, domain="en.wikipedia.org", as_of=None):
        called["title"] = title
        called["as_of"] = as_of
        return 1, 2, "2000-01-01T00:00:00Z", []

    monkeypatch.setattr(article, "extract_references_from_page", fake_extract)

    rc = cli.main(["Easter Island", "2004-01-01T00:00:00Z"])
    assert rc == 0
    assert called["as_of"] == "2004-01-01T00:00:00Z"

    out = capsys.readouterr().out
    assert out == ""


def test_cli_full_prints_json(monkeypatch, capsys):
    from refs_extractor import article, cli

    def fake_extract(title, domain="en.wikipedia.org", as_of=None):
        return 1, 2, "2000-01-01T00:00:00Z", [{"raw_reference": "<ref>a</ref>"}]

    monkeypatch.setattr(article, "extract_references_from_page", fake_extract)

    rc = cli.main(["--full", "Easter Island", "2004-01-01T00:00:00Z"])
    assert rc == 0

    out = capsys.readouterr().out
    data = json.loads(out)
    assert data["title"] == "Easter Island"
    assert data["as_of"] == "2004-01-01T00:00:00Z"
    assert data["page_id"] == 1
    assert data["revision_id"] == 2
    assert data["revision_timestamp"] == "2000-01-01T00:00:00Z"
    assert data["references"] == [{"raw_reference": "<ref>a</ref>"}]
