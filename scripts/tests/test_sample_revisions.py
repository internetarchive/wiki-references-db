"""Offline tests for scripts/sample_revisions.py against a fake MediaWiki API."""
from __future__ import annotations

import bz2
import importlib.util
import json
import random
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "sample_revisions.py"
spec = importlib.util.spec_from_file_location("sample_revisions", SCRIPT)
sr = importlib.util.module_from_spec(spec)
sys.modules["sample_revisions"] = sr
spec.loader.exec_module(sr)

NS = "{http://www.mediawiki.org/xml/export-0.11/}"


class FakeResponse:
    def __init__(self, data, status=200):
        self.status_code = status
        self.headers = {}
        self._data = data

    def json(self):
        return self._data

    def raise_for_status(self):
        pass


class FakeWiki:
    """Just enough of the Action API for the sampler. Page n has revisions n*100 (current) and n*100-1."""

    def __init__(self, wiki: str, n_pages: int, articles: int | None = None):
        self.wiki = wiki
        self.n = n_pages
        self.articles = articles or n_pages
        self.headers = {}
        self.calls = 0
        self.rng = random.Random(wiki)

    def text(self, rev):
        # Includes a CR, a non-BMP char and leading whitespace to check round-tripping.
        return (f"  Intro of rev {rev} \U0001F600\r\n== References ==\n"
                f"<ref name=\"a\">{{{{cite web |url=https://example.org/{rev} |title=T}}}}</ref>\n"
                f"* {{{{Cite book |title=B{rev}}}}}\n[https://x.example/{rev} label]\n")

    def page(self, pid):
        return {"pageid": pid, "ns": 0, "title": f"Page {pid}", "lastrevid": pid * 100}

    def get(self, url, params, timeout):
        self.calls += 1
        p = params
        q = {}
        if p.get("meta") == "siteinfo":
            q = {"general": {"sitename": "Wikipedia", "wikiid": f"{self.wiki}wiki", "lang": self.wiki,
                             "base": f"https://{self.wiki}.wikipedia.org/wiki/Main_Page",
                             "generator": "MediaWiki 1.45", "case": "first-letter"},
                 "namespaces": {"0": {"id": 0, "name": "", "case": "first-letter"},
                                "14": {"id": 14, "name": "Category", "case": "first-letter"}},
                 "statistics": {"articles": self.articles}}
        elif p.get("meta") == "linterstats":
            q = {"linterstats": {"totals": {"missing-end-tag": 30, "fostered": 3, "obsolete-tag": 0}}}
        elif p.get("list") == "linterrors":
            ids = range(1, 31) if p["lntcategories"] == "missing-end-tag" else range(40, 43)
            q = {"linterrors": [{"pageid": i, "ns": 0, "title": f"Page {i}", "category": p["lntcategories"],
                                 "location": [5, 9]} for i in ids if i <= self.n]}
        elif p.get("meta") == "allmessages":
            q = {"allmessages": [
                {"name": "cite-tracking-category-cite-error", "content": "Pages with reference errors"},
                {"name": "template-loop-category", "content": "-"},
                {"name": "duplicate-args-category", "missing": True},
            ]}
        elif p.get("generator") == "categorymembers":
            q = {"pages": [self.page(i) for i in range(50, 70) if i <= self.n]}
        elif p.get("list") == "recentchanges":
            q = {"recentchanges": [{"type": "edit", "ns": 0, "title": f"Page {i}", "pageid": i,
                                    "revid": i * 100 - 1} for i in range(1, 25) if i <= self.n]}
        elif p.get("list") == "querypage":
            q = {"querypage": {"results": [{"ns": 0, "title": f"Page {i}", "value": 1}
                                           for i in range(self.n, 0, -1)][:40]}}
        elif p.get("generator") == "random":
            q = {"pages": [self.page(self.rng.randint(1, self.n)) for _ in range(20)]}
        elif p.get("list") == "allpages":
            # Exercise continuation: two halves.
            half = self.n // 2
            if "apcontinue" in p:
                return FakeResponse({"query": {"allpages": [{"title": f"Page {i}"} for i in range(half + 1, self.n + 1)]}})
            return FakeResponse({"continue": {"apcontinue": "x", "continue": "-||"},
                                 "query": {"allpages": [{"title": f"Page {i}"} for i in range(1, half + 1)]}})
        elif p.get("prop") == "info":
            if "pageids" in p:
                q = {"pages": [self.page(int(i)) for i in p["pageids"].split("|")]}
            else:
                q = {"pages": [self.page(int(t.split()[-1])) for t in p["titles"].split("|")]}
        elif p.get("prop") == "revisions":
            pages, bad = {}, {}
            for r in map(int, p["revids"].split("|")):
                pid = (r + 1) // 100 if r % 100 else r // 100
                if r == 300:  # simulate a revision-deleted text
                    slot = {"texthidden": True, "contentmodel": "wikitext"}
                elif r == 400:
                    bad[str(r)] = {"revid": r, "missing": True}
                    continue
                else:
                    slot = {"content": self.text(r), "contentmodel": "wikitext", "contentformat": "text/x-wiki"}
                pages.setdefault(pid, {"pageid": pid, "ns": 0, "title": f"Page {pid}", "revisions": []})
                pages[pid]["revisions"].append({"revid": r, "parentid": r - 1, "timestamp": "2026-01-01T00:00:00Z",
                                                "user": "Ed", "userid": 7, "comment": "c & <d>",
                                                "slots": {"main": slot}})
            q = {"pages": list(pages.values())}
            if bad:
                q["badrevids"] = bad
        elif p.get("prop") == "categoryinfo":
            q = {"pages": [{"title": t, "categoryinfo": {"pages": 20}} for t in p["titles"].split("|")]}
        else:
            raise AssertionError(f"unhandled request {p}")
        return FakeResponse({"batchcomplete": True, "query": q})


@pytest.fixture
def fake(monkeypatch):
    # en claims to be large, so it samples with generator=random; rm is small (allpages).
    wikis = {"en": FakeWiki("en", 400, articles=7_000_000), "rm": FakeWiki("rm", 30)}
    monkeypatch.setattr(sr, "SESSION_FACTORY", lambda w: wikis[w])
    return wikis


def run(*argv):
    assert sr.main(list(argv)) == 0


def test_manifest_fetch_mutate(tmp_path, fake):
    man, data = tmp_path / "man", tmp_path / "data"
    run("manifest", "--wikis", "en,rm", "--total", "200", "--delay", "0", "--out", str(man))

    meta = json.loads((man / "sample.json").read_text())
    assert meta["actual_total"] == 200
    en = sr.read_manifest(man / "en.tsv")
    rm = sr.read_manifest(man / "rm.tsv")
    # rm has 30 pages + reverted revisions; the remainder moves to en.
    assert len(en) + len(rm) == 200 and len(en) > 100
    assert len({r.rev_id for r in en}) == len(en)
    strata = {r.stratum for r in en}
    assert {"random", "linter", "tracking", "reverted", "long"} <= strata
    linter = [r for r in en if r.stratum == "linter"]
    assert any(r.detail.startswith("fostered") for r in linter), "rare category must be represented"
    tracking = [r.detail for r in en if r.stratum == "tracking"]
    assert any(d == "Pages with reference errors [cite-tracking-category-cite-error]" for d in tracking)
    assert not any("template-loop" in d or "duplicate-args" in d for d in tracking), \
        "disabled ('-') and missing messages must be skipped"
    # A page is not sampled twice as a current revision.
    current = [r.page_id for r in en if r.stratum != "reverted"]
    assert len(current) == len(set(current))

    run("fetch", "--wikis", "en,rm", "--delay", "0", "--manifest", str(man), "--out", str(data))
    calls = fake["en"].calls
    run("fetch", "--wikis", "en", "--delay", "0", "--manifest", str(man), "--out", str(data))
    assert fake["en"].calls - calls <= 1, "second fetch should come entirely from the cache"

    with bz2.open(data / "en.xml.bz2", "rb") as fh:
        root = ET.parse(fh).getroot()
    revs = root.findall(f"{NS}page/{NS}revision")
    texts = {int(r.find(f"{NS}id").text): r.find(f"{NS}text").text for r in revs}
    wanted = {r.rev_id for r in en} - {300, 400}
    assert set(texts) == wanted
    some = next(iter(texts))
    assert texts[some] == fake["en"].text(some), "text must round-trip exactly, CR included"
    sha = revs[0].find(f"{NS}sha1").text
    assert len(sha) == 31

    run("mutate", "--wikis", "en", "--variants", "2", "--out", str(data))
    with bz2.open(data / "en.mutated.xml.bz2", "rb") as fh:
        mroot = ET.parse(fh).getroot()
    mrevs = mroot.findall(f"{NS}page/{NS}revision")
    assert len(mrevs) == 2 * len(texts)
    for r in mrevs:
        rid = int(r.find(f"{NS}id").text)
        parent = int(r.find(f"{NS}parentid").text)
        assert rid == sr.MUTATION_REV_BASE + parent * 10 + (rid - sr.MUTATION_REV_BASE) % 10
        assert (r.find(f"{NS}text").text or "") != texts[parent]
    first = (data / "en.mutations.tsv").read_text()
    run("mutate", "--wikis", "en", "--variants", "2", "--out", str(data))
    assert (data / "en.mutations.tsv").read_text() == first, "mutations must be deterministic"


def test_mutate_text_ops_apply():
    text = "a {{cite web|url=x}} <ref>b</ref> [[c]] <!-- d --> {| e |}"
    seen = set()
    for i in range(200):
        out, op = sr.mutate_text(text, random.Random(i))
        assert out != text and op != "unchanged"
        seen.add(op.split()[0])
    assert seen == {"truncate_in_construct", "drop_closer", "drop_opener", "insert_stray",
                    "close_to_open", "truncate_anywhere"}
    assert sr.mutate_text("", random.Random(0)) == ("", "unchanged")


def test_sha1_base36_matches_dump_convention():
    # SHA1("") = da39a3ee5e6b4b0d3255bfef95601890afd80709; dumps store it base36, 31 chars.
    assert sr.sha1_base36("") == "phoiac9h4m842xq45sp7s6u21eteeq1"
