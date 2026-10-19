"""Offline tests for scripts/select_fixture_pages.py and scripts/export_history.py."""
from __future__ import annotations

import bz2
import importlib.util
import json
import random
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from urllib.parse import unquote

import pytest

SCRIPTS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))


def _load(name):
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


sr = _load("sample_revisions")
sel = _load("select_fixture_pages")
eh = _load("export_history")

NS = "{http://www.mediawiki.org/xml/export-0.11/}"
CUTOFF = "2020-01-04T00:00:00Z"
SITE = {"general": {"sitename": "Wikipedia", "wikiid": "enwiki", "lang": "en",
                    "base": "https://en.wikipedia.org/wiki/Main_Page", "generator": "MediaWiki 1.45",
                    "case": "first-letter"},
        "namespaces": {"0": {"id": 0, "name": "", "case": "first-letter"}},
        "statistics": {"articles": 7_000_000}}


class Resp:
    def __init__(self, data, status=200):
        self.status_code = status
        self.headers = {}
        self._data = data

    def json(self):
        return self._data

    def raise_for_status(self):
        pass


def ok(q, cont=None):
    d = {"batchcomplete": True, "query": q}
    if cont:
        d["continue"] = cont
    return Resp(d)


# ── selection ─────────────────────────────────────────────────────────────────

REDIRECT, LATE, GONE = 13, 17, 299
CLASSES = ["FA", "A", "GA", "B", "C", "Start", "Stub"]


class SelectWiki:
    """Pages 1..300. Medicine = 1..120, Military history = 100..200 (overlap 100..120).
    Page p links to every q with |p-q| <= 25; q links back unless q % 3 == 0.
    Page p has 10*p edits; page 13 is a redirect, 17 was created after the cutoff, 299 is gone."""

    headers: dict = {}

    def __init__(self):
        self.rng = random.Random(1)
        self.calls = 0

    @staticmethod
    def project_pages(project):
        rng = range(1, 121) if project == "Medicine" else range(100, 201)
        return {str(p): {"pageid": p, "ns": 0, "title": f"Page {p}",
                         "assessment": {"class": CLASSES[p % 7], "importance": "Top" if p % 4 == 0 else "Low"}}
                for p in rng}

    @staticmethod
    def info(p):
        if p == GONE or p > 300:
            return {"pageid": p, "missing": True} if p == GONE else {"title": f"Page {p}", "missing": True}
        d = {"pageid": p, "ns": 0, "title": f"Page {p}", "lastrevid": p * 100}
        if p == REDIRECT:
            d["redirect"] = True
        return d

    @staticmethod
    def links(p):
        return [q for q in range(max(1, p - 25), min(300, p + 25) + 1) if q != p]

    def get(self, url, params=None, timeout=None):
        self.calls += 1
        if "rest.php" in url:
            title = unquote(url.split("/page/")[1].split("/history")[0]).replace("_", " ")
            n = int(title.split()[-1]) * 10
            return Resp({"count": n, "limit": n > 2000})
        p = params
        if p.get("meta") == "siteinfo":
            return ok(SITE)
        if p.get("list") == "projectpages":
            pages = self.project_pages(p["wppprojects"])
            keys = sorted(pages, key=int)
            half = keys[: len(keys) // 2] if "wppcontinue" not in p else keys[len(keys) // 2:]
            q = {"projects": {p["wppprojects"]: {k: pages[k] for k in half}}}
            return ok(q, None if "wppcontinue" in p else {"wppcontinue": "x", "continue": "-||"})
        if p.get("prop") == "info" and "pageids" in p:
            return ok({"pages": [self.info(int(i)) for i in p["pageids"].split("|")]})
        if p.get("prop") == "info" and "titles" in p:
            return ok({"pages": [self.info(int(t.split()[-1])) if t.startswith("Page ") else
                                 {"title": t, "missing": True} for t in p["titles"].split("|")]})
        if p.get("prop") == "revisions":
            pid = int(p["pageids"])
            ts = "2030-01-01T00:00:00Z" if pid == LATE else "2005-01-01T00:00:00Z"
            return ok({"pages": [{"pageid": pid, "ns": 0, "title": f"Page {pid}",
                                  "revisions": [{"revid": pid * 100 - 50, "timestamp": ts}]}]})
        if p.get("prop") == "links":
            titles = p["titles"].split("|")
            out = []
            for t in titles:
                src = int(t.split()[-1])
                ls = [f"Page {q}" for q in self.links(src)]
                if "pltitles" in p:
                    ls = [l for l in ls if l == p["pltitles"] and src % 3 != 0]
                d = {"pageid": src, "ns": 0, "title": t}
                if ls:
                    d["links"] = [{"ns": 0, "title": l} for l in ls]
                out.append(d)
            return ok({"pages": out})
        if p.get("generator") == "random":
            return ok({"pages": [self.info(self.rng.randint(1, 300)) for _ in range(20)]})
        if p.get("list") == "querypage":
            return ok({"querypage": {"results": [{"ns": 0, "title": "Page 190", "value": 9},
                                                 {"ns": 0, "title": "Elsewhere", "value": 8},
                                                 {"ns": 0, "title": "Page 5", "value": 7}]}})
        if p.get("meta") == "allmessages":
            return ok({"allmessages": [{"name": "cite-tracking-category-cite-error",
                                        "content": "Pages with reference errors"}]})
        if p.get("generator") == "categorymembers":
            return ok({"pages": [self.info(i) for i in range(30, 61)]})
        if p.get("list") == "search":
            return ok({"search": [{"ns": 0, "title": f"Page {i}", "pageid": i} for i in range(200, 260)]})
        raise AssertionError(f"unhandled {url} {p}")


def read_tsv(path):
    meta, rows = eh.read_pages(path)
    lines = [l for l in path.read_text().splitlines() if l and not l.startswith("#")]
    header = lines[0].split("\t")
    return meta, [dict(zip(header, l.split("\t"))) for l in lines[1:]]


def test_select(tmp_path, monkeypatch):
    wiki = SelectWiki()
    monkeypatch.setattr(sr, "SESSION_FACTORY", lambda w: wiki)
    extra = tmp_path / "extra.tsv"
    extra.write_text("# hand-picked\nPage_250\tmoved page\tci\nNo such page\tx\n")
    out = tmp_path / "pages.tsv"
    argv = ["--cutoff", CUTOFF, "-o", str(out), "--candidates", str(tmp_path / "c.tsv.gz"),
            "--delay", "0", "--clusters", "2", "--cluster-size", "8", "--per-class", "2", "--both", "4",
            "--random", "6", "--heavy", "2", "--cite-errors", "3", "--ldr", "3",
            "--seed-page", "Medicine=Page 60", "--extra", str(extra)]
    assert sel.main(argv) == 0
    meta, rows = read_tsv(out)
    assert meta["cutoff"] == CUTOFF and meta["wiki"] == "en"
    ids = [int(r["page_id"]) for r in rows]
    assert len(ids) == len(set(ids))
    assert not {REDIRECT, LATE, GONE} & set(ids)
    strata = {}
    for r in rows:
        strata.setdefault(r["stratum"], []).append(r)
    assert len(strata["cluster"]) == 2 * 2 * 8
    assert len(strata["both"]) == 4 and all(100 <= int(r["page_id"]) <= 120 for r in strata["both"])
    # MostRevisions order, skipping the title that is in no project.
    assert [r["title"] for r in strata["heavy"]] == ["Page 190", "Page 5"]
    assert all(r["tier"] == "rehearsal" for r in strata["heavy"])
    assert len(strata["cite-error"]) == 3 and len(strata["ldr"]) == 3 and len(strata["random"]) == 6
    assert strata["manual"][0]["title"] == "Page 250" and strata["manual"][0]["tier"] == "ci"
    # The named seed leads its cluster, and its members are within 25 of it, reciprocal first.
    med1 = [r for r in strata["cluster"] if r["group"] == "Medicine-1"]
    assert med1[0]["title"] == "Page 60" and med1[0]["detail"].startswith("seed")
    assert all(abs(int(r["page_id"]) - 60) <= 25 for r in med1)
    assert all(int(r["page_id"]) % 3 != 0 for r in med1[1:] if "links back" in r["detail"])
    # ci tier: within the revision cap (manual rows excepted), one whole-ish cluster per project.
    ci = [r for r in rows if r["tier"] == "ci"]
    assert ci and all(int(r["revisions"].rstrip("+")) <= 1500 and not r["revisions"].endswith("+")
                      for r in ci if r["stratum"] != "manual")
    assert {r["group"] for r in ci if r["stratum"] == "cluster"} <= {"Medicine-1", "Medicine-2",
                                                                    "Military history-1", "Military history-2"}
    assert len({r["group"].split("-")[0] for r in ci if r["stratum"] == "cluster"}) == 2
    # Companion id lists.
    txt = [l.split()[0] for l in (tmp_path / "pages.txt").read_text().splitlines() if not l.startswith("#")]
    citxt = [l.split()[0] for l in (tmp_path / "pages.ci.txt").read_text().splitlines() if not l.startswith("#")]
    assert sorted(map(int, txt)) == sorted(ids) and len(citxt) == len(ci)

    # A second run reuses the cached candidates and, with the same answers, selects the same pages.
    out2 = tmp_path / "pages2.tsv"
    monkeypatch.setattr(sr, "SESSION_FACTORY", lambda w: SelectWiki())
    assert sel.main([a if a != str(out) else str(out2) for a in argv]) == 0
    assert [r["page_id"] for r in read_tsv(out2)[1]] == [r["page_id"] for r in rows]


# ── export ────────────────────────────────────────────────────────────────────


class HistoryWiki:
    """prop=revisions per page, two revisions per batch, honouring rvend.
    Page 1: five revisions on 2020-01-01..05 (four before the cutoff); rev 102 is revision-deleted.
    Page 2: one revision. Page 3: deleted page. Page 4: created after the cutoff."""

    headers: dict = {}

    def __init__(self, fail_once_at=None):
        self.fail_once_at = fail_once_at
        self.calls = []

    def revs(self, pid):
        if pid == 1:
            return [(100 + i, f"2020-01-0{i + 1}T00:00:00Z") for i in range(5)]
        if pid == 2:
            return [(200, "2019-06-01T00:00:00Z")]
        if pid == 4:
            return [(400, "2021-01-01T00:00:00Z")]
        return None

    def get(self, url, params=None, timeout=None):
        p = params
        if p.get("meta") == "siteinfo":
            return ok(SITE)
        assert p["prop"] == "revisions" and p["rvdir"] == "newer" and p["rvend"] == CUTOFF
        pid = int(p["pageids"])
        start = int(p.get("rvcontinue", 0))
        self.calls.append((pid, start))
        if self.fail_once_at == (pid, start):
            self.fail_once_at = None
            raise RuntimeError("connection dropped")
        revs = self.revs(pid)
        if revs is None:
            return ok({"pages": [{"pageid": pid, "missing": True}]})
        revs = [r for r in revs if r[1] <= p["rvend"]]
        batch = revs[start:start + 2]
        page = {"pageid": pid, "ns": 0, "title": f"Page {pid} & co"}
        if batch:
            page["revisions"] = []
            for rid, ts in batch:
                slot = ({"texthidden": True, "contentmodel": "wikitext"} if rid == 102 else
                        {"content": f"rev {rid}\r\n<ref>{{{{cite web|url=https://e.org/{rid}}}}}</ref>",
                         "contentmodel": "wikitext", "contentformat": "text/x-wiki"})
                page["revisions"].append({"revid": rid, "parentid": rid - 1 if rid % 100 else 0,
                                          "timestamp": ts, "user": "Ed", "userid": 7, "comment": "c",
                                          "slots": {"main": slot}})
        cont = {"rvcontinue": str(start + 2), "continue": "||"} if start + 2 < len(revs) else None
        return ok({"pages": [page]}, cont)


def write_pages(path):
    path.write_text(f"# wiki: en\n# cutoff: {CUTOFF}\n"
                    "page_id\ttitle\tproject\tstratum\tgroup\ttier\trevisions\tdetail\n"
                    "1\tPage 1\tMedicine\tcluster\tMedicine-1\tci\t5\t\n"
                    "2\tPage 2\tMedicine\tclass\tMedicine/B\trehearsal\t1\t\n"
                    "3\tPage 3\t\trandom\t\trehearsal\t1\t\n"
                    "4\tPage 4\t\trandom\t\tci\t1\t\n")


def parse(path):
    root = ET.fromstring(bz2.open(path).read())
    return {int(pg.find(f"{NS}id").text): pg for pg in root.iter(f"{NS}page")}


def test_export_history(tmp_path, monkeypatch):
    pages, out = tmp_path / "pages.tsv", tmp_path / "out"
    write_pages(pages)
    wiki = HistoryWiki(fail_once_at=(1, 2))
    monkeypatch.setattr(sr, "SESSION_FACTORY", lambda w: wiki)
    with pytest.raises(RuntimeError):
        eh.main(["fetch", "--pages", str(pages), "--out", str(out), "--delay", "0"])
    assert (out / "cache" / "1.continue.json").exists()
    assert eh.main(["fetch", "--pages", str(pages), "--out", str(out), "--delay", "0"]) == 0
    # Resumed from the checkpoint, not from the start.
    assert [c for c in wiki.calls if c[0] == 1] == [(1, 0), (1, 2), (1, 2)]
    done = {p: json.loads((out / "cache" / f"{p}.done.json").read_text()) for p in (1, 2, 3, 4)}
    assert done[1]["revisions"] == 4 and done[1]["texthidden"] == 1
    assert done[3]["status"] == "missing" and done[4]["status"] == "no-revisions-before-cutoff"
    assert not (out / "cache" / "1.continue.json").exists()

    assert eh.main(["write", "--pages", str(pages), "--out", str(out)]) == 0
    full, ci = parse(out / "fixture.xml.bz2"), parse(out / "fixture-ci.xml.bz2")
    assert sorted(full) == [1, 2] and sorted(ci) == [1]
    revs = full[1].findall(f"{NS}revision")
    assert [int(r.find(f"{NS}id").text) for r in revs] == [100, 101, 102, 103]
    hidden = revs[2].find(f"{NS}text")
    assert hidden.get("deleted") == "deleted" and revs[2].find(f"{NS}sha1") is None
    assert revs[0].find(f"{NS}text").text == "rev 100\r\n<ref>{{cite web|url=https://e.org/100}}</ref>"
    assert revs[0].find(f"{NS}sha1").text == sr.sha1_base36(revs[0].find(f"{NS}text").text)
    assert full[1].find(f"{NS}title").text == "Page 1 & co"
    summary = json.loads((out / "export.json").read_text())
    assert summary["cutoff"] == CUTOFF
    assert summary["outputs"]["fixture.xml.bz2"]["revisions"] == 5
    assert summary["outputs"]["fixture.xml.bz2"]["left_out"] == {"3": "missing", "4": "no-revisions-before-cutoff"}
    assert len(summary["outputs"]["fixture-ci.xml.bz2"]["sha256"]) == 64

    # A second fetch is a no-op.
    n = len(wiki.calls)
    assert eh.main(["fetch", "--pages", str(pages), "--out", str(out), "--delay", "0"]) == 0
    assert len(wiki.calls) == n


def test_read_pages_requires_header(tmp_path):
    p = tmp_path / "pages.tsv"
    p.write_text("page_id\ttitle\n1\tA\n")
    with pytest.raises(SystemExit):
        eh.read_pages(p)
