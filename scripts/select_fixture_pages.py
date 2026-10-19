#!/usr/bin/env python3
"""Select the pages that make up the WCD test-article fixture.

Writes fixtures/pages.tsv — the record of the fixture, meant to be committed —
plus fixtures/pages.txt and fixtures/pages.ci.txt (bare page-id lists for
`wcd fixture-dump`, if the fixture is ever cut from real dumps instead).
scripts/export_history.py then exports every listed page's full history up
to the cutoff recorded in the pages.tsv header.

Selection criteria (defaults in brackets)

The fixture centres on two WikiProjects with dense citation networks
[Medicine, Military history]: medicine for journal citations with PMID/DOI,
military history for {{sfn}}/Harvard shortened footnotes with a bibliography
and book citations with ISBNs. Project membership and assessment come from
the PageAssessments API (list=projectpages); only namespace-0 pages count.

  cluster     Per project, [3] clusters of up to [20] pages: a seed page
              (FA/A/GA class, Top/High importance, or given by --seed-page)
              plus project pages it links to, those that link back first.
              Neighbouring articles cite the same sources, which is what
              cross-page matching (Stage 6d) needs to be exercised.
  class       Per project, [10] pages from each assessment class
              (FA, A, GA, B, C, Start, Stub; empty classes are skipped).
  both        [15] pages assessed by every listed project.
  random      [80] ns-0 non-redirect pages from generator=random: a control
              group for "ordinary" articles.
  heavy       Up to [3] project pages from Special:MostRevisions (very long
              histories; rehearsal tier only).
  cite-error  [10] members of the Cite error tracking category.
  ldr         [10] pages found by insource:"reflist refs" (list-defined refs).
  manual      Rows of --extra (title<TAB>reason[<TAB>ci]): hand-picked edge
              cases such as moved, history-merged or blanked-and-restored pages.

Every page is checked before it is taken: it must exist, be in namespace 0,
not be a redirect, and have its first revision at or before --cutoff. Pages
are taken in a seeded random order, and a page already taken by an earlier
stratum is not taken again.

Tiers. Every page is in the rehearsal tier. The ci tier — the small fixture
that runs on every build — is a subset: from each project the cluster with
the most pages within --ci-max-revisions [1500] (those pages only), and,
within the same limit, [1] page per class per project, [5] both-project
pages, [10] random pages, [5] cite-error and [5] ldr pages, plus manual rows
marked ci. Revision counts come from the REST history-counts endpoint and are
today's counts, slightly above the count at the cutoff.

Reproducibility. Assessments, links, random pages and search results change
over time, so re-running does not reproduce a selection: pages.tsv is the
record. The seed only makes a run deterministic given the same API answers.

Requests are serial, carry maxlag=5, back off on 429/5xx/maxlag, and send a
User-Agent with CONTACT_EMAIL (from the environment or .env).

Usage:
    uv run python scripts/select_fixture_pages.py --cutoff 2026-10-01T00:00:00Z
    uv run python scripts/select_fixture_pages.py --cutoff … \\
        --seed-page "Medicine=Tuberculosis" --seed-page "Military history=Battle of Gettysburg" \\
        --extra fixtures/fixture_extra.tsv
"""
from __future__ import annotations

import argparse
import gzip
import itertools
import random
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator
from urllib.parse import quote

sys.path.insert(0, str(Path(__file__).resolve().parent))
import sample_revisions as sr  # noqa: E402  (Api client)

SCRIPT_VERSION = "1"
CLASSES = ["FA", "A", "GA", "B", "C", "Start", "Stub"]
SEED_CLASSES = {"FA", "A", "GA"}
SEED_IMPORTANCE = {"Top", "High"}
CUTOFF_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
COLUMNS = ["page_id", "title", "project", "stratum", "group", "tier", "revisions", "detail"]
log = sr.log


@dataclass
class Candidate:
    page_id: int
    title: str
    project: str
    cls: str = ""
    importance: str = ""


@dataclass
class Check:
    page_id: int
    title: str
    ok: bool
    reason: str = ""
    revisions: int | None = None
    capped: bool = False  # REST count hit its limit: the true count is higher


@dataclass
class Selected:
    page_id: int
    title: str
    project: str
    stratum: str
    group: str
    detail: str
    revisions: int | None
    capped: bool
    tier: str = "rehearsal"

    def within(self, cap: int) -> bool:
        return self.revisions is not None and not self.capped and self.revisions <= cap


# ── candidates (PageAssessments) ──────────────────────────────────────────────


def fetch_projectpages(api: sr.Api, project: str) -> list[Candidate]:
    out: list[Candidate] = []
    for data in api.query_all(list="projectpages", wppprojects=project, wppassessments=1,
                              wpplimit="max"):
        projects = data.get("query", {}).get("projects", {}) or {}
        entries = projects.get(project)
        if entries is None and len(projects) == 1:
            entries = next(iter(projects.values()))
        if isinstance(entries, dict):
            entries = list(entries.values())
        for e in entries or []:
            if int(e.get("ns", -1)) != 0:
                continue
            a = e.get("assessment") or {}
            out.append(Candidate(int(e["pageid"]), e["title"], project,
                                 a.get("class") or "", a.get("importance") or ""))
        requests_made = getattr(api, "_pp_requests", 0) + 1
        api._pp_requests = requests_made
        if requests_made % 50 == 0:
            log(f"{project}: {len(out)} pages so far")
    return out


def load_candidates(api: sr.Api, projects: list[str], path: Path) -> list[Candidate]:
    if path.exists():
        cands = []
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            next(fh)
            for line in fh:
                project, pid, title, cls, imp = line.rstrip("\n").split("\t")
                if project in projects:
                    cands.append(Candidate(int(pid), title, project, cls, imp))
        have = {c.project for c in cands}
        if have == set(projects):
            log(f"candidates from {path} ({len(cands)} rows)")
            return cands
    cands = []
    for project in projects:
        got = fetch_projectpages(api, project)
        log(f"{project}: {len(got)} ns-0 pages")
        if not got:
            raise SystemExit(f"no pages for project {project!r}; check the name (as PageAssessments spells it)")
        cands.extend(got)
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        fh.write("project\tpage_id\ttitle\tclass\timportance\n")
        for c in cands:
            fh.write(f"{c.project}\t{c.page_id}\t{c.title}\t{c.cls}\t{c.importance}\n")
    return cands


# ── checks ────────────────────────────────────────────────────────────────────


class Checker:
    """Validates pages (exists, ns 0, not a redirect, created by the cutoff) and counts edits."""

    def __init__(self, api: sr.Api, cutoff: str):
        self.api = api
        self.cutoff = cutoff
        self.info: dict[int, dict] = {}
        self.done: dict[int, Check] = {}
        self.rejects: dict[str, int] = {}

    def prefetch(self, pids: Iterable[int]) -> None:
        todo = [p for p in dict.fromkeys(pids) if p not in self.info]
        for batch in sr.chunks(todo, 50):
            data = self.api.get(prop="info", pageids="|".join(map(str, batch)))
            for p in data.get("query", {}).get("pages", []):
                if "pageid" in p:
                    self.info[int(p["pageid"])] = p
            for pid in batch:
                self.info.setdefault(pid, {"missing": True})

    def check(self, pid: int) -> Check:
        if pid in self.done:
            return self.done[pid]
        self.prefetch([pid])
        p = self.info[pid]
        title = p.get("title", "")
        if p.get("missing") or p.get("invalid"):
            c = Check(pid, title, False, "missing")
        elif int(p.get("ns", -1)) != 0:
            c = Check(pid, title, False, "not ns 0")
        elif p.get("redirect"):
            c = Check(pid, title, False, "redirect")
        else:
            data = self.api.get(prop="revisions", pageids=pid, rvdir="newer", rvlimit=1,
                                rvprop="ids|timestamp")
            pages = data.get("query", {}).get("pages", [])
            revs = pages[0].get("revisions", []) if pages else []
            first = revs[0]["timestamp"] if revs else None
            if first is None or first > self.cutoff:
                c = Check(pid, title, False, "created after cutoff")
            else:
                n, capped = rest_edit_count(self.api, title)
                c = Check(pid, title, True, "", n, capped)
        if not c.ok:
            self.rejects[c.reason] = self.rejects.get(c.reason, 0) + 1
        self.done[pid] = c
        return c


def rest_edit_count(api: sr.Api, title: str) -> tuple[int | None, bool]:
    """Edit count from GET /w/rest.php/v1/page/{title}/history/counts/edits."""
    import requests

    url = (f"https://{api.domain}/w/rest.php/v1/page/"
           f"{quote(title.replace(' ', '_'), safe='')}/history/counts/edits")
    backoff = 2.0
    for _ in range(6):
        try:
            r = api.session.get(url, timeout=60)
        except requests.RequestException as e:
            log(f"history count {title!r}: {type(e).__name__}; retrying in {backoff:.0f}s")
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)
            continue
        if r.status_code == 429 or r.status_code >= 500:
            time.sleep(float(r.headers.get("Retry-After") or backoff))
            backoff = min(backoff * 2, 60)
            continue
        if r.status_code != 200:
            log(f"history count {title!r}: HTTP {r.status_code}")
            return None, False
        d = r.json()
        if api.delay:
            time.sleep(api.delay)
        return int(d.get("count", 0)), bool(d.get("limit"))
    return None, False


# ── selection ─────────────────────────────────────────────────────────────────


class Selection:
    def __init__(self, checker: Checker):
        self.checker = checker
        self.pages: dict[int, Selected] = {}

    def take(self, pool: Iterable[tuple[int, str, str]], n: int, stratum: str,
             group: str = "", project: str = "") -> list[Selected]:
        """Take up to n pages from pool, an iterable of (page_id, project, detail) in preference order."""
        got: list[Selected] = []
        it = iter(pool)
        while len(got) < n:
            batch = list(itertools.islice(it, 50))
            if not batch:
                break
            self.checker.prefetch(pid for pid, _, _ in batch if pid not in self.pages)
            for pid, proj, detail in batch:
                if len(got) >= n:
                    break
                if pid in self.pages:
                    continue
                c = self.checker.check(pid)
                if not c.ok:
                    continue
                s = Selected(pid, c.title, proj or project, stratum, group, detail, c.revisions, c.capped)
                self.pages[pid] = s
                got.append(s)
        if len(got) < n:
            log(f"{stratum}{' ' + group if group else ''}: only {len(got)} of {n}")
        return got


def links_from(api: sr.Api, title: str) -> list[str]:
    out: list[str] = []
    for data in api.query_all(prop="links", titles=title, plnamespace=0, pllimit="max"):
        for p in data.get("query", {}).get("pages", []):
            out.extend(l["title"] for l in p.get("links", []))
    return list(dict.fromkeys(out))


def link_back(api: sr.Api, titles: list[str], target: str) -> set[str]:
    back: set[str] = set()
    for batch in sr.chunks(titles, 50):
        for data in api.query_all(prop="links", titles="|".join(batch), pltitles=target, pllimit="max"):
            for p in data.get("query", {}).get("pages", []):
                if p.get("links"):
                    back.add(p["title"])
    return back


def build_clusters(api, sel: Selection, project: str, cands: list[Candidate], n_clusters: int,
                   size: int, seeds: list[str], rng: random.Random) -> list[list[Selected]]:
    by_title = {c.title: c for c in cands}
    seed_pool = [c for c in sorted(cands, key=lambda c: c.page_id)
                 if c.cls in SEED_CLASSES and c.importance in SEED_IMPORTANCE]
    rng.shuffle(seed_pool)
    named = []
    for t in seeds:
        if t in by_title:
            named.append(by_title[t])
        else:
            log(f"{project}: --seed-page {t!r} is not an assessed ns-0 page of this project; ignored")
    clusters: list[list[Selected]] = []
    for seed in itertools.chain(named, seed_pool):
        if len(clusters) >= n_clusters:
            break
        if seed.page_id in sel.pages or not sel.checker.check(seed.page_id).ok:
            continue
        linked = [by_title[t] for t in links_from(api, seed.title) if t in by_title and t != seed.title]
        linked = [c for c in linked if c.page_id not in sel.pages]
        if len(linked) < size // 2:
            log(f"{project}: seed {seed.title!r} links to only {len(linked)} project pages; skipped")
            continue
        back = link_back(api, [c.title for c in linked], seed.title)
        recip = [c for c in linked if c.title in back]
        other = [c for c in linked if c.title not in back]
        rng.shuffle(recip)
        rng.shuffle(other)
        group = f"{project}-{len(clusters) + 1}"
        members = sel.take([(seed.page_id, project, f"seed ({seed.cls}/{seed.importance})")], 1,
                           "cluster", group, project)
        pool = [(c.page_id, project, f"links back to seed {seed.title!r}") for c in recip] + \
               [(c.page_id, project, f"linked from seed {seed.title!r}") for c in other]
        members += sel.take(pool, size - 1, "cluster", group, project)
        clusters.append(members)
        log(f"{group}: seed {seed.title!r}, {len(members)} pages ({len(recip)} reciprocal links available)")
    return clusters


def random_pool(api: sr.Api, limit: int = 20_000) -> Iterator[tuple[int, str, str]]:
    seen: set[int] = set()
    while len(seen) < limit:
        data = api.get(generator="random", grnnamespace=0, grnfilterredir="nonredirects",
                       grnlimit="max", prop="info")
        pages = data.get("query", {}).get("pages", [])
        if not pages:
            return
        for p in pages:
            pid = int(p["pageid"])
            if pid not in seen:
                seen.add(pid)
                yield pid, "", "generator=random"


def mostrevisions(api: sr.Api) -> list[str]:
    try:
        data = api.get(list="querypage", qppage="Mostrevisions", qplimit="max")
    except sr.ApiError as e:
        log(f"Special:MostRevisions unavailable ({e})")
        return []
    return [r["title"] for r in data["query"]["querypage"].get("results", []) if int(r.get("ns", 0)) == 0]


def cite_error_pool(api: sr.Api, rng: random.Random, cap: int = 500) -> list[tuple[int, str, str]]:
    data = api.get(meta="allmessages", ammessages="cite-tracking-category-cite-error", amenableparser=1)
    msgs = data.get("query", {}).get("allmessages", [])
    name = (msgs[0].get("content") or "").strip() if msgs and not msgs[0].get("missing") else ""
    if not name or name == "-":
        log("Cite error tracking category not found")
        return []
    pool = []
    for data in api.query_all(generator="categorymembers", gcmtitle=f"Category:{name}", gcmnamespace=0,
                              gcmtype="page", gcmlimit="max", prop="info"):
        pool += [(int(p["pageid"]), "", f"Category:{name}") for p in data.get("query", {}).get("pages", [])]
        if len(pool) >= cap:
            break
    pool.sort()
    rng.shuffle(pool)
    return pool


def search_pool(api: sr.Api, query: str, rng: random.Random, cap: int = 500) -> list[tuple[int, str, str]]:
    pool = []
    for data in api.query_all(list="search", srsearch=query, srnamespace=0, srlimit="max",
                              srprop=""):
        pool += [(int(r["pageid"]), "", f"search {query}") for r in data["query"].get("search", [])]
        if len(pool) >= cap:
            break
    pool.sort()
    rng.shuffle(pool)
    return pool


def resolve_titles(api: sr.Api, titles: list[str]) -> dict[str, int]:
    out: dict[str, int] = {}
    for batch in sr.chunks(titles, 50):
        data = api.get(prop="info", titles="|".join(batch))
        q = data.get("query", {})
        back = {n["to"]: n["from"] for n in q.get("normalized", [])}
        for p in q.get("pages", []):
            if "pageid" in p:
                out[back.get(p["title"], p["title"])] = int(p["pageid"])
    return out


def read_extra(path: Path) -> list[tuple[str, str, bool]]:
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip() or line.startswith("#"):
            continue
        cells = line.split("\t")
        rows.append((cells[0].replace("_", " ").strip(), cells[1].strip() if len(cells) > 1 else "",
                     len(cells) > 2 and cells[2].strip().lower() == "ci"))
    return rows


# ── tiers and output ──────────────────────────────────────────────────────────


def assign_ci(sel: Selection, clusters: dict[str, list[list[Selected]]], manual_ci: set[int],
              args) -> None:
    cap = args.ci_max_revisions
    for project, cl in clusters.items():
        if not cl:
            continue
        best = max(cl, key=lambda m: (sum(s.within(cap) for s in m),
                                      -sum(s.revisions or 0 for s in m if s.within(cap))))
        for s in best:
            if s.within(cap):
                s.tier = "ci"

    def first_within(stratum: str, n: int, project: str | None = None, group: str | None = None):
        k = 0
        for s in sel.pages.values():
            if k >= n:
                break
            if s.stratum == stratum and (project is None or s.project == project) \
                    and (group is None or s.group == group) and s.within(cap):
                s.tier = "ci"
                k += 1

    for project in args.projects:
        for cls in CLASSES:
            first_within("class", args.ci_per_class, project, f"{project}/{cls}")
    first_within("both", args.ci_both)
    first_within("random", args.ci_random)
    first_within("cite-error", args.ci_cite_errors)
    first_within("ldr", args.ci_ldr)
    for pid in manual_ci:
        if pid in sel.pages:
            s = sel.pages[pid]
            s.tier = "ci"
            if not s.within(cap):
                log(f"manual page {s.title!r} is marked ci but has {s.revisions} revisions (> {cap})")


def clean(v) -> str:
    return "" if v is None else str(v).replace("\t", " ").replace("\n", " ")


def write_outputs(sel: Selection, args, argv: list[str]) -> None:
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    rows = list(sel.pages.values())
    with open(out, "w", encoding="utf-8") as fh:
        fh.write(f"# generated by scripts/select_fixture_pages.py v{SCRIPT_VERSION}\n")
        fh.write(f"# created: {datetime.now(timezone.utc).isoformat(timespec='seconds')}\n")
        fh.write(f"# wiki: {args.wiki}\n")
        fh.write(f"# cutoff: {args.cutoff}\n")
        fh.write(f"# seed: {args.seed}\n")
        fh.write(f"# args: {' '.join(argv)}\n")
        fh.write("# revisions: today's edit count (REST history counts); '+' = at least\n")
        fh.write("\t".join(COLUMNS) + "\n")
        for s in rows:
            revs = "" if s.revisions is None else f"{s.revisions}{'+' if s.capped else ''}"
            fh.write("\t".join(clean(v) for v in (s.page_id, s.title, s.project, s.stratum, s.group,
                                                   s.tier, revs, s.detail)) + "\n")
    for path, subset in ((out.with_suffix(".txt"), rows),
                         (out.with_name(out.stem + ".ci.txt"), [s for s in rows if s.tier == "ci"])):
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(f"# generated by scripts/select_fixture_pages.py; cutoff {args.cutoff}\n")
            for s in sorted(subset, key=lambda s: s.page_id):
                fh.write(f"{s.page_id}  # {clean(s.title)} [{s.stratum}]\n")

    summary: dict[tuple[str, str], list[int]] = {}
    for s in rows:
        k = summary.setdefault((s.stratum, s.tier), [0, 0])
        k[0] += 1
        k[1] += s.revisions or 0
    log(f"wrote {len(rows)} pages to {out}")
    for (stratum, tier), (n, revs) in sorted(summary.items()):
        log(f"  {stratum:<11} {tier:<10} {n:>4} pages {revs:>9} revisions")
    for tier in ("ci", "rehearsal"):
        sub = [s for s in rows if tier == "rehearsal" or s.tier == "ci"]
        log(f"  {tier} tier: {len(sub)} pages, ~{sum(s.revisions or 0 for s in sub)} revisions")
    if sel.checker.rejects:
        log(f"  rejected while drawing: {sel.checker.rejects}")


def select(args, argv: list[str]) -> int:
    if not CUTOFF_RE.match(args.cutoff):
        raise SystemExit("--cutoff must look like 2026-10-01T00:00:00Z")
    if args.cutoff > datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"):
        raise SystemExit("--cutoff is in the future")
    rng = random.Random(args.seed)
    api = sr.Api(args.wiki, delay=args.delay)
    cands = load_candidates(api, args.projects, Path(args.candidates))
    by_project = {p: [c for c in cands if c.project == p] for p in args.projects}
    sel = Selection(Checker(api, args.cutoff))

    seeds: dict[str, list[str]] = {}
    for spec in args.seed_page:
        project, _, title = spec.partition("=")
        seeds.setdefault(project.strip(), []).append(title.replace("_", " ").strip())

    # Hand-picked pages first, so no other stratum takes them under another label.
    manual_ci: set[int] = set()
    if args.extra:
        extra = read_extra(Path(args.extra))
        ids = resolve_titles(api, [t for t, _, _ in extra])
        for title, reason, ci in extra:
            if title not in ids:
                log(f"manual page {title!r} not found")
                continue
            got = sel.take([(ids[title], "", reason)], 1, "manual")
            if got and ci:
                manual_ci.add(got[0].page_id)

    clusters = {p: build_clusters(api, sel, p, by_project[p], args.clusters, args.cluster_size,
                                  seeds.get(p, []), rng) for p in args.projects}

    if len(args.projects) > 1:
        common = set.intersection(*({c.page_id for c in by_project[p]} for p in args.projects))
        pool = sorted(common)
        rng.shuffle(pool)
        sel.take([(pid, "+".join(args.projects), "assessed by every project") for pid in pool],
                 args.both, "both")

    for project in args.projects:
        for cls in CLASSES:
            pool = sorted(c.page_id for c in by_project[project] if c.cls == cls)
            if not pool:
                continue
            rng.shuffle(pool)
            sel.take([(pid, project, f"class {cls}") for pid in pool], args.per_class, "class",
                     f"{project}/{cls}", project)

    if args.heavy:
        project_of = {c.title: c for c in cands}
        hits = [project_of[t] for t in mostrevisions(api) if t in project_of]
        sel.take([(c.page_id, c.project, "Special:MostRevisions") for c in hits], args.heavy, "heavy")
    if args.cite_errors:
        sel.take(cite_error_pool(api, rng), args.cite_errors, "cite-error")
    if args.ldr:
        sel.take(search_pool(api, 'insource:"reflist refs"', rng), args.ldr, "ldr")
    if args.random:
        sel.take(random_pool(api), args.random, "random")

    assign_ci(sel, clusters, manual_ci, args)
    write_outputs(sel, args, argv)
    return 0


def main(argv=None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0],
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    ap.add_argument("--cutoff", required=True, help="no revision after this (YYYY-MM-DDTHH:MM:SSZ)")
    ap.add_argument("-o", "--output", default="fixtures/pages.tsv")
    ap.add_argument("--wiki", default="en")
    ap.add_argument("--projects", type=lambda s: [p.strip() for p in s.split(",") if p.strip()],
                    default=["Medicine", "Military history"], help="comma-separated PageAssessments names")
    ap.add_argument("--candidates", default="data/fixture/candidates.tsv.gz",
                    help="cache of the projects' assessed pages; reused when present")
    ap.add_argument("--seed", type=int, default=20261019)
    ap.add_argument("--delay", type=float, default=0.1)
    ap.add_argument("--seed-page", action="append", default=[],
                    help="'Project=Title' to use as a cluster seed (repeatable); others are drawn")
    ap.add_argument("--extra", help="TSV of hand-picked pages: title<TAB>reason[<TAB>ci]")
    ap.add_argument("--clusters", type=int, default=3, help="clusters per project")
    ap.add_argument("--cluster-size", type=int, default=20)
    ap.add_argument("--per-class", type=int, default=10, help="pages per assessment class per project")
    ap.add_argument("--both", type=int, default=15)
    ap.add_argument("--random", type=int, default=80)
    ap.add_argument("--heavy", type=int, default=3)
    ap.add_argument("--cite-errors", type=int, default=10)
    ap.add_argument("--ldr", type=int, default=10)
    ap.add_argument("--ci-max-revisions", type=int, default=1500)
    ap.add_argument("--ci-per-class", type=int, default=1)
    ap.add_argument("--ci-both", type=int, default=5)
    ap.add_argument("--ci-random", type=int, default=10)
    ap.add_argument("--ci-cite-errors", type=int, default=5)
    ap.add_argument("--ci-ldr", type=int, default=5)
    args = ap.parse_args(argv)
    return select(args, argv)


if __name__ == "__main__":
    raise SystemExit(main())
