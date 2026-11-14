#!/usr/bin/env python3
"""Build a layered sample of Wikipedia revisions for comparing reference extractors.

The sample feeds the Python-vs-Rust extractor comparison: both extractors read
the same RevisionChest bundles built from the XML this script writes, and the
comparison runs over clean, heavy, and deliberately broken wikitext.

Subcommands (run in this order):

  probe     Try every source against every wiki with tiny limits and print what
            comes back. Run this first: it shows which linter categories and
            tracking categories exist on each wiki before a long run.
  manifest  Choose revisions and write fixtures/sample/<wiki>.tsv plus
            fixtures/sample/sample.json. The manifest is the reproducible
            record of the sample and is meant to be committed.
  fetch     Download every manifest revision (resumable, cached as JSONL) and
            write data/sample/<wiki>.xml.bz2 in MediaWiki export 0.11 format,
            which `RevisionChest build` reads directly.
  mutate    Write data/sample/<wiki>.mutated.xml.bz2: damaged copies of the
            fetched revisions (truncated mid-construct, dropped or stray
            delimiters). Deterministic for a given --seed.

Strata (share of each wiki's quota; shortfalls are filled from `random`):

  random    0.45  ns-0 non-redirects. generator=random on large wikis; on small
                  wikis (rm) a seeded sample of list=allpages.
  long      0.10  Special:LongPages (list=querypage).
  linter    0.25  list=linterrors, spread evenly over every category that has
                  errors on that wiki (categories read from meta=linterstats).
  tracking  0.10  Members of MediaWiki core / Cite tracking categories; names are
                  resolved per wiki through meta=allmessages, plus EXTRA_CATEGORIES.
  reverted  0.10  Edits tagged mw-reverted in recentchanges (historical
                  revisions, roughly the last 30 days).

A wiki that cannot fill its quota (rm has a few thousand articles) passes the
remainder to the other wikis' random strata, so the total stays as requested.

Requests are serial, carry maxlag=5, back off on 429/5xx/maxlag, and send a
User-Agent with CONTACT_EMAIL from the environment or .env (same variable the
extractor uses), per the Wikimedia User-Agent policy.

Usage:
    uv run python scripts/sample_revisions.py probe
    uv run python scripts/sample_revisions.py manifest --total 70000
    uv run python scripts/sample_revisions.py fetch
    uv run python scripts/sample_revisions.py mutate --variants 1
    RevisionChest build data/sample/en.xml.bz2 -o data/sample/bundles --no-db
"""
from __future__ import annotations

import argparse
import bz2
import hashlib
import json
import os
import random
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable, Iterator
from xml.sax.saxutils import escape, quoteattr

SCRIPT_VERSION = "1"

DEFAULT_WIKIS = ["en", "fr", "de", "it", "rm", "zh", "ar", "sr"]

STRATA = {
    "random": 0.45,
    "long": 0.10,
    "linter": 0.25,
    "tracking": 0.10,
    "reverted": 0.10,
}

# Message keys whose content is the name of a tracking category. A key a wiki
# does not have comes back as missing and is skipped; a category set to "-" is
# disabled on that wiki and is skipped too. `probe` lists what resolved.
TRACKING_MESSAGES = [
    # MediaWiki core parser limits and template errors
    "duplicate-args-category",
    "template-loop-category",
    "expansion-depth-exceeded-category",
    "post-expand-template-inclusion-category",
    "post-expand-template-argument-category",
    "node-count-exceeded-category",
    "unstrip-depth-category",
    "unstrip-size-category",
    # Cite
    "cite-tracking-category-cite-error",
]

# Wiki-specific categories that are not tracking-category messages. Missing
# categories are harmless (they return no members); `probe` shows their sizes.
EXTRA_CATEGORIES: dict[str, list[str]] = {
    "en": [
        "Pages with broken reference names",
        "Pages with incorrect ref formatting",
        "CS1 errors: dates",
        "CS1 errors: missing title",
        "CS1 errors: URL",
    ],
}

# Used when meta=linterstats is unavailable.
FALLBACK_LINTER_CATEGORIES = [
    "missing-end-tag",
    "stripped-tag",
    "misnested-tag",
    "multiple-unclosed-formatting-tags",
    "unclosed-quotes-in-heading",
    "wikilink-in-extlink",
    "fostered",
    "html5-misnesting",
    "bogus-image-options",
    "self-closed-tag",
    "deletable-table-tag",
    "pwrap-bug-workaround",
    "tidy-whitespace-bug",
    "multiline-html-table-in-list",
    "misc-tidy-replacement-issues",
    "night-mode-unaware-background-color",
    "duplicate-ids",
    "large-tables",
    "obsolete-tag",
]

SMALL_WIKI_ARTICLES = 50_000  # below this, sample allpages instead of generator=random
MUTATION_REV_BASE = 10_000_000_000  # synthetic rev ids: BASE + rev_id * 10 + variant

# Tests set this to a callable(wiki) -> session-like object to avoid the network.
SESSION_FACTORY: Callable | None = None

MANIFEST_COLUMNS = ["wiki", "rev_id", "page_id", "ns", "title", "stratum", "detail"]


def log(msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", file=sys.stderr, flush=True)


def chunks(items: list, n: int) -> Iterator[list]:
    for i in range(0, len(items), n):
        yield items[i:i + n]


# ── API client ────────────────────────────────────────────────────────────────


class ApiError(RuntimeError):
    def __init__(self, code: str, info: str):
        super().__init__(f"{code}: {info}")
        self.code = code


def user_agent() -> str:
    try:  # same .env convention as refs_extractor.wikiapi
        from dotenv import load_dotenv  # type: ignore
        load_dotenv()
    except Exception:
        pass
    email = (os.getenv("CONTACT_EMAIL") or "").strip()
    if "@" not in email:
        raise SystemExit(
            "CONTACT_EMAIL is not set. Add CONTACT_EMAIL=you@example.com to .env or the "
            "environment; Wikimedia requires a contact in the User-Agent."
        )
    return f"WCD-revision-sampler/{SCRIPT_VERSION} ({email}) python-requests"


class Api:
    """Serial MediaWiki Action API client with polite retry behaviour."""

    def __init__(self, wiki: str, delay: float = 0.1, session=None):
        import requests

        self.wiki = wiki
        self.domain = f"{wiki}.wikipedia.org"
        self.url = f"https://{self.domain}/w/api.php"
        self.delay = delay
        if session is None and SESSION_FACTORY is not None:
            session = SESSION_FACTORY(wiki)
        self.session = session or requests.Session()
        if session is None:
            self.session.headers["User-Agent"] = user_agent()
        self._warned: set[str] = set()

    def get(self, **params) -> dict:
        import requests

        q = {"action": "query", "format": "json", "formatversion": "2", "maxlag": "5"}
        q.update({k: str(v) for k, v in params.items()})
        backoff = 2.0
        for _attempt in range(8):
            try:
                r = self.session.get(self.url, params=q, timeout=120)
            except requests.RequestException as e:
                log(f"{self.wiki}: {type(e).__name__}; retrying in {backoff:.0f}s")
                time.sleep(backoff)
                backoff = min(backoff * 2, 120)
                continue
            if r.status_code == 429 or r.status_code >= 500:
                wait = float(r.headers.get("Retry-After") or backoff)
                log(f"{self.wiki}: HTTP {r.status_code}; retrying in {wait:.0f}s")
                time.sleep(wait)
                backoff = min(backoff * 2, 120)
                continue
            r.raise_for_status()
            data = r.json()
            err = data.get("error")
            if err:
                if err.get("code") == "maxlag":
                    wait = float(r.headers.get("Retry-After") or 5)
                    log(f"{self.wiki}: maxlag; waiting {wait:.0f}s")
                    time.sleep(wait)
                    continue
                raise ApiError(err.get("code", "?"), err.get("info", ""))
            for module, w in (data.get("warnings") or {}).items():
                text = w.get("warnings") if isinstance(w, dict) else str(w)
                key = f"{module}:{text}"
                if key not in self._warned:
                    self._warned.add(key)
                    log(f"{self.wiki}: API warning [{module}] {text}")
            if self.delay:
                time.sleep(self.delay)
            return data
        raise ApiError("retries-exhausted", f"{self.url} {params}")

    def query_all(self, **params) -> Iterator[dict]:
        """Yield each response, following `continue` until done."""
        cont: dict = {}
        while True:
            data = self.get(**params, **cont)
            yield data
            if "continue" not in data:
                return
            cont = data["continue"]


# ── Sources ───────────────────────────────────────────────────────────────────


@dataclass
class Row:
    wiki: str
    rev_id: int
    page_id: int
    ns: int
    title: str
    stratum: str
    detail: str = ""


def siteinfo(api: Api) -> dict:
    data = api.get(meta="siteinfo", siprop="general|namespaces|statistics")
    return data["query"]


def latest_revisions(api: Api, keys: list, by: str, stratum: str,
                     details: dict | None = None) -> list[Row]:
    """Resolve page ids or titles to their current revision, keeping input order."""
    details = details or {}
    found: dict = {}
    for batch in chunks(keys, 50):
        data = api.get(prop="info", **{by: "|".join(str(k) for k in batch)})
        q = data.get("query", {})
        norm = {n["to"]: n["from"] for n in q.get("normalized", [])}
        for p in q.get("pages", []):
            if p.get("missing") or p.get("invalid") or p.get("redirect") or "lastrevid" not in p:
                continue
            key = p["pageid"] if by == "pageids" else norm.get(p["title"], p["title"])
            found[key] = Row(api.wiki, int(p["lastrevid"]), int(p["pageid"]), int(p["ns"]),
                             p["title"], stratum, details.get(key, ""))
    return [found[k] for k in keys if k in found]


def source_random(api: Api, info: dict, rng: random.Random) -> Iterator[Row]:
    articles = int(info.get("statistics", {}).get("articles", 0))
    if 0 < articles <= SMALL_WIKI_ARTICLES:
        # Small wiki: list every ns-0 page and take a seeded sample.
        titles: list[str] = []
        for data in api.query_all(list="allpages", apnamespace=0,
                                  apfilterredir="nonredirects", aplimit="max"):
            titles.extend(p["title"] for p in data["query"]["allpages"])
        rng.shuffle(titles)
        log(f"{api.wiki}: random via allpages ({len(titles)} pages)")
        for batch in chunks(titles, 500):
            yield from latest_revisions(api, batch, "titles", "random", {t: "allpages" for t in batch})
        return
    stale = 0
    while stale < 5:
        data = api.get(generator="random", grnnamespace=0, grnfilterredir="nonredirects",
                       grnlimit="max", prop="info")
        pages = data.get("query", {}).get("pages", [])
        if not pages:
            stale += 1
            continue
        stale = 0
        for p in pages:
            if "lastrevid" in p and not p.get("redirect"):
                yield Row(api.wiki, int(p["lastrevid"]), int(p["pageid"]), int(p["ns"]),
                          p["title"], "random", "generator=random")


def source_long(api: Api, cap: int) -> list[Row]:
    titles: list[str] = []
    for data in api.query_all(list="querypage", qppage="Longpages", qplimit="max"):
        for r in data["query"]["querypage"].get("results", []):
            if int(r.get("ns", 0)) == 0:
                titles.append(r["title"])
        if len(titles) >= cap:
            break
    titles = titles[:cap]
    return latest_revisions(api, titles, "titles", "long", {t: "Longpages" for t in titles})


def linter_categories(api: Api) -> dict[str, int]:
    try:
        data = api.get(meta="linterstats")
        totals = data["query"]["linterstats"]["totals"]
        return {k: int(v) for k, v in totals.items() if int(v) > 0}
    except (ApiError, KeyError, TypeError, ValueError) as e:
        log(f"{api.wiki}: linterstats unavailable ({e}); using the fallback category list")
        return {c: -1 for c in FALLBACK_LINTER_CATEGORIES}


def source_linter(api: Api, rng: random.Random, quota: int) -> list[Row]:
    cats = linter_categories(api)
    if not cats or quota <= 0:
        return []
    per_cat = max(1, -(-quota // len(cats)))
    cap = max(per_cat * 3, 50)  # read extra so the pick is not just the oldest lint ids
    pools: dict[str, list[tuple[int, str]]] = {}
    for cat in sorted(cats):
        seen: set[int] = set()
        pool: list[tuple[int, str]] = []
        try:
            for data in api.query_all(list="linterrors", lntcategories=cat, lntnamespace=0,
                                      lntlimit="max"):
                for e in data["query"].get("linterrors", []):
                    pid = int(e["pageid"])
                    if pid in seen:
                        continue
                    seen.add(pid)
                    loc = e.get("location") or []
                    where = f"@{loc[0]}-{loc[1]}" if len(loc) == 2 else ""
                    pool.append((pid, f"{cat}{where}"))
                if len(pool) >= cap:
                    break
        except ApiError as e:
            log(f"{api.wiki}: linter category {cat} skipped ({e})")
            continue
        rng.shuffle(pool)
        pools[cat] = pool
    # Round-robin across categories so rare categories are represented.
    picked: list[int] = []
    details: dict[int, str] = {}
    while len(picked) < quota and any(pools.values()):
        for cat in sorted(pools):
            while pools[cat]:
                pid, detail = pools[cat].pop()
                if pid not in details:
                    details[pid] = detail
                    picked.append(pid)
                    break
            if len(picked) >= quota:
                break
    return latest_revisions(api, picked, "pageids", "linter", details)


def tracking_categories(api: Api, lang: str) -> list[tuple[str, str]]:
    """Return (category name, source) pairs that exist on this wiki."""
    names: list[tuple[str, str]] = []
    data = api.get(meta="allmessages", ammessages="|".join(TRACKING_MESSAGES),
                   amenableparser=1, amlang=lang)
    for m in data["query"].get("allmessages", []):
        content = (m.get("content") or "").strip()
        if m.get("missing") or not content or content == "-":
            continue
        names.append((content, m["name"]))
    names.extend((c, "extra") for c in EXTRA_CATEGORIES.get(api.wiki, []))
    return names


def category_sizes(api: Api, names: list[str]) -> dict[str, int]:
    sizes: dict[str, int] = {}
    for batch in chunks(names, 50):
        data = api.get(prop="categoryinfo", titles="|".join(f"Category:{n}" for n in batch))
        q = data.get("query", {})
        back = {n["to"]: n["from"] for n in q.get("normalized", [])}
        for p in q.get("pages", []):
            title = back.get(p["title"], p["title"])
            name = title.split(":", 1)[1] if ":" in title else title
            sizes[name] = int((p.get("categoryinfo") or {}).get("pages", 0))
    return sizes


def source_tracking(api: Api, lang: str, rng: random.Random, quota: int) -> list[Row]:
    cats = tracking_categories(api, lang)
    if not cats or quota <= 0:
        return []
    per_cat = max(1, -(-quota // len(cats)))
    cap = max(per_cat * 3, 50)
    pools: dict[str, list[Row]] = {}
    for name, origin in cats:
        pool: list[Row] = []
        for data in api.query_all(generator="categorymembers", gcmtitle=f"Category:{name}",
                                  gcmnamespace=0, gcmtype="page", gcmlimit="max", prop="info"):
            for p in data.get("query", {}).get("pages", []):
                if "lastrevid" in p:
                    pool.append(Row(api.wiki, int(p["lastrevid"]), int(p["pageid"]), int(p["ns"]),
                                    p["title"], "tracking", f"{name} [{origin}]"))
            if len(pool) >= cap:
                break
        rng.shuffle(pool)
        pools[name] = pool
    out: list[Row] = []
    seen: set[int] = set()
    while len(out) < quota and any(pools.values()):
        for name in sorted(pools):
            while pools[name]:
                row = pools[name].pop()
                if row.rev_id not in seen:
                    seen.add(row.rev_id)
                    out.append(row)
                    break
            if len(out) >= quota:
                break
    return out


def source_reverted(api: Api, rng: random.Random, quota: int) -> list[Row]:
    pool: list[Row] = []
    cap = max(quota * 3, 50)
    try:
        for data in api.query_all(list="recentchanges", rctag="mw-reverted", rcnamespace=0,
                                  rctype="edit", rcprop="ids|title", rclimit="max"):
            for c in data["query"].get("recentchanges", []):
                pool.append(Row(api.wiki, int(c["revid"]), int(c["pageid"]), int(c["ns"]),
                                c["title"], "reverted", "mw-reverted"))
            if len(pool) >= cap:
                break
    except ApiError as e:
        log(f"{api.wiki}: reverted edits unavailable ({e})")
        return []
    rng.shuffle(pool)
    return pool[:quota]


# ── manifest ──────────────────────────────────────────────────────────────────


def split_quota(total: int, n: int) -> list[int]:
    base, extra = divmod(total, n)
    return [base + (1 if i < extra else 0) for i in range(n)]


def stratum_quotas(quota: int) -> dict[str, int]:
    q = {s: int(quota * f) for s, f in STRATA.items() if s != "random"}
    q["random"] = quota - sum(q.values())
    return q


class WikiSample:
    def __init__(self, wiki: str, api: Api, rng: random.Random):
        self.wiki = wiki
        self.api = api
        self.rng = rng
        self.info = siteinfo(api)
        self.lang = self.info["general"].get("lang", wiki)
        self.rows: list[Row] = []
        self.rev_ids: set[int] = set()
        self._random: Iterator[Row] | None = None
        self.random_exhausted = False

    def add(self, rows: Iterable[Row], limit: int) -> int:
        added = 0
        for r in rows:
            if added >= limit:
                break
            if r.ns != 0 or r.rev_id in self.rev_ids:
                continue
            self.rev_ids.add(r.rev_id)
            self.rows.append(r)
            added += 1
        return added

    def add_random(self, n: int) -> int:
        """Add up to n random revisions; pages already sampled as current revisions are skipped."""
        if self._random is None:
            self._random = source_random(self.api, self.info, self.rng)
        pages = {r.page_id for r in self.rows if r.stratum != "reverted"}
        added = 0
        while added < n:
            try:
                r = next(self._random)
            except StopIteration:
                self.random_exhausted = True
                break
            if r.page_id in pages or r.rev_id in self.rev_ids:
                continue
            pages.add(r.page_id)
            added += self.add([r], 1)
        return added


def build_manifest(args) -> int:
    wikis = args.wikis
    rng_root = random.Random(args.seed)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    quotas = dict(zip(wikis, split_quota(args.total, len(wikis))))
    samples: dict[str, WikiSample] = {}
    deficit = 0

    for wiki in wikis:
        api = Api(wiki, delay=args.delay)
        s = WikiSample(wiki, api, random.Random(f"{args.seed}:{wiki}:{rng_root.random()}"))
        samples[wiki] = s
        q = stratum_quotas(quotas[wiki])
        log(f"{wiki}: quota {quotas[wiki]} {q}")
        got = {}
        got["linter"] = s.add(source_linter(api, s.rng, q["linter"]), q["linter"])
        got["tracking"] = s.add(source_tracking(api, s.lang, s.rng, q["tracking"]), q["tracking"])
        got["reverted"] = s.add(source_reverted(api, s.rng, q["reverted"]), q["reverted"])
        got["long"] = s.add(source_long(api, q["long"] * 2), q["long"])
        want_random = quotas[wiki] - sum(got.values())
        got["random"] = s.add_random(want_random)
        short = quotas[wiki] - len(s.rows)
        deficit += short
        log(f"{wiki}: got {got}; short by {short}")

    # Hand any shortfall to wikis whose random source is not exhausted.
    while deficit > 0:
        donors = [w for w in wikis if not samples[w].random_exhausted]
        if not donors:
            log(f"no wiki can supply the remaining {deficit} revisions")
            break
        for wiki, n in zip(donors, split_quota(deficit, len(donors))):
            if n:
                deficit -= samples[wiki].add_random(n)
                log(f"{wiki}: topped up by {n}")

    counts: dict[str, dict[str, int]] = {}
    for wiki in wikis:
        s = samples[wiki]
        path = out_dir / f"{wiki}.tsv"
        with open(path, "w", encoding="utf-8", newline="") as fh:
            fh.write("\t".join(MANIFEST_COLUMNS) + "\n")
            for r in sorted(s.rows, key=lambda r: (r.page_id, r.rev_id)):
                d = asdict(r)
                fh.write("\t".join(clean_cell(d[c]) for c in MANIFEST_COLUMNS) + "\n")
        c: dict[str, int] = {}
        for r in s.rows:
            c[r.stratum] = c.get(r.stratum, 0) + 1
        counts[wiki] = c
        log(f"{wiki}: wrote {len(s.rows)} rows to {path}")

    meta = {
        "script_version": SCRIPT_VERSION,
        "created": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "requested_total": args.total,
        "actual_total": sum(len(s.rows) for s in samples.values()),
        "seed": args.seed,
        "wikis": wikis,
        "strata_fractions": STRATA,
        "counts": counts,
        "note": "generator=random and recentchanges are not reproducible; the TSVs are the record.",
    }
    (out_dir / "sample.json").write_text(json.dumps(meta, indent=2, ensure_ascii=False) + "\n",
                                         encoding="utf-8")
    return 0


def clean_cell(v) -> str:
    return str(v).replace("\t", " ").replace("\n", " ")


def read_manifest(path: Path) -> list[Row]:
    rows = []
    with open(path, encoding="utf-8") as fh:
        header = fh.readline().rstrip("\n").split("\t")
        for line in fh:
            if not line.strip():
                continue
            d = dict(zip(header, line.rstrip("\n").split("\t")))
            rows.append(Row(d["wiki"], int(d["rev_id"]), int(d["page_id"]), int(d["ns"]),
                            d["title"], d["stratum"], d.get("detail", "")))
    return rows


# ── fetch ─────────────────────────────────────────────────────────────────────


def fetch(args) -> int:
    manifest_dir = Path(args.manifest)
    out_dir = Path(args.out)
    cache_dir = out_dir / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    for wiki in args.wikis:
        mpath = manifest_dir / f"{wiki}.tsv"
        if not mpath.exists():
            log(f"{wiki}: no manifest at {mpath}; skipped")
            continue
        rows = read_manifest(mpath)
        api = Api(wiki, delay=args.delay)
        site_path = cache_dir / f"{wiki}.siteinfo.json"
        if not site_path.exists():
            site_path.write_text(json.dumps(siteinfo(api), ensure_ascii=False), encoding="utf-8")
        cache_path = cache_dir / f"{wiki}.jsonl"
        done = {rec["rev_id"] for rec in read_cache(cache_path)}
        todo = [r.rev_id for r in rows if r.rev_id not in done]
        log(f"{wiki}: {len(rows)} in manifest, {len(done)} cached, {len(todo)} to fetch")
        with open(cache_path, "a", encoding="utf-8") as fh:
            for i, batch in enumerate(chunks(todo, 50)):
                for rec in fetch_batch(api, batch):
                    fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
                fh.flush()
                if i % 20 == 0:
                    log(f"{wiki}: {min((i + 1) * 50, len(todo))}/{len(todo)}")
        write_xml(out_dir / f"{wiki}.xml.bz2", json.loads(site_path.read_text(encoding="utf-8")),
                  read_cache(cache_path), {r.rev_id for r in rows})
    return 0


def rev_record(p: dict, rev: dict) -> dict:
    """Flatten one API revision (formatversion=2, rvslots=main) into the cache record shape."""
    slot = (rev.get("slots") or {}).get("main") or {}
    hidden = bool(slot.get("texthidden") or "content" not in slot)
    return {
        "rev_id": int(rev["revid"]),
        "page_id": int(p["pageid"]),
        "ns": int(p["ns"]),
        "title": p["title"],
        "parent_id": int(rev.get("parentid") or 0),
        "timestamp": rev.get("timestamp", ""),
        "user": None if rev.get("userhidden") else rev.get("user"),
        "user_id": None if rev.get("userhidden") else rev.get("userid"),
        "comment": None if rev.get("commenthidden") else rev.get("comment", ""),
        "sha1": slot.get("sha1") or rev.get("sha1"),
        "model": slot.get("contentmodel", "wikitext"),
        "format": slot.get("contentformat", "text/x-wiki"),
        "text": None if hidden else slot["content"],
        "status": "texthidden" if hidden else "ok",
    }


def fetch_batch(api: Api, rev_ids: list[int]) -> Iterator[dict]:
    returned: set[int] = set()
    for data in api.query_all(prop="revisions", revids="|".join(map(str, rev_ids)),
                              rvprop="ids|timestamp|user|userid|comment|content|sha1|size",
                              rvslots="main"):
        q = data.get("query", {})
        for p in q.get("pages", []):
            for rev in p.get("revisions", []):
                rid = int(rev["revid"])
                if rid in returned:
                    continue
                returned.add(rid)
                yield rev_record(p, rev)
        bad = q.get("badrevids") or []
        for b in bad.values() if isinstance(bad, dict) else bad:
            rid = int(b["revid"])
            if rid not in returned:
                returned.add(rid)
                yield {"rev_id": rid, "status": "missing"}
    for rid in rev_ids:
        if rid not in returned:
            yield {"rev_id": rid, "status": "missing"}


def read_cache(path: Path) -> list[dict]:
    if not path.exists():
        return []
    recs: dict[int, dict] = {}
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue  # a torn final line from an interrupted run
            recs[rec["rev_id"]] = rec
    return list(recs.values())


# ── XML (MediaWiki export 0.11, the subset RevisionChest and mwxml read) ───────


def sha1_base36(text: str) -> str:
    n = int(hashlib.sha1(text.encode("utf-8")).hexdigest(), 16)
    digits = "0123456789abcdefghijklmnopqrstuvwxyz"
    out = ""
    while n:
        n, r = divmod(n, 36)
        out = digits[r] + out
    return out.rjust(31, "0")


def _invalid_xml_char(text: str) -> str | None:
    for ch in text:
        o = ord(ch)
        if (o < 0x20 and ch not in "\t\n\r") or 0xD800 <= o <= 0xDFFF or o in (0xFFFE, 0xFFFF):
            return f"U+{o:04X}"
    return None


def xml_text(text: str) -> str:
    # &#13; keeps a literal CR; XML parsers would otherwise normalise it to LF.
    return escape(text, {"\r": "&#13;"})


def xml_header(site: dict) -> str:
    """The <mediawiki> open tag and <siteinfo>, from a meta=siteinfo response."""
    g = site["general"]
    lang = g.get("lang", "en")
    parts: list[str] = []
    w = parts.append
    w('<mediawiki xmlns="http://www.mediawiki.org/xml/export-0.11/" '
      'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
      'xsi:schemaLocation="http://www.mediawiki.org/xml/export-0.11/ '
      'http://www.mediawiki.org/xml/export-0.11.xsd" '
      f'version="0.11" xml:lang={quoteattr(lang)}>\n')
    w("  <siteinfo>\n")
    w(f"    <sitename>{escape(g.get('sitename', ''))}</sitename>\n")
    w(f"    <dbname>{escape(g.get('wikiid', ''))}</dbname>\n")
    w(f"    <base>{escape(g.get('base', ''))}</base>\n")
    w(f"    <generator>{escape(g.get('generator', ''))}</generator>\n")
    w(f"    <case>{escape(g.get('case', 'first-letter'))}</case>\n")
    w("    <namespaces>\n")
    nss = site.get("namespaces", {})
    for ns in sorted(nss.values(), key=lambda n: int(n["id"])):
        name = ns.get("name", "")
        case = ns.get("case", "first-letter")
        if name:
            w(f'      <namespace key="{int(ns["id"])}" case={quoteattr(case)}>{escape(name)}</namespace>\n')
        else:
            w(f'      <namespace key="{int(ns["id"])}" case={quoteattr(case)} />\n')
    w("    </namespaces>\n")
    w("  </siteinfo>\n")
    return "".join(parts)


XML_FOOTER = "</mediawiki>\n"
XML_PAGE_CLOSE = "  </page>\n"


def xml_page_open(page_id: int, title: str, ns: int) -> str:
    return (f"  <page>\n    <title>{escape(title)}</title>\n"
            f"    <ns>{int(ns)}</ns>\n    <id>{int(page_id)}</id>\n")


def xml_revision(r: dict, comment_for: Callable[[dict], str] | None = None) -> str:
    """One <revision>. A record whose text is None (revision-deleted) is written the way
    dumps write it: <text bytes="0" deleted="deleted" /> and no <sha1>."""
    parts: list[str] = []
    w = parts.append
    text = r.get("text")
    w("    <revision>\n")
    w(f"      <id>{r['rev_id']}</id>\n")
    if r.get("parent_id"):
        w(f"      <parentid>{r['parent_id']}</parentid>\n")
    w(f"      <timestamp>{escape(r.get('timestamp', ''))}</timestamp>\n")
    if r.get("user") is None:
        w('      <contributor deleted="deleted" />\n')
    elif r.get("user_id"):
        w(f"      <contributor>\n        <username>{escape(r['user'])}</username>\n"
          f"        <id>{int(r['user_id'])}</id>\n      </contributor>\n")
    else:
        w(f"      <contributor>\n        <ip>{escape(r['user'])}</ip>\n      </contributor>\n")
    comment = comment_for(r) if comment_for else r.get("comment")
    if comment is None:
        w('      <comment deleted="deleted" />\n')
    elif comment:
        w(f"      <comment>{escape(comment)}</comment>\n")
    w(f"      <model>{escape(r.get('model') or 'wikitext')}</model>\n")
    w(f"      <format>{escape(r.get('format') or 'text/x-wiki')}</format>\n")
    if text is None:
        w('      <text bytes="0" deleted="deleted" />\n')
    else:
        nbytes = len(text.encode("utf-8"))
        if text:
            w(f'      <text bytes="{nbytes}" xml:space="preserve">{xml_text(text)}</text>\n')
        else:
            w(f'      <text bytes="0" xml:space="preserve" />\n')
        w(f"      <sha1>{sha1_base36(text)}</sha1>\n")
    w("    </revision>\n")
    return "".join(parts)


def write_xml(path: Path, site: dict, recs: Iterable[dict], keep: set[int] | None = None,
              comment_for: Callable[[dict], str] | None = None) -> None:
    pages: dict[int, list[dict]] = {}
    skipped: dict[str, int] = {}
    for rec in recs:
        if keep is not None and rec["rev_id"] not in keep:
            continue
        if rec.get("status") != "ok":
            skipped[rec.get("status", "?")] = skipped.get(rec.get("status", "?"), 0) + 1
            continue
        bad = _invalid_xml_char(rec["text"])
        if bad:
            skipped[f"invalid-xml-char {bad}"] = skipped.get(f"invalid-xml-char {bad}", 0) + 1
            continue
        pages.setdefault(rec["page_id"], []).append(rec)

    n_revs = 0
    with bz2.open(path, "wt", encoding="utf-8") as fh:
        fh.write(xml_header(site))
        for pid in sorted(pages):
            revs = sorted(pages[pid], key=lambda r: r["rev_id"])
            fh.write(xml_page_open(pid, revs[0]["title"], revs[0]["ns"]))
            for r in revs:
                fh.write(xml_revision(r, comment_for))
                n_revs += 1
            fh.write(XML_PAGE_CLOSE)
        fh.write(XML_FOOTER)
    log(f"wrote {n_revs} revisions on {len(pages)} pages to {path}"
        + (f"; skipped {skipped}" if skipped else ""))


# ── mutate ────────────────────────────────────────────────────────────────────

CLOSERS = ["}}", "</ref>", "]]", "-->", "|}"]
OPENERS = ["{{", "<ref", "[[", "<!--", "{|"]
STRAYS = ["{{", "}}", "<ref>", "</ref>", "<ref name=\"x\"", "[[", "]]", "<!--", "|", "=", "{{{"]


def _positions(text: str, needles: list[str]) -> list[tuple[int, str]]:
    out = []
    for n in needles:
        i = text.find(n)
        while i != -1:
            out.append((i, n))
            i = text.find(n, i + 1)
    return out


def mutate_text(text: str, rng: random.Random) -> tuple[str, str]:
    """Return (damaged text, description). Ops are tried in random order until one applies."""
    ops = ["truncate_in_construct", "drop_closer", "drop_opener", "insert_stray",
           "close_to_open", "truncate_anywhere"]
    rng.shuffle(ops)
    for op in ops:
        if op == "truncate_in_construct":
            # Cut somewhere inside the 200 chars after an opener (never at the very end).
            pos = [(i, n) for i, n in _positions(text, ["{{", "<ref", "[[", "{|"])
                   if i + len(n) < len(text)]
            if pos:
                i, n = rng.choice(pos)
                cut = rng.randint(i + len(n), min(len(text) - 1, i + len(n) + 200))
                return text[:cut], f"{op} after {n!r}@{i} cut@{cut}"
        elif op == "drop_closer":
            pos = _positions(text, CLOSERS)
            if pos:
                i, n = rng.choice(pos)
                return text[:i] + text[i + len(n):], f"{op} {n!r}@{i}"
        elif op == "drop_opener":
            pos = _positions(text, OPENERS)
            if pos:
                i, n = rng.choice(pos)
                return text[:i] + text[i + len(n):], f"{op} {n!r}@{i}"
        elif op == "close_to_open":
            pos = _positions(text, ["</ref>", "}}"])
            if pos:
                i, n = rng.choice(pos)
                repl = "<ref>" if n == "</ref>" else "{{"
                return text[:i] + repl + text[i + len(n):], f"{op} {n!r}@{i}"
        elif op == "insert_stray":
            if text:
                i = rng.randint(0, len(text))
                s = rng.choice(STRAYS)
                return text[:i] + s + text[i:], f"{op} {s!r}@{i}"
        elif op == "truncate_anywhere":
            if len(text) > 1:
                cut = rng.randint(1, len(text) - 1)
                return text[:cut], f"{op} cut@{cut}"
    return text, "unchanged"


def mutate(args) -> int:
    out_dir = Path(args.out)
    cache_dir = out_dir / "cache"
    for wiki in args.wikis:
        cache_path = cache_dir / f"{wiki}.jsonl"
        site_path = cache_dir / f"{wiki}.siteinfo.json"
        if not cache_path.exists() or not site_path.exists():
            log(f"{wiki}: nothing fetched yet; skipped")
            continue
        mutated: list[dict] = []
        log_path = out_dir / f"{wiki}.mutations.tsv"
        with open(log_path, "w", encoding="utf-8") as lf:
            lf.write("mutated_rev_id\trev_id\tpage_id\tvariant\top\n")
            for rec in sorted(read_cache(cache_path), key=lambda r: r["rev_id"]):
                if rec.get("status") != "ok" or not rec.get("text"):
                    continue
                for k in range(args.variants):
                    rng = random.Random(f"{args.seed}:{wiki}:{rec['rev_id']}:{k}")
                    text, op = mutate_text(rec["text"], rng)
                    if op == "unchanged":
                        continue
                    new_id = MUTATION_REV_BASE + rec["rev_id"] * 10 + k
                    m = dict(rec, rev_id=new_id, parent_id=rec["rev_id"], text=text,
                             comment=f"wcd-mutation {op}")
                    mutated.append(m)
                    lf.write(f"{new_id}\t{rec['rev_id']}\t{rec['page_id']}\t{k}\t{clean_cell(op)}\n")
        write_xml(out_dir / f"{wiki}.mutated.xml.bz2",
                  json.loads(site_path.read_text(encoding="utf-8")), mutated)
        log(f"{wiki}: mutation log at {log_path}")
    return 0


# ── probe ─────────────────────────────────────────────────────────────────────


def probe(args) -> int:
    for wiki in args.wikis:
        api = Api(wiki, delay=args.delay)
        print(f"\n== {wiki}.wikipedia.org")
        try:
            info = siteinfo(api)
            g = info["general"]
            print(f"  site: {g.get('sitename')} lang={g.get('lang')} "
                  f"articles={info.get('statistics', {}).get('articles')}")
        except Exception as e:
            print(f"  siteinfo FAILED: {e}")
            continue
        try:
            cats = linter_categories(api)
            shown = ", ".join(f"{k}={v}" for k, v in sorted(cats.items(), key=lambda x: -x[1])[:12])
            print(f"  linter categories ({len(cats)}): {shown}")
            first = next(iter(sorted(cats)), None)
            if first:
                d = api.get(list="linterrors", lntcategories=first, lntnamespace=0, lntlimit=2)
                print(f"  linterrors sample [{first}]: {d['query'].get('linterrors', [])[:2]}")
        except Exception as e:
            print(f"  linter FAILED: {e}")
        try:
            names = tracking_categories(api, info["general"].get("lang", wiki))
            sizes = category_sizes(api, [n for n, _ in names])
            for n, origin in names:
                print(f"  tracking [{origin}] {n}: {sizes.get(n, 'not found')} pages")
        except Exception as e:
            print(f"  tracking FAILED: {e}")
        try:
            d = api.get(list="querypage", qppage="Longpages", qplimit=3)
            print(f"  Longpages: {[r['title'] for r in d['query']['querypage'].get('results', [])]}")
        except Exception as e:
            print(f"  Longpages FAILED: {e}")
        try:
            d = api.get(list="recentchanges", rctag="mw-reverted", rcnamespace=0, rctype="edit",
                        rcprop="ids|title|timestamp", rclimit="max")
            rc = d["query"].get("recentchanges", [])
            more = " (more available)" if "continue" in d else ""
            print(f"  mw-reverted edits in first page: {len(rc)}{more}")
            if rc:
                recs = list(fetch_batch(api, [int(rc[0]["revid"])]))
                print(f"  fetch round-trip rev {rc[0]['revid']}: status={recs[0]['status']} "
                      f"chars={len(recs[0].get('text') or '')}")
        except Exception as e:
            print(f"  recentchanges/fetch FAILED: {e}")
    return 0


# ── CLI ───────────────────────────────────────────────────────────────────────


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)

    def common(p, out_default="data/sample"):
        p.add_argument("--wikis", type=lambda s: [w.strip() for w in s.split(",") if w.strip()],
                       default=DEFAULT_WIKIS, help="comma-separated language codes")
        p.add_argument("--delay", type=float, default=0.1,
                       help="seconds to wait after each request (default 0.1)")
        p.add_argument("--out", default=out_default)

    p = sub.add_parser("probe", help="check every source on every wiki")
    common(p)
    p = sub.add_parser("manifest", help="choose revisions; writes fixtures/sample/*.tsv")
    common(p, "fixtures/sample")
    p.add_argument("--total", type=int, default=70_000)
    p.add_argument("--seed", type=int, default=20261008)
    p = sub.add_parser("fetch", help="download manifest revisions; writes <out>/<wiki>.xml.bz2")
    common(p)
    p.add_argument("--manifest", default="fixtures/sample")
    p = sub.add_parser("mutate", help="write damaged copies; <out>/<wiki>.mutated.xml.bz2")
    common(p)
    p.add_argument("--variants", type=int, default=1, help="damaged copies per revision")
    p.add_argument("--seed", type=int, default=20261008)

    args = ap.parse_args(argv)
    return {"probe": probe, "manifest": build_manifest, "fetch": fetch, "mutate": mutate}[args.cmd](args)


if __name__ == "__main__":
    raise SystemExit(main())
