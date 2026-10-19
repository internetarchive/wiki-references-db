#!/usr/bin/env python3
"""Export the full revision history of the fixture pages, up to a cutoff, from the Action API.

This approximates a `pages-meta-history` dump taken at the cutoff, without
downloading dump chunks: for every page in fixtures/pages.tsv (written by
scripts/select_fixture_pages.py, which also records the wiki and the cutoff)
it pages through prop=revisions with rvdir=newer and rvend=<cutoff>, caches
the revisions as JSONL, and writes MediaWiki export-0.11 XML that
`RevisionChest build` reads.

Subcommands (run in this order):

  fetch   Download every page's history into <out>/cache/. Resumable: a page
          is checkpointed after each API batch (<page_id>.continue.json) and
          marked finished with <page_id>.done.json; re-running skips finished
          pages and resumes unfinished ones where they stopped.
  write   Stream the cache into <out>/fixture.xml.bz2 (every page) and
          <out>/fixture-ci.xml.bz2 (tier = ci), plus <out>/export.json with
          per-page counts, problems and the SHA-256 of both files.

Differences from a real dump taken at the cutoff (acceptable for a fixture;
see fixtures/README.md): revisions or pages deleted since the cutoff are
missing; revisions history-merged into a page since the cutoff are present;
titles are today's titles; pages created after the cutoff have no revisions
and are left out (export.json lists them). Revision-deleted text is written
as dumps write it, <text bytes="0" deleted="deleted" />. Revisions appear in
timestamp order (rvdir=newer).

Requests are serial, carry maxlag=5, back off on 429/5xx/maxlag, and send a
User-Agent with CONTACT_EMAIL (from the environment or .env).

Usage:
    uv run python scripts/export_history.py fetch [--pages fixtures/pages.tsv] [--out data/fixture]
    uv run python scripts/export_history.py write [--pages fixtures/pages.tsv] [--out data/fixture]
    RevisionChest build data/fixture/fixture.xml.bz2 -o fixtures/shards/rehearsal \\
        --parquet fixtures/shards/rehearsal/meta.parquet --domain en.wikipedia.org
"""
from __future__ import annotations

import argparse
import bz2
import hashlib
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import sample_revisions as sr  # noqa: E402  (Api client, record shape, XML writers)

SCRIPT_VERSION = "1"
RVPROP = "ids|timestamp|user|userid|comment|content|sha1|size"
CUTOFF_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
log = sr.log


# ── pages.tsv ─────────────────────────────────────────────────────────────────


@dataclass
class PageRow:
    page_id: int
    title: str
    tier: str
    stratum: str


def read_pages(path: Path) -> tuple[dict[str, str], list[PageRow]]:
    """Return (header metadata from '# key: value' lines, rows)."""
    meta: dict[str, str] = {}
    rows: list[PageRow] = []
    header: list[str] | None = None
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.rstrip("\n")
            if not line.strip():
                continue
            if line.startswith("#"):
                m = re.match(r"#\s*([\w-]+):\s*(.*)$", line)
                if m:
                    meta[m.group(1)] = m.group(2).strip()
                continue
            cells = line.split("\t")
            if header is None:
                header = cells
                continue
            d = dict(zip(header, cells))
            rows.append(PageRow(int(d["page_id"]), d.get("title", ""), d.get("tier", "rehearsal"),
                                d.get("stratum", "")))
    for key in ("wiki", "cutoff"):
        if key not in meta:
            raise SystemExit(f"{path}: missing '# {key}: …' header line")
    if not CUTOFF_RE.match(meta["cutoff"]):
        raise SystemExit(f"{path}: cutoff {meta['cutoff']!r} is not YYYY-MM-DDTHH:MM:SSZ")
    return meta, rows


# ── fetch ─────────────────────────────────────────────────────────────────────


def _write_atomic(path: Path, text: str) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def iter_cached(path: Path):
    """Yield cached records once each, in file order; tolerate a torn final line."""
    if not path.exists():
        return
    seen: set[int] = set()
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            if rec["rev_id"] in seen:
                continue
            seen.add(rec["rev_id"])
            yield rec


def fetch_page(api: sr.Api, page_id: int, cutoff: str, cache: Path) -> dict:
    done_path = cache / f"{page_id}.done.json"
    if done_path.exists():
        return json.loads(done_path.read_text(encoding="utf-8"))
    data_path = cache / f"{page_id}.jsonl"
    cont_path = cache / f"{page_id}.continue.json"
    cont = json.loads(cont_path.read_text(encoding="utf-8")) if cont_path.exists() else {}
    params = dict(prop="revisions", pageids=page_id, rvdir="newer", rvend=cutoff,
                  rvslots="main", rvlimit="max", rvprop=RVPROP)
    status = "ok"
    batches = 0
    with open(data_path, "a", encoding="utf-8") as fh:
        while True:
            data = api.get(**params, **cont)
            pages = data.get("query", {}).get("pages", [])
            p = pages[0] if pages else {}
            if not p or p.get("missing") or p.get("invalid"):
                status = "missing"
                break
            for rev in p.get("revisions", []):
                fh.write(json.dumps(sr.rev_record(p, rev), ensure_ascii=False) + "\n")
            fh.flush()
            batches += 1
            if "continue" not in data:
                break
            cont = data["continue"]
            _write_atomic(cont_path, json.dumps(cont))
            if batches % 20 == 0:
                log(f"  page {page_id}: {batches} batches so far")

    n = hidden = 0
    first = last = title = None
    for rec in iter_cached(data_path):
        n += 1
        hidden += rec.get("text") is None
        first = first or rec["timestamp"]
        last = rec["timestamp"]
        title = rec["title"]
    if status == "ok" and n == 0:
        status = "no-revisions-before-cutoff"
    result = {"page_id": page_id, "title": title, "status": status, "revisions": n,
              "texthidden": hidden, "first": first, "last": last}
    _write_atomic(done_path, json.dumps(result, ensure_ascii=False))
    if cont_path.exists():
        cont_path.unlink()
    return result


def fetch(args) -> int:
    meta, rows = read_pages(Path(args.pages))
    out = Path(args.out)
    cache = out / "cache"
    cache.mkdir(parents=True, exist_ok=True)
    api = sr.Api(meta["wiki"], delay=args.delay)
    site_path = cache / "siteinfo.json"
    if not site_path.exists():
        site_path.write_text(json.dumps(sr.siteinfo(api), ensure_ascii=False), encoding="utf-8")
    total = 0
    problems: dict[str, int] = {}
    log(f"{len(rows)} pages from {args.pages}; cutoff {meta['cutoff']}")
    for i, row in enumerate(rows, 1):
        r = fetch_page(api, row.page_id, meta["cutoff"], cache)
        total += r["revisions"]
        if r["status"] != "ok":
            problems[r["status"]] = problems.get(r["status"], 0) + 1
        log(f"[{i}/{len(rows)}] {row.page_id} {r['title'] or row.title}: {r['revisions']} revisions"
            + ("" if r["status"] == "ok" else f" ({r['status']})"))
    log(f"done: {total} revisions" + (f"; problems {problems}" if problems else ""))
    return 0


# ── write ─────────────────────────────────────────────────────────────────────


def write_dump(path: Path, site: dict, rows: list[PageRow], cache: Path, cutoff: str) -> dict:
    stats = {"pages": 0, "revisions": 0, "texthidden": 0, "skipped": {}, "left_out": {}}
    skipped = stats["skipped"]
    with bz2.open(path, "wt", encoding="utf-8") as fh:
        fh.write(sr.xml_header(site))
        for row in sorted(rows, key=lambda r: r.page_id):
            done_path = cache / f"{row.page_id}.done.json"
            if not done_path.exists():
                stats["left_out"][row.page_id] = "not fetched"
                continue
            done = json.loads(done_path.read_text(encoding="utf-8"))
            if done["status"] != "ok":
                stats["left_out"][row.page_id] = done["status"]
                continue
            opened = False
            for rec in iter_cached(cache / f"{row.page_id}.jsonl"):
                if rec["timestamp"] > cutoff:  # ISO-8601 Z strings sort chronologically
                    skipped["after-cutoff"] = skipped.get("after-cutoff", 0) + 1
                    continue
                if rec.get("text") is not None:
                    bad = sr._invalid_xml_char(rec["text"])
                    if bad:
                        key = f"invalid-xml-char {bad}"
                        skipped[key] = skipped.get(key, 0) + 1
                        continue
                if not opened:
                    fh.write(sr.xml_page_open(row.page_id, rec["title"], rec["ns"]))
                    opened = True
                fh.write(sr.xml_revision(rec))
                stats["revisions"] += 1
                stats["texthidden"] += rec.get("text") is None
            if opened:
                fh.write(sr.XML_PAGE_CLOSE)
                stats["pages"] += 1
        fh.write(sr.XML_FOOTER)
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            h.update(block)
    stats["sha256"] = h.hexdigest()
    stats["bytes"] = path.stat().st_size
    log(f"wrote {stats['revisions']} revisions on {stats['pages']} pages to {path}"
        + (f"; skipped {skipped}" if skipped else "")
        + (f"; left out {len(stats['left_out'])} pages" if stats["left_out"] else ""))
    return stats


def write(args) -> int:
    meta, rows = read_pages(Path(args.pages))
    out = Path(args.out)
    cache = out / "cache"
    site_path = cache / "siteinfo.json"
    if not site_path.exists():
        raise SystemExit(f"{site_path} not found; run `fetch` first")
    site = json.loads(site_path.read_text(encoding="utf-8"))
    summary = {
        "script_version": SCRIPT_VERSION,
        "created": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "pages_file": str(args.pages),
        "wiki": meta["wiki"],
        "cutoff": meta["cutoff"],
        "outputs": {},
    }
    ci = [r for r in rows if r.tier == "ci"]
    for name, subset in (("fixture.xml.bz2", rows), ("fixture-ci.xml.bz2", ci)):
        summary["outputs"][name] = write_dump(out / name, site, subset, cache, meta["cutoff"])
    (out / "export.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n",
                                     encoding="utf-8")
    left = summary["outputs"]["fixture.xml.bz2"]["left_out"]
    return 1 if any(v == "not fetched" for v in left.values()) else 0


# ── CLI ───────────────────────────────────────────────────────────────────────


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name, helptext in (("fetch", "download page histories into <out>/cache (resumable)"),
                           ("write", "write <out>/fixture.xml.bz2, fixture-ci.xml.bz2, export.json")):
        p = sub.add_parser(name, help=helptext)
        p.add_argument("--pages", default="fixtures/pages.tsv")
        p.add_argument("--out", default="data/fixture")
        if name == "fetch":
            p.add_argument("--delay", type=float, default=0.1,
                           help="seconds to wait after each request (default 0.1)")
    args = ap.parse_args(argv)
    return {"fetch": fetch, "write": write}[args.cmd](args)


if __name__ == "__main__":
    raise SystemExit(main())
