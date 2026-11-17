"""Wikimedia content-history exports: discovery and download, for ``wcd setup``.

A wiki's full history is published monthly as a MediaWiki Content File Export
(https://wikitech.wikimedia.org/wiki/MediaWiki_Content_File_Exports), the
replacement for the legacy ``pages-meta-history`` XML dumps, which are not
published after January 2027. A run lives at::

    <prefix>/other/mediawiki_content_history/<dbname>/<YYYY-MM-DD>/xml/<7z|bzip2>/

where ``prefix`` is https://dumps.wikimedia.org or a mirror with the same
layout, ``dbname`` the site's ``wikiid`` (``en.wikipedia.org`` → ``enwiki``)
and the date the run's cutoff: each export is the wiki as of 00:00 UTC that
day, consistent across pages. The XML is the legacy dumps' format
(export-0.11). Files are split by page-id range (``<dbname>-<date>-p<lo>p<hi>.xml.7z``);
a very large page is split further by revision range (``…-p<page>r<lo>r<hi>.xml.7z``),
so one page can span several files.

A run is published when its ``SHA256SUMS`` exists: it lists every file with its
sha256, and appears only once the whole export is written. File sizes come from
the directory listing (or a HEAD request when the listing does not show one).

Downloads resume (HTTP Range) into ``<file>.part``, are checked against the
size and sha256, then renamed and marked ``<file>.ok``. Only the standard
library is used.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

PRIMARY = "https://dumps.wikimedia.org"
#: the content-history dataset, below the prefix
DATASET = "other/mediawiki_content_history"
#: ``--format`` → the run's subdirectory under ``xml/``
FORMATS = {"7z": "7z", "bz2": "bzip2"}
SUMS = "SHA256SUMS"
CHUNK = 4 * 2**20


class DumpError(RuntimeError):
    pass


@dataclass(frozen=True)
class DumpFile:
    name: str
    path: str        # URL path below the prefix, e.g. /other/mediawiki_content_history/enwiki/2026-11-01/xml/7z/enwiki-2026-11-01-p1p794.xml.7z
    size: int
    sha256: str


@dataclass(frozen=True)
class DumpRun:
    dbname: str
    date: str        # the export's date, YYYY-MM-DD (its cutoff, 00:00 UTC)
    fmt: str
    files: tuple[DumpFile, ...]

    @property
    def run_id(self) -> str:
        """``<dbname>-<YYYYMMDD>``: the build run id (``WCD_RUN_ID``) of a build from this export."""
        return f"{self.dbname}-{self.date.replace('-', '')}"

    @property
    def total_bytes(self) -> int:
        return sum(f.size for f in self.files)


def user_agent() -> str:
    """Wikimedia asks automated clients for a descriptive User-Agent with contact
    details: ``WIKIPEDIA_API_USER_AGENT`` plus ``(WIKIPEDIA_API_CONTACT_EMAIL)``."""
    ua = os.getenv("WIKIPEDIA_API_USER_AGENT") or "WikipediaCitationsDatabase/2.0"
    contact = os.getenv("WIKIPEDIA_API_CONTACT_EMAIL") or os.getenv("CONTACT_EMAIL")
    return f"{ua} ({contact})" if contact else ua


def _open(url: str, *, headers: dict | None = None, timeout: float = 60, method: str | None = None):
    req = urllib.request.Request(url, headers={"User-Agent": user_agent(), **(headers or {})}, method=method)
    return urllib.request.urlopen(req, timeout=timeout)


def http_get(url: str, *, retries: int = 4, timeout: float = 60) -> bytes:
    """GET with a few retries on network errors and 5xx; 4xx raise at once."""
    for attempt in range(retries + 1):
        try:
            with _open(url, timeout=timeout) as resp:
                return resp.read()
        except urllib.error.HTTPError as e:
            if e.code < 500 or attempt == retries:
                raise
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            if attempt == retries:
                raise
        time.sleep(min(60, 2 ** attempt))
    raise AssertionError("unreachable")


def http_size(url: str, *, retries: int = 4) -> int:
    """Content-Length of ``url`` from a HEAD request."""
    for attempt in range(retries + 1):
        try:
            with _open(url, method="HEAD") as resp:
                length = resp.headers.get("Content-Length")
                if length is None:
                    raise DumpError(f"{url}: no Content-Length")
                return int(length)
        except urllib.error.HTTPError as e:
            if e.code < 500 or attempt == retries:
                raise DumpError(f"{url}: HTTP {e.code}") from e
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            if attempt == retries:
                raise DumpError(f"{url}: {e}") from e
        time.sleep(min(60, 2 ** attempt))
    raise AssertionError("unreachable")


def wiki_dbname(domain: str) -> str:
    """The site's database name (``wikiid``) from its siteinfo: what the exports are filed under."""
    body = http_get(f"https://{domain}/w/api.php?action=query&meta=siteinfo&siprop=general&format=json")
    try:
        return json.loads(body)["query"]["general"]["wikiid"]
    except (KeyError, ValueError) as e:
        raise DumpError(f"no wikiid in {domain}'s siteinfo") from e


def normalize_date(date: str) -> str:
    """``YYYYMMDD`` or ``YYYY-MM-DD`` → ``YYYY-MM-DD`` (the exports' directory names)."""
    m = re.fullmatch(r"(\d{4})-?(\d{2})-?(\d{2})", date.strip())
    if not m:
        raise DumpError(f"--date {date!r} is not YYYYMMDD or YYYY-MM-DD")
    return "-".join(m.groups())


def _wiki_url(prefix: str, dbname: str) -> str:
    return f"{prefix.rstrip('/')}/{DATASET}/{dbname}/"


def _run_path(dbname: str, date: str, fmt: str) -> str:
    """URL path of a run's file directory, below the prefix (leading and trailing slash)."""
    return f"/{DATASET}/{dbname}/{date}/xml/{FORMATS[fmt]}/"


def list_dates(prefix: str, dbname: str) -> list[str]:
    """Export dates in the wiki's directory listing, newest first."""
    html = http_get(_wiki_url(prefix, dbname)).decode("utf-8", "replace")
    return sorted(set(re.findall(r'href="(\d{4}-\d{2}-\d{2})/"', html)), reverse=True)


def parse_sums(text: str) -> dict[str, str]:
    """``SHA256SUMS`` (``<sha256>  <name>`` per line, ``sha256sum`` format) → {name: sha256}."""
    sums = {}
    for line in text.splitlines():
        if not line.strip():
            continue
        m = re.fullmatch(r"([0-9a-fA-F]{64}) [ *]?(\S.*)", line.strip())
        if not m:
            raise DumpError(f"unexpected {SUMS} line: {line!r}")
        sums[Path(m.group(2).strip()).name] = m.group(1).lower()
    return sums


def parse_listing_sizes(html: str) -> dict[str, int]:
    """File sizes in bytes from an nginx-style directory listing
    (``<a href="NAME">…</a>  12-Nov-2026 05:19  95504828``). The ``href`` holds the
    full name even where the shown text is truncated. Entries without a byte count
    (directories, human-readable sizes) are left out."""
    return {urllib.request.url2pathname(name): int(size)
            for name, size in re.findall(r'<a href="([^"/?]+)">[^<]*</a>\s+\S+\s+\S+\s+(\d+)\s*$', html, re.M)}


def run_files(prefix: str, dbname: str, date: str, fmt: str) -> tuple[DumpFile, ...] | None:
    """The run's files, or None when it is not published (no ``SHA256SUMS`` yet)."""
    base = prefix.rstrip("/") + _run_path(dbname, date, fmt)
    try:
        sums = parse_sums(http_get(base + SUMS).decode("utf-8", "replace"))
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise
    if not sums:
        return None
    sizes = parse_listing_sizes(http_get(base).decode("utf-8", "replace"))
    missing = [n for n in sums if n not in sizes]
    if missing:                                       # the listing does not show them all: ask the server
        with ThreadPoolExecutor(16) as pool:
            sizes.update(zip(missing, pool.map(lambda n: http_size(base + urllib.request.pathname2url(n)), missing)))
    path = _run_path(dbname, date, fmt)
    return tuple(DumpFile(name=n, path=path + n, size=sizes[n], sha256=h) for n, h in sorted(sums.items()))


def find_run(dbname: str, *, prefix: str = PRIMARY, date: str | None = None, fmt: str = "7z",
             log: Callable[[str], None] = lambda m: None) -> DumpRun:
    """The run to build: ``date`` if given (it must be published), else the newest
    published run. A run is published once its ``SHA256SUMS`` exists."""
    if fmt not in FORMATS:
        raise DumpError(f"format {fmt!r}: one of {', '.join(FORMATS)}")
    dates = [normalize_date(date)] if date else list_dates(prefix, dbname)
    if not dates:
        raise DumpError(f"no content-history exports listed at {_wiki_url(prefix, dbname)}")
    for d in dates:
        files = run_files(prefix, dbname, d, fmt)
        if files:
            return DumpRun(dbname=dbname, date=d, fmt=fmt, files=files)
        if date:
            raise DumpError(f"{dbname} {d}: the {fmt} export is not published (no {SUMS})")
        log(f"{dbname} {d}: {fmt} export not published yet, trying an older run")
    raise DumpError(f"no published {dbname} {fmt} export at {_wiki_url(prefix, dbname)}")


def _sha256_of(path: Path) -> "hashlib._Hash":
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(CHUNK), b""):
            h.update(block)
    return h


def is_downloaded(dest: Path) -> bool:
    return dest.exists() and Path(f"{dest}.ok").exists()


def download(f: DumpFile, dest_dir: Path, *, prefix: str = PRIMARY, retries: int = 8,
             progress: Callable[[int], None] = lambda n: None) -> Path:
    """Fetch ``f`` into ``dest_dir`` (resuming a ``.part``), verify size and sha256,
    mark it ``.ok``. Already-verified files are skipped. ``progress`` gets byte
    counts as they arrive. A checksum mismatch deletes the partial file and fails."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / f.name
    if is_downloaded(dest):
        return dest
    part = dest_dir / (f.name + ".part")
    url = prefix.rstrip("/") + urllib.request.pathname2url(f.path)
    for attempt in range(retries + 1):
        have = part.stat().st_size if part.exists() else 0
        if have > f.size:
            part.unlink()
            have = 0
        try:
            if have < f.size:
                headers = {"Range": f"bytes={have}-"} if have else {}
                with _open(url, headers=headers, timeout=120) as resp:
                    if have and resp.status != 206:      # server ignored the range: start over
                        have = 0
                    with open(part, "ab" if have else "wb") as out:
                        while True:
                            block = resp.read(CHUNK)
                            if not block:
                                break
                            out.write(block)
                            progress(len(block))
            break
        except urllib.error.HTTPError as e:
            if e.code == 416:              # range past the end: the part is complete (or bad); verify below
                break
            if e.code < 500 or attempt == retries:
                raise DumpError(f"{url}: HTTP {e.code}") from e
        except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as e:
            if attempt == retries:
                raise DumpError(f"{url}: {e}") from e
        time.sleep(min(120, 2 ** attempt))
    size = part.stat().st_size if part.exists() else 0
    if size != f.size:
        raise DumpError(f"{f.name}: got {size:,} bytes, expected {f.size:,}")
    digest = _sha256_of(part).hexdigest()
    if digest != f.sha256:
        part.unlink()
        raise DumpError(f"{f.name}: sha256 {digest} does not match {SUMS}' {f.sha256}")
    os.replace(part, dest)
    Path(f"{dest}.ok").write_text(f"{f.sha256}\n")
    return dest
