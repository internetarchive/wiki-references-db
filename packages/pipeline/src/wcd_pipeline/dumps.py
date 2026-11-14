"""Wikimedia dump discovery and download, for ``wcd setup``.

A wiki's dumps live at ``<prefix>/<dbname>/<date>/``, where ``prefix`` is
https://dumps.wikimedia.org or a mirror with the same layout (e.g.
https://dumps.wikimedia.your.org), ``dbname`` is the site's ``wikiid``
(``en.wikipedia.org`` → ``enwiki``) and ``<date>`` a run date (``20261101``).
Each run has a ``dumpstatus.json`` listing every job's files with size and
sha1; the page-history dump is the ``metahistory7zdump`` job (or
``metahistorybz2dump``), split into many files ("shards") on large wikis.

Downloads resume (HTTP Range) into ``<file>.part``, are checked against the
listed size and sha1, then renamed and marked ``<file>.ok`` — the marker the
earlier ``tools/fetch_one.sh`` wrote, so an existing dump directory is reused.
Only the standard library is used.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

PRIMARY = "https://dumps.wikimedia.org"
#: dumpstatus.json job name per history-dump format
HISTORY_JOBS = {"7z": "metahistory7zdump", "bz2": "metahistorybz2dump"}
CHUNK = 4 * 2**20


class DumpError(RuntimeError):
    pass


@dataclass(frozen=True)
class DumpFile:
    name: str
    path: str        # URL path below the prefix, e.g. /enwiki/20261101/enwiki-20261101-pages-meta-history1.xml-p1p812.7z
    size: int
    sha1: str


@dataclass(frozen=True)
class DumpRun:
    dbname: str
    date: str
    job: str
    files: tuple[DumpFile, ...]

    @property
    def run_id(self) -> str:
        """``<dbname>-<date>``: the build run id (``WCD_RUN_ID``) of a build from this dump."""
        return f"{self.dbname}-{self.date}"

    @property
    def total_bytes(self) -> int:
        return sum(f.size for f in self.files)


def user_agent() -> str:
    """Wikimedia asks automated clients for a descriptive User-Agent with contact
    details: ``WIKIPEDIA_API_USER_AGENT`` plus ``(WIKIPEDIA_API_CONTACT_EMAIL)``."""
    ua = os.getenv("WIKIPEDIA_API_USER_AGENT") or "WikipediaCitationsDatabase/2.0"
    contact = os.getenv("WIKIPEDIA_API_CONTACT_EMAIL") or os.getenv("CONTACT_EMAIL")
    return f"{ua} ({contact})" if contact else ua


def _open(url: str, *, headers: dict | None = None, timeout: float = 60):
    req = urllib.request.Request(url, headers={"User-Agent": user_agent(), **(headers or {})})
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


def wiki_dbname(domain: str) -> str:
    """The site's database name (``wikiid``) from its siteinfo: what the dumps are filed under."""
    body = http_get(f"https://{domain}/w/api.php?action=query&meta=siteinfo&siprop=general&format=json")
    try:
        return json.loads(body)["query"]["general"]["wikiid"]
    except (KeyError, ValueError) as e:
        raise DumpError(f"no wikiid in {domain}'s siteinfo") from e


def list_dates(prefix: str, dbname: str) -> list[str]:
    """Run dates in the wiki's dump directory listing, newest first."""
    html = http_get(f"{prefix.rstrip('/')}/{dbname}/").decode("utf-8", "replace")
    return sorted(set(re.findall(r'href="(\d{8})/"', html)), reverse=True)


def dump_status(prefix: str, dbname: str, date: str) -> dict:
    return json.loads(http_get(f"{prefix.rstrip('/')}/{dbname}/{date}/dumpstatus.json"))


def _history_files(status: dict, job: str) -> tuple[DumpFile, ...] | None:
    j = status.get("jobs", {}).get(job)
    if not j or j.get("status") != "done" or not j.get("files"):
        return None
    return tuple(DumpFile(name=n, path=f["url"], size=int(f["size"]), sha1=f["sha1"])
                 for n, f in sorted(j["files"].items()))


def find_run(dbname: str, *, prefix: str = PRIMARY, date: str | None = None, fmt: str = "7z",
             log: Callable[[str], None] = lambda m: None) -> DumpRun:
    """The run to build: ``date`` if given (its history job must be done), else the
    newest run whose history job is done. Listing and status come from ``prefix``;
    if the mirror has no ``dumpstatus.json`` for a run, the primary site's is used
    (the files themselves are still fetched from the mirror)."""
    job = HISTORY_JOBS[fmt]

    def status_for(d: str) -> dict | None:
        try:
            return dump_status(prefix, dbname, d)
        except urllib.error.HTTPError as e:
            if e.code != 404:
                raise
        if prefix.rstrip("/") == PRIMARY:
            return None
        log(f"{prefix} has no dumpstatus.json for {dbname}/{d}; using {PRIMARY}'s")
        try:
            return dump_status(PRIMARY, dbname, d)
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            raise

    dates = [date] if date else list_dates(prefix, dbname)
    if not dates:
        raise DumpError(f"no dump runs listed under {prefix}/{dbname}/")
    for d in dates:
        status = status_for(d)
        files = _history_files(status, job) if status else None
        if files:
            return DumpRun(dbname=dbname, date=d, job=job, files=files)
        if date:
            raise DumpError(f"{dbname} {d}: the {job} job is not done (or has no files)")
        log(f"{dbname} {d}: {job} not done, trying an older run")
    raise DumpError(f"no {dbname} run with a finished {job} under {prefix}")


def _sha1_of(path: Path) -> "hashlib._Hash":
    h = hashlib.sha1()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(CHUNK), b""):
            h.update(block)
    return h


def is_downloaded(dest: Path) -> bool:
    return dest.exists() and Path(f"{dest}.ok").exists()


def download(f: DumpFile, dest_dir: Path, *, prefix: str = PRIMARY, retries: int = 8,
             progress: Callable[[int], None] = lambda n: None) -> Path:
    """Fetch ``f`` into ``dest_dir`` (resuming a ``.part``), verify size and sha1,
    mark it ``.ok``. Already-verified files are skipped. ``progress`` gets byte
    counts as they arrive. A sha1 mismatch deletes the partial file and fails."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / f.name
    if is_downloaded(dest):
        return dest
    part = dest_dir / (f.name + ".part")
    url = prefix.rstrip("/") + f.path
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
    digest = _sha1_of(part).hexdigest()
    if digest != f.sha1:
        part.unlink()
        raise DumpError(f"{f.name}: sha1 {digest} does not match the listed {f.sha1}")
    os.replace(part, dest)
    Path(f"{dest}.ok").write_text(f"{f.sha1}\n")
    return dest
