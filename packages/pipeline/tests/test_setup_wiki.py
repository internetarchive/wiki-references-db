"""``wcd setup`` against a local content-history export site: discovery, resumable verified downloads,
shards streamed through a stub RevisionChest and the real Stage 1, resume after a
failure, and (with Postgres) the whole-wiki steps after the barrier."""
from __future__ import annotations

import hashlib
import http.server
import json
import os
import stat
import sys
import threading
from pathlib import Path

import pytest

from wcd_pipeline import dumps, setup_wiki
from wcd_pipeline.synthetic import PAGE_A, PAGE_B

DB, DATE, OLD, NEWER = "testwiki", "2026-11-01", "2026-10-01", "2026-12-01"
RUN_ID = f"{DB}-20261101"
D = "en.wikipedia.org"
#: page ranges as in real export file names; the synthetic pages are 101 (A) and 202 (B)
SHARDS = {f"{DB}-{DATE}-p1p150.xml.7z": 3_000,
          f"{DB}-{DATE}-p151p300.xml.7z": 9_000,
          f"{DB}-{DATE}-p301p400.xml.7z": 1_000}   # no synthetic pages: an empty shard
#: the one shard the run directory's listing leaves out: its size comes from a HEAD request
UNLISTED = f"{DB}-{DATE}-p301p400.xml.7z"


def _stem(name: str) -> str:
    return setup_wiki.bundle_name(name)[: -len(".mwrev.zst")]


class DumpSite:
    """A dumps.wikimedia.org look-alike on localhost, laid out like the content-history
    exports: directory listings, SHA256SUMS, and files served with Range support.
    Counts file GETs."""

    def __init__(self, root: Path):
        self.root, self.gets = root, []
        site = self

        class Handler(http.server.SimpleHTTPRequestHandler):
            def __init__(self, *a, **kw):
                super().__init__(*a, directory=str(site.root), **kw)

            def log_message(self, *a):
                pass

            def do_GET(self):
                path = Path(self.translate_path(self.path))
                rng = self.headers.get("Range")
                if not path.is_file() or path.suffix not in (".7z", ".bz2"):
                    return super().do_GET()
                site.gets.append((path.name, rng))
                data = path.read_bytes()
                start = int(rng.split("=")[1].split("-")[0]) if rng else 0
                if start >= len(data) and rng:
                    self.send_response(416)
                    self.end_headers()
                    return
                self.send_response(206 if rng else 200)
                self.send_header("Content-Length", str(len(data) - start))
                self.end_headers()
                self.wfile.write(data[start:])

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"

    def close(self):
        self.server.shutdown()


def _nginx_listing(entries) -> str:
    """An nginx autoindex page: names truncated in the text, full in the href."""
    rows = "".join(f'<a href="{n}">{n[:20]}..&gt;</a>{" " * 20}12-Nov-2026 05:19{" " * 12}{size}\n'
                   for n, size in entries)
    return f'<html><body><pre><a href="../">../</a>\n{rows}</pre></body></html>'


@pytest.fixture
def site(tmp_path):
    root = tmp_path / "site"
    wiki = root / dumps.DATASET / DB
    run = wiki / DATE / "xml" / "7z"
    run.mkdir(parents=True)
    sums = []
    for name, size in SHARDS.items():
        data = (hashlib.sha256(name.encode()).digest() * (size // 32 + 1))[:size]
        (run / name).write_bytes(data)
        sums.append(f"{hashlib.sha256(data).hexdigest()}  {name}\n")
    (run / dumps.SUMS).write_text("".join(sums))
    (run / "index.html").write_text(_nginx_listing((n, s) for n, s in SHARDS.items() if n != UNLISTED))
    # an older run whose SHA256SUMS is empty, and a newer one still being written (no SHA256SUMS): both skipped
    (wiki / OLD / "xml" / "7z").mkdir(parents=True)
    (wiki / OLD / "xml" / "7z" / dumps.SUMS).write_text("")
    (wiki / NEWER / "xml" / "7z").mkdir(parents=True)
    (wiki / NEWER / "xml" / "7z" / f"{DB}-{NEWER}-p1p150.xml.7z").write_bytes(b"partial")
    (wiki / "index.html").write_text("".join(f'<a href="{d}/">{d}/</a>\n' for d in (OLD, DATE, NEWER)))
    s = DumpSite(root)
    yield s
    s.close()


STUB = r'''#!{python}
"""Stub RevisionChest: `build <dump> -o <dir> --parquet <meta> --domain D [--namespace N]` writes
the synthetic bundle's pages in the dump name's p<lo>p<hi> range, and its metadata."""
import os, re, sys
import pyarrow as pa, pyarrow.parquet as pq, zstandard
from wcd_pipeline import synthetic
a = sys.argv[1:]
with open(os.environ["STUB_LOG"], "a") as fh:
    fh.write(os.path.basename(a[1]) + "\n")
if os.path.basename(a[1]) == os.environ.get("STUB_FAIL"):
    sys.exit(3)
name = os.path.basename(a[1])
lo, hi = map(int, re.search(r"p(\d+)p(\d+)", name).groups())
bundle = re.sub(r"\.(bz2|7z|gz|xml)$", "", name) + ".mwrev.zst"
synthetic.REVISIONS = [r for r in synthetic.REVISIONS if lo <= r[0] <= hi]
with open(os.path.join(a[a.index("-o") + 1], bundle), "wb") as fh:
    fh.write(zstandard.ZstdCompressor().compress(synthetic.render()))
rows = [r for r in synthetic.metadata_rows(bundle) if lo <= r["page_id"] <= hi]
schema = pa.schema([("type", pa.string()), ("page_id", pa.int64()), ("ns", pa.int64()), ("title", pa.string()),
                    ("rev_id", pa.int64()), ("parent_rev_id", pa.int64()), ("file_path", pa.string()),
                    ("offset_begin", pa.int64()), ("length", pa.int64()), ("timestamp", pa.string())])
pq.write_table(pa.Table.from_pylist(rows, schema=schema), a[a.index("--parquet") + 1])
'''


@pytest.fixture
def stub(tmp_path, monkeypatch):
    path = tmp_path / "RevisionChest"
    path.write_text(STUB.replace("{python}", sys.executable))
    path.chmod(path.stat().st_mode | stat.S_IXUSR)
    log = tmp_path / "stub.log"
    monkeypatch.setenv("STUB_LOG", str(log))
    monkeypatch.delenv("STUB_FAIL", raising=False)
    monkeypatch.delenv("WCD_RUN_ID", raising=False)
    return path, log


def _args(site, tmp_path, stub, *extra):
    return ["--domain", D, "--dbname", DB, "--mirror", site.url, "--work-dir", str(tmp_path / "work"),
            "--parquet-dir", str(tmp_path / "parquet"), "--jobs", "2", "--download-jobs", "2",
            "--revisionchest", str(stub), "--status-interval", "0.5", *extra]


# -- discovery and download ------------------------------------------------------

def test_find_run_takes_newest_published(site):
    run = dumps.find_run(DB, prefix=site.url)
    assert run.date == DATE and run.run_id == RUN_ID and len(run.files) == 3
    assert run.total_bytes == sum(SHARDS.values())                 # listed sizes, and one from HEAD
    assert {f.name: f.size for f in run.files} == SHARDS
    assert all(f.path.startswith(f"/{dumps.DATASET}/{DB}/{DATE}/xml/7z/") for f in run.files)
    assert dumps.find_run(DB, prefix=site.url, date="20261101").files == run.files    # either date form
    with pytest.raises(dumps.DumpError, match="not published"):
        dumps.find_run(DB, prefix=site.url, date=NEWER)            # still being written: no SHA256SUMS
    with pytest.raises(dumps.DumpError, match="not published"):
        dumps.find_run(DB, prefix=site.url, date=OLD)              # empty SHA256SUMS
    with pytest.raises(dumps.DumpError, match="no published"):
        dumps.find_run(DB, prefix=site.url, fmt="bz2")             # no bzip2 export at all


def test_parse_sums_and_listing():
    h = "ab" * 32
    assert dumps.parse_sums(f"{h}  a-p1p2.xml.7z\n{h.upper()} *b-p3r1r9.xml.7z\n\n") == \
        {"a-p1p2.xml.7z": h, "b-p3r1r9.xml.7z": h}
    with pytest.raises(dumps.DumpError):
        dumps.parse_sums("not a checksum line\n")
    html = _nginx_listing([("enwiki-2026-11-01-p100096p100653.xml.7z", 33821005),
                           ("enwiki-2026-11-01-p352651r1645726r973546134.xml.7z", 7)])
    html += '<a href="SHA256SUMS">SHA256SUMS</a>   12-Nov-2026 12:25   1.2K\n'       # not a byte count: skipped
    assert dumps.parse_listing_sizes(html) == {"enwiki-2026-11-01-p100096p100653.xml.7z": 33821005,
                                               "enwiki-2026-11-01-p352651r1645726r973546134.xml.7z": 7}
    assert dumps.normalize_date("20261101") == dumps.normalize_date("2026-11-01") == "2026-11-01"
    with pytest.raises(dumps.DumpError):
        dumps.normalize_date("Nov 2026")


def test_download_resumes_and_verifies(site, tmp_path):
    run = dumps.find_run(DB, prefix=site.url)
    f = max(run.files, key=lambda f: f.size)
    dest = tmp_path / "dl"
    dest.mkdir()
    full = (site.root / f.path.lstrip("/")).read_bytes()
    (dest / (f.name + ".part")).write_bytes(full[:4000])           # an interrupted download
    got = dumps.download(f, dest, prefix=site.url)
    assert got.read_bytes() == full and dumps.is_downloaded(got) and not (dest / (f.name + ".part")).exists()
    assert site.gets[-1] == (f.name, "bytes=4000-")
    assert Path(f"{got}.ok").read_text().strip() == f.sha256
    n = len(site.gets)
    dumps.download(f, dest, prefix=site.url)                         # verified already: no request
    assert len(site.gets) == n
    bad = dumps.DumpFile(f.name, f.path, f.size, "0" * 64)
    with pytest.raises(dumps.DumpError, match="sha256"):
        dumps.download(bad, tmp_path / "dl2", prefix=site.url)
    assert not list((tmp_path / "dl2").iterdir())                   # a bad file is not kept


def test_bundle_name_matches_revisionchest():
    assert setup_wiki.bundle_name("enwiki-2026-11-01-p1p794.xml.7z") == "enwiki-2026-11-01-p1p794.xml.mwrev.zst"
    assert setup_wiki.bundle_name("enwiki-2026-11-01-p352651r1645726r973546134.xml.bz2") == \
        "enwiki-2026-11-01-p352651r1645726r973546134.xml.mwrev.zst"
    assert setup_wiki.bundle_name("a.xml") == "a.mwrev.zst"
    assert setup_wiki.bundle_name("a") == "a.mwrev.zst"


# -- streaming shards ------------------------------------------------------------

def test_shards_stream_and_resume(site, tmp_path, stub, monkeypatch):
    path, log = stub
    monkeypatch.setenv("STUB_FAIL", f"{DB}-{DATE}-p151p300.xml.7z")
    assert setup_wiki.main(_args(site, tmp_path, path, "--shards-only")) == 1    # one shard fails
    run = tmp_path / "work" / D / RUN_ID
    staging = run / "staging"
    assert (staging / _stem(f"{DB}-{DATE}-p1p150.xml.7z") / "DONE.txt").exists()
    assert (staging / _stem(f"{DB}-{DATE}-p301p400.xml.7z") / "DONE.txt").exists()   # empty shard is fine
    assert not (staging / _stem(f"{DB}-{DATE}-p151p300.xml.7z")).exists()
    assert len(site.gets) == 3 and all(dumps.is_downloaded(run / "dumps" / n) for n in SHARDS)
    # Stage 1 wrote the shard's tier files (page A is in shard 1)
    hist = list((tmp_path / "parquet" / "citation_history").rglob("*p1p150*.parquet"))
    assert hist, "citation_history for shard 1"

    # re-run: nothing is downloaded again, only the failed shard is built and extracted
    monkeypatch.delenv("STUB_FAIL")
    log.write_text("")
    assert setup_wiki.main(_args(site, tmp_path, path, "--shards-only")) == 0
    assert len(site.gets) == 3
    assert log.read_text().split() == [f"{DB}-{DATE}-p151p300.xml.7z"]
    assert (staging / _stem(f"{DB}-{DATE}-p151p300.xml.7z") / "DONE.txt").exists()
    assert sorted(p.name for p in (run / "meta").iterdir()) == sorted(
        setup_wiki.bundle_name(n).replace(".mwrev.zst", ".parquet") for n in SHARDS)
    assert json.loads((run / "run.json").read_text())["run_id"] == RUN_ID


def test_delete_dumps_keeps_markers(site, tmp_path, stub):
    path, _ = stub
    assert setup_wiki.main(_args(site, tmp_path, path, "--shards-only", "--delete-dumps")) == 0
    run = tmp_path / "work" / D / RUN_ID
    assert not any((run / "dumps" / n).exists() for n in SHARDS)
    assert all(Path(f"{run / 'dumps' / n}.ok").exists() for n in SHARDS)
    assert setup_wiki.main(_args(site, tmp_path, path, "--shards-only", "--delete-dumps")) == 0   # nothing to redo
    assert len(site.gets) == 3


def test_bundles_cut_into_pieces(site, tmp_path, stub):
    """--pieces 2: each bundle becomes two extract units on page boundaries; the pieces are
    deleted once extracted, and the shard counts as extracted when both are done."""
    path, _ = stub
    assert setup_wiki.main(_args(site, tmp_path, path, "--shards-only", "--pieces", "2")) == 0
    run = tmp_path / "work" / D / RUN_ID
    for name in SHARDS:
        shard = _stem(name)
        assert (run / "state" / f"{shard}.pieces").read_text().strip() == "2"
        assert all((run / "staging" / f"{shard}-{i}" / "DONE.txt").exists() for i in (0, 1))
        assert not (run / "staging" / shard).exists()
    assert not list((run / "pieces").iterdir())
    # tier files carry the piece stems
    assert list((tmp_path / "parquet" / "citation_history").rglob("*p1p150.xml-*.parquet"))


def test_pieces_are_automatic_only_with_few_files(site, tmp_path, stub, monkeypatch):
    path, _ = stub
    monkeypatch.setattr(setup_wiki, "MIN_PIECE_BYTES", 1)      # the synthetic bundles are tiny
    # 3 files, --jobs 2: enough files, no pieces
    assert setup_wiki.main(_args(site, tmp_path, path, "--shards-only")) == 0
    run = tmp_path / "work" / D / RUN_ID
    assert {p.read_text().strip() for p in (run / "state").glob("*.pieces")} == {"1"}
    # 3 files, --jobs 7: ceil(7 / 3) = 3 pieces each
    args = _args(site, tmp_path, path, "--shards-only", "--work-dir", str(tmp_path / "work7"))
    args[args.index("--jobs") + 1] = "7"
    assert setup_wiki.main(args) == 0
    run7 = tmp_path / "work7" / D / RUN_ID
    assert {p.read_text().strip() for p in (run7 / "state").glob("*.pieces")} == {"3"}


def test_dry_run_lists_shards(site, tmp_path, stub, capsys):
    path, _ = stub
    assert setup_wiki.main(_args(site, tmp_path, path, "--dry-run")) == 0
    out = capsys.readouterr().out
    assert all(n in out for n in SHARDS) and not site.gets


# -- the whole wiki, after the barrier (Postgres) --------------------------------

DSN = os.environ.get("WCD_TEST_DATABASE_URL")


@pytest.mark.skipif(not DSN, reason="set WCD_TEST_DATABASE_URL to a scratch Postgres database")
def test_setup_builds_the_whole_wiki(site, tmp_path, stub, monkeypatch):
    from sqlalchemy import create_engine, text

    url = "postgresql+psycopg2://" + DSN.split("://", 1)[1]
    engine = create_engine(url)
    with engine.begin() as con:
        con.execute(text("DROP SCHEMA public CASCADE"))
        con.execute(text("CREATE SCHEMA public"))
    monkeypatch.setenv("DATABASE_URL", DSN)
    path, _ = stub
    try:
        # with pieces: the whole-wiki steps see piece staging dirs, the metadata the original bundles
        assert setup_wiki.main(_args(site, tmp_path, path, "--pieces", "2")) == 0
        run = tmp_path / "work" / D / RUN_ID
        steps = [n for n, _ in setup_wiki.whole_wiki_steps(D, run, 2)]
        assert all((run / "state" / "steps" / f"{n}.done").exists() for n in steps)
        with engine.connect() as con:
            assert con.execute(text("SELECT count(*) FROM page_summary_cache")).scalar() == 2
            assert {r[0] for r in con.execute(text("SELECT page_id FROM page_metadata"))} == {PAGE_A, PAGE_B}
            gids = [r[0] for r in con.execute(text("SELECT global_source_id FROM global_sources"))]
            assert gids and all(g.startswith(f"{RUN_ID}.") for g in gids)   # the run id qualifies them
        # a second run finds everything done
        assert setup_wiki.main(_args(site, tmp_path, path, "--pieces", "2")) == 0
    finally:
        engine.dispose()
