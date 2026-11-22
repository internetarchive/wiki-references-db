"""A refresh equals a full build: the ci fixture built from its history before a cutoff (A),
then refreshed with the revisions after it (B), must give exactly the goldens of the full
build of A+B in fixtures/golden/ci/ (see test_fixture_ci, whose comparison it reuses).

This is the contract of docs/design/proposed_incremental_refresh.md §2. The refresh here is
whatever `refresh()` runs; today it is the runbook's "Incremental updates" path (stage0-load on
the metadata directory, extract the new bundle as a shard, then the domain from dedup on).
Every step that is made incremental must keep this test passing.

B holds the revisions from CUTOFF on of the pages with page_id % 4 != 0 (1,373 of 17,432
revisions, on 35 of the 51 pages, 3 of which are created in B), as a `sync` bundle would hold
recent edits; the other 16 pages are untouched, so a step that re-runs B's pages only has to
keep their rows as they were.

Needs a scratch Postgres database, like test_fixture_ci (its schema ``public`` is dropped).
``WCD_REFRESH_WORK=/some/dir`` keeps the work directory.
"""
from __future__ import annotations

import io
import json
import os
import shutil
import time
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
import pytest
import zstandard as zstd

from wcd_pipeline import cli
from test_fixture_ci import (DOMAIN, JOBS, PQ_DATASETS, SHARD, SNAPSHOT_NAMES, compare_with_golden,
                             pg_snapshots, pq_snapshot)

CUTOFF = "2024-01-01T00:00:00Z"
BUNDLE = SHARD / "fixture-ci.xml.mwrev.zst"

pytestmark = [
    pytest.mark.skipif(not os.environ.get("WCD_TEST_DATABASE_URL"),
                       reason="set WCD_TEST_DATABASE_URL to a scratch Postgres database"),
    pytest.mark.skipif(not (SHARD / "meta.parquet").exists(), reason=f"no fixture shard at {SHARD}"),
]


def in_b(page_id: int, timestamp: str) -> bool:
    return timestamp >= CUTOFF and page_id % 4 != 0


IN_B_SQL = f"(timestamp >= '{CUTOFF}' AND page_id % 4 <> 0)"   # in_b, for the metadata


def split_bundle(bundle: Path, a: Path, b: Path) -> tuple[int, int]:
    """Copy each revision of ``bundle`` to ``a`` or ``b`` (``in_b``); returns the counts."""
    counts = [0, 0]
    with open(a, "wb") as fa, open(b, "wb") as fb:
        wa = zstd.ZstdCompressor(level=3).stream_writer(fa)
        wb = zstd.ZstdCompressor(level=3).stream_writer(fb)
        target = None
        with open(bundle, "rb") as fh:
            reader = zstd.ZstdDecompressor().stream_reader(fh, read_across_frames=True)
            for line in io.BufferedReader(reader, buffer_size=4 * 2**20):
                if line.startswith(b"#"):
                    meta = dict(t.decode().split("=", 1) for t in line[1:].split() if b"=" in t)
                    k = int(in_b(int(meta["page_id"]), meta.get("timestamp", "")))
                    target = (wa, wb)[k]
                    counts[k] += 1
                if target is not None:
                    target.write(line)
        wa.close()
        wb.close()
    return counts[0], counts[1]


def split_metadata(meta: Path, a: Path, b: Path) -> None:
    """Revision rows by ``in_b`` (``IN_B_SQL``); each side gets the page rows of the pages it has revisions of
    (as RevisionChest writes a page row for every page in a build or sync batch)."""
    con = duckdb.connect()
    src = f"read_parquet('{meta}')"
    for path, cond in ((a, f"NOT {IN_B_SQL}"), (b, IN_B_SQL)):
        con.execute(f"""COPY (
            SELECT * FROM {src} WHERE type = 'revision' AND {cond}
            UNION ALL
            SELECT * FROM {src} WHERE type <> 'revision' AND page_id IN
                (SELECT page_id FROM {src} WHERE type = 'revision' AND {cond})
            ) TO '{path}' (FORMAT parquet)""")


class Run:
    def __init__(self, work: Path):
        self.work = work
        self.parquet = work / "parquet"
        self.staging = work / "staging"
        self.staging_b = work / "staging-b"
        self.meta = work / "meta"
        self.shard_a = work / "shard-a"
        self.shard_b = work / "shard-b"
        self.failed: str | None = None
        self.timings: dict[str, float] = {}

    def wcd(self, label: str, *argv: str) -> None:
        if self.failed:
            pytest.skip(f"skipped: earlier step {self.failed!r} failed")
        t0 = time.monotonic()
        try:
            rc = cli.main(list(argv))
        except BaseException:
            self.failed = label
            raise
        finally:
            self.timings[label] = round(time.monotonic() - t0, 2)
            (self.work / "timings.json").write_text(json.dumps(self.timings, indent=2) + "\n")
        if rc != 0:
            self.failed = label
            pytest.fail(f"`wcd {' '.join(argv)}` returned {rc}")

    def pq(self) -> tuple[str, str]:
        return "--parquet-dir", str(self.parquet)


@pytest.fixture(scope="module")
def run(tmp_path_factory) -> Run:
    keep = os.environ.get("WCD_REFRESH_WORK")
    if keep:
        work = Path(keep)
        if work.exists():
            shutil.rmtree(work)
        work.mkdir(parents=True)
    else:
        work = tmp_path_factory.mktemp("refresh_ci")
    os.environ["DATABASE_URL"] = os.environ["WCD_TEST_DATABASE_URL"]
    r = Run(work)
    for d in (r.meta, r.shard_a, r.shard_b):
        d.mkdir()
    return r


def test_00_split(run: Run):
    a, b = split_bundle(BUNDLE, run.shard_a / "fixture-ci-a.mwrev.zst", run.shard_b / "fixture-ci-b.mwrev.zst")
    split_metadata(SHARD / "meta.parquet", run.work / "meta-a.parquet", run.work / "meta-b.parquet")
    assert (a, b) == (16059, 1373), (a, b)
    con = duckdb.connect()
    assert con.execute(f"SELECT count(*) FILTER (WHERE type = 'revision'), count(DISTINCT page_id) "
                       f"FROM '{run.work / 'meta-b.parquet'}'").fetchone() == (1373, 35)
    assert pq.read_metadata(run.work / "meta-a.parquet").num_rows > a


def build(run: Run) -> None:
    """The full build of A, in README order (as test_fixture_ci runs it)."""
    from sqlalchemy import create_engine, text
    from wcd_pipeline.config import database_url
    engine = create_engine(database_url())
    with engine.begin() as con:
        con.execute(text("DROP SCHEMA public CASCADE"))
        con.execute(text("CREATE SCHEMA public"))
    engine.dispose()
    shutil.copy(run.work / "meta-a.parquet", run.meta / "a.parquet")
    run.wcd("A: init-db", "init-db", "--no-indexes")
    run.wcd("A: stage0-load", "stage0-load", "--domain", DOMAIN, "--metadata", str(run.meta), *run.pq())
    run.wcd("A: extract-all", "extract-all", "-d", str(run.shard_a), "-o", str(run.staging), "--domain", DOMAIN,
            *run.pq(), "--jobs", JOBS)
    run.wcd("A: dedup", "dedup", "-d", str(run.staging))
    run.wcd("A: load", "load", "-d", str(run.staging), *run.pq())
    run.wcd("A: init-db --add-indexes", "init-db", "--add-indexes")
    run.wcd("A: parts", "parts", "--domain", DOMAIN)
    run.wcd("A: denorm-export", "denorm-export", "--domain", DOMAIN, *run.pq())
    run.wcd("A: ref-name-links", "ref-name-links", "--domain", DOMAIN, *run.pq())
    run.wcd("A: match", "match", "--domain", DOMAIN, *run.pq(), "--jobs", JOBS)
    run.wcd("A: materialize", "materialize", "--domain", DOMAIN, *run.pq())
    run.wcd("A: revision-stats", "revision-stats", "--domain", DOMAIN, *run.pq(), "--jobs", JOBS)


def refresh(run: Run) -> None:
    """Apply B to the build of A, with every gate: the runbook's "Incremental updates" path,
    with the steps made incremental so far run for B's pages only."""
    shutil.copy(run.work / "meta-b.parquet", run.meta / "b.parquet")
    run.wcd("B: stage0-load", "stage0-load", "--domain", DOMAIN, "--metadata", str(run.meta), *run.pq())
    run.wcd("B: gate 0", "gate", "0", "--domain", DOMAIN, *run.pq())
    # B's shards staged on their own, deduped together with A's deduped set, and only the
    # rows new or changed against it loaded
    run.wcd("B: extract-all", "extract-all", "-d", str(run.shard_b), "-o", str(run.staging_b), "--domain", DOMAIN,
            *run.pq(), "--jobs", JOBS)
    run.wcd("B: dedup", "dedup", "-d", str(run.staging_b), "--previous", str(run.staging / "deduped"))
    run.wcd("B: load", "load", "-d", str(run.staging_b), *run.pq(), "--changes")
    run.wcd("B: gate 3", "gate", "3", "--domain", DOMAIN, *run.pq(), "--all-buckets")
    run.wcd("B: parts", "parts", "--domain", DOMAIN, "--resume",
            "--redo-from", str(run.staging_b / "deduped" / "changes" / "normalized_citations.parquet"))
    run.wcd("B: gate 4", "gate", "4", "--domain", DOMAIN, *run.pq())
    run.wcd("B: denorm-export", "denorm-export", "--domain", DOMAIN, *run.pq())
    run.wcd("B: ref-name-links", "ref-name-links", "--domain", DOMAIN, *run.pq())
    run.wcd("B: gate 5", "gate", "5", "--domain", DOMAIN, *run.pq())
    # 6a-6c for B's pages only (its metadata lists them); 6d over the domain
    run.wcd("B: match", "match", "--domain", DOMAIN, *run.pq(), "--jobs", JOBS,
            "--pages-from", str(run.meta / "b.parquet"))
    run.wcd("B: gate 6", "gate", "6", "--domain", DOMAIN, *run.pq())
    run.wcd("B: materialize", "materialize", "--domain", DOMAIN, *run.pq(), "--force")
    run.wcd("B: gate 7", "gate", "7", "--domain", DOMAIN, *run.pq())
    run.wcd("B: revision-stats", "revision-stats", "--domain", DOMAIN, *run.pq(), "--jobs", JOBS)
    run.wcd("B: gate 8", "gate", "8", "--domain", DOMAIN, *run.pq())


def test_01_build_a(run: Run):
    build(run)


def test_02_refresh_b(run: Run):
    refresh(run)


@pytest.fixture(scope="module")
def snapshots(run: Run) -> dict:
    if run.failed:
        pytest.skip(f"skipped: pipeline step {run.failed!r} failed")
    snaps = pg_snapshots(os.environ["WCD_TEST_DATABASE_URL"])
    con = duckdb.connect()
    for ds in PQ_DATASETS:
        t = pq_snapshot(con, run.parquet, ds)
        if t is not None:
            snaps["pq." + ds.replace("/", ".")] = t
    return snaps


@pytest.fixture(scope="module")
def duck():
    return duckdb.connect()


@pytest.mark.parametrize("name", SNAPSHOT_NAMES)
def test_10_refresh_equals_full_build(name: str, snapshots, duck):
    compare_with_golden(name, snapshots.get(name), duck)
