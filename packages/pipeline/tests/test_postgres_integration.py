"""End-to-end Postgres run: ``init-db`` from the models → Stage 0 (RevisionChest
metadata → ``revisions`` / ``page_metadata`` + the ``revisions`` Parquet tier,
Stage-0 gate) → Stage 1 + 3 on the synthetic bundle → ``load``.

Needs a scratch database; skipped unless ``WCD_TEST_DATABASE_URL`` is set
(the schema ``public`` of that database is dropped and recreated). Optionally,
``REVISIONCHEST_BIN`` pointing at a RevisionChest binary makes the test
regenerate the Stage-0 metadata from the mwdump fixture and check that the
committed ``fixtures/golden/stage0/fixture.parquet`` is still what it writes.
"""
from __future__ import annotations

import os
import subprocess
from pathlib import Path

import duckdb
import pytest
import zstandard as zstd
from sqlalchemy import create_engine, text

from wcd_pipeline import dedup, extract, gates, init_db, load, stage0
from wcd_pipeline.config import database_url
from wcd_schema import parquet as layout
from wcd_schema.ids import instance_id
from wcd_schema.postgres import Base

from synthetic_bundle import render  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[3]
STAGE0_META = REPO_ROOT / "fixtures" / "golden" / "stage0" / "fixture.parquet"
MWDUMP_FIXTURE = REPO_ROOT / "rust" / "revisionchest" / "crates" / "mwdump" / "tests" / "fixtures" / "fixture.xml.bz2"
DOMAIN = "en.wikipedia.org"

# From the mwdump fixture (crates/mwdump/tests/fixtures/expected.json)
FIXTURE_PAGES = {101: ("Example Island", 0), 102: ("Talk:Example Island", 1),
                 103: ("Redirect Page", 0), 104: ("Draft:Something", 118)}
FIXTURE_REVISIONS = {1001: (101, None, "2004-01-01 00:00:00"), 1002: (101, 1001, "2005-06-15 12:00:00"),
                     1003: (101, 1002, "2010-03-03 03:03:03"), 1004: (101, 1003, "2011-01-01 00:00:00"),
                     1005: (101, 1004, "2012-01-01 00:00:00"), 1006: (101, 1005, "2013-01-01 00:00:00"),
                     2001: (102, None, "2006-02-02 00:00:00"), 3001: (103, None, "2007-07-07 07:07:07"),
                     4001: (104, None, "2020-05-05 05:05:05")}

pytestmark = pytest.mark.skipif(not os.environ.get("WCD_TEST_DATABASE_URL"),
                                reason="set WCD_TEST_DATABASE_URL to a scratch Postgres database")


@pytest.fixture(scope="module")
def db(tmp_path_factory):
    """A fresh schema with the tables created by ``wcd init-db``."""
    os.environ["DATABASE_URL"] = os.environ["WCD_TEST_DATABASE_URL"]
    engine = create_engine(database_url())
    with engine.begin() as con:
        con.execute(text("DROP SCHEMA public CASCADE"))
        con.execute(text("CREATE SCHEMA public"))
    init_db.main([])
    with engine.connect() as con:
        tables = {r[0] for r in con.execute(text(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"))}
    assert tables == set(Base.metadata.tables)
    yield engine
    engine.dispose()


@pytest.fixture(scope="module")
def work(tmp_path_factory) -> Path:
    return tmp_path_factory.mktemp("pg")


def q(engine, sql, **params):
    with engine.connect() as con:
        return con.execute(text(sql), params).fetchall()


def scalar(engine, sql, **params):
    return q(engine, sql, **params)[0][0]


# ---------------------------------------------------------------------------
# Stage 0
# ---------------------------------------------------------------------------

def test_stage0_metadata_fixture_matches_binary(work: Path):
    binary = os.environ.get("REVISIONCHEST_BIN")
    if not binary:
        pytest.skip("set REVISIONCHEST_BIN to regenerate the Stage-0 metadata from the mwdump fixture")
    out = work / "rc"
    out.mkdir()
    subprocess.run([binary, "build", str(MWDUMP_FIXTURE), "-o", str(out), "--parquet", str(out / "meta.parquet")],
                   check=True, capture_output=True)
    con = duckdb.connect()
    a, b = f"SELECT * FROM read_parquet('{STAGE0_META}')", f"SELECT * FROM read_parquet('{out / 'meta.parquet'}')"
    assert con.execute(f"SELECT count(*) FROM ({a} EXCEPT {b})").fetchone()[0] == 0
    assert con.execute(f"SELECT count(*) FROM ({b} EXCEPT {a})").fetchone()[0] == 0


def test_stage0_load(db, work: Path):
    parquet_base = work / "parquet"
    rc = stage0.main(["--domain", DOMAIN, "--metadata", str(STAGE0_META), "--parquet-dir", str(parquet_base)])
    assert rc == 0

    # domains / bundles
    assert q(db, "SELECT value FROM domains") == [(DOMAIN,)]
    assert q(db, "SELECT file_path FROM revision_bundles") == [("fixture.xml.mwrev.zst",)]

    # page_metadata and the article web_resources
    rows = q(db, "SELECT page_id, page_title, page_namespace FROM page_metadata ORDER BY page_id")
    assert {r[0]: (r[1], r[2]) for r in rows} == FIXTURE_PAGES
    urls = q(db, "SELECT numeric_page_id, url FROM web_resources ORDER BY numeric_page_id")
    assert urls == [(pid, f"https://{DOMAIN}/w/index.php?curid={pid}") for pid in sorted(FIXTURE_PAGES)]

    # revisions: every row, parent chain, real TIMESTAMP, bundle offsets
    rows = q(db, "SELECT revision_id, page_id, parent_revision_id, revision_timestamp::text, found_in_bundle, "
                 "offset_begin, length FROM revisions ORDER BY revision_id")
    assert {r[0]: (r[1], r[2], r[3]) for r in rows} == FIXTURE_REVISIONS
    assert all(r[4] == 1 and r[5] is not None and r[6] is not None for r in rows)
    assert scalar(db, "SELECT data_type FROM information_schema.columns WHERE table_name = 'revisions' "
                      "AND column_name = 'revision_timestamp'") == "timestamp without time zone"
    assert scalar(db, "SELECT count(*) FROM revisions WHERE domain_id <> (SELECT id FROM domains WHERE value = :d)",
                  d=DOMAIN) == 0

    # revisions Parquet tier: hive layout, bucket = page_id % 512, typed timestamp
    buckets = layout.list_buckets(parquet_base, "revisions", DOMAIN)
    assert buckets == sorted({pid % layout.PAGE_BUCKETS for pid in FIXTURE_PAGES})
    con = duckdb.connect()
    pq = con.execute(f"SELECT revision_id, page_id, parent_revision_id, CAST(revision_timestamp AS VARCHAR), page_bucket "
                     f"FROM read_parquet('{layout.partition_glob(parquet_base, 'revisions', DOMAIN)}', hive_partitioning=true) "
                     "ORDER BY revision_id").fetchall()
    assert {r[0]: (r[1], r[2], r[3]) for r in pq} == FIXTURE_REVISIONS
    assert all(r[4] == r[1] % layout.PAGE_BUCKETS for r in pq)
    types = {r[0]: r[1] for r in con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{layout.dataset_glob(parquet_base, 'revisions')}')").fetchall()}
    assert types["revision_timestamp"] == "TIMESTAMP"
    assert types["page_id"] == "INTEGER"

    # the gate agrees
    with db.connect() as con_pg:
        from sqlalchemy.orm import Session
        assert stage0.gate(Session(con_pg), DOMAIN, str(parquet_base)) == []


def test_stage0_load_is_idempotent(db, work: Path):
    before = {t: scalar(db, f"SELECT count(*) FROM {t}")
              for t in ("revisions", "page_metadata", "web_resources", "revision_bundles", "domains")}
    assert stage0.main(["--domain", DOMAIN, "--metadata", str(STAGE0_META), "--parquet-dir", str(work / "parquet")]) == 0
    after = {t: scalar(db, f"SELECT count(*) FROM {t}") for t in before}
    assert after == before
    # the Parquet partition was rewritten, not appended to
    con = duckdb.connect()
    n = con.execute(f"SELECT count(*) FROM read_parquet('{layout.dataset_glob(work / 'parquet', 'revisions')}')").fetchone()[0]
    assert n == len(FIXTURE_REVISIONS)


def test_stage0_gate_detects_drift(db, work: Path):
    from sqlalchemy.orm import Session
    with db.connect() as con:
        s = Session(con)
        assert stage0.gate(s, "xx.wikipedia.org", str(work / "parquet")) == ["domain 'xx.wikipedia.org' is not in domains"]
        failures = stage0.gate(s, DOMAIN, str(work / "nowhere"))
        assert any("no revisions Parquet" in f for f in failures)


# ---------------------------------------------------------------------------
# Stage 1 + 3 + load on the synthetic bundle, on top of the Stage-0 rows
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def staged(work: Path) -> Path:
    bundle = work / "synthetic.mwrev.zst"
    bundle.write_bytes(zstd.ZstdCompressor(level=3).compress(render()))
    staging = work / "staging"
    extract.main([str(bundle), "-o", str(staging / "synthetic"), "--domain", DOMAIN, "--parquet-dir", str(work / "parquet")])
    dedup.main(["-d", str(staging)])
    return staging


def test_load_synthetic(db, staged: Path, work: Path):
    load.main(["-d", str(staged)])
    con = duckdb.connect()
    n_inst = con.execute(f"SELECT count(*) FROM read_parquet('{staged / 'deduped' / 'citation_instances.parquet'}')").fetchone()[0]
    n_norm = con.execute(f"SELECT count(*) FROM read_parquet('{staged / 'deduped' / 'normalized_citations.parquet'}')").fetchone()[0]
    assert scalar(db, "SELECT count(*) FROM citation_instances") == n_inst
    assert scalar(db, "SELECT count(*) FROM normalized_citations") == n_norm

    # deterministic instance ids
    rows = q(db, "SELECT ci.id, d.value, ci.page_id, ci.raw_sha1 FROM citation_instances ci "
                 "JOIN domains d ON d.id = ci.domain_id")
    assert rows and all(r[0] == instance_id(r[1], r[2], r[3]) for r in rows)
    assert scalar(db, "SELECT count(DISTINCT id) FROM citation_instances") == n_inst

    # normalized_citations carry (domain, page) of first appearance; article web_resources were not duplicated
    assert scalar(db, "SELECT count(*) FROM normalized_citations WHERE appears_on_domain_id IS NULL "
                      "OR appears_on_page_id IS NULL") == 0
    assert scalar(db, "SELECT count(*) FROM web_resources WHERE numeric_page_id IS NOT NULL") == len(FIXTURE_PAGES)
    assert scalar(db, "SELECT count(*) FROM normalized_citation_web_resources") > 0
    assert scalar(db, "SELECT count(*) FROM template_data") > 0
    # Stage-0 rows untouched
    assert scalar(db, "SELECT count(*) FROM revisions") == len(FIXTURE_REVISIONS)
    # citation_type arrived from Stage 1 and the Stage-3 gate passes
    assert scalar(db, "SELECT count(*) FROM normalized_citations WHERE citation_type IS NULL") == 0
    assert set(r[0] for r in q(db, "SELECT DISTINCT citation_type FROM normalized_citations")) >= {"template", "ref_tag", "shorthand", "endnote"}
    from sqlalchemy.orm import Session
    with db.connect() as con_pg:
        assert gates.stage3(Session(con_pg), DOMAIN, str(work / "parquet"), sample_buckets=None) == []
        # a history id without an instance is caught
        con_pg.execute(text("DELETE FROM citation_instances WHERE id = (SELECT min(id) FROM citation_instances)"))
        failures = gates.stage3(Session(con_pg), DOMAIN, str(work / "parquet"), sample_buckets=None)
        assert any("have no citation_instances row" in f for f in failures), failures
        con_pg.rollback()


def test_load_is_idempotent(db, staged: Path):
    tables = ("citation_instances", "normalized_citations", "web_resources", "wiki_templates",
              "template_data", "normalized_citation_web_resources", "domains", "containers")
    before = {t: scalar(db, f"SELECT count(*) FROM {t}") for t in tables}
    load.main(["-d", str(staged)])
    assert {t: scalar(db, f"SELECT count(*) FROM {t}") for t in tables} == before
