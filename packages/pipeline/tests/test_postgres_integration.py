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
import shutil
import subprocess
from pathlib import Path

import duckdb
import pytest
import zstandard as zstd
from sqlalchemy import create_engine, text

from wcd_pipeline import dedup, extract, gates, init_db, load, materialize, stage0, stage4, stage5, stage6
from wcd_pipeline.config import database_url
from wcd_schema import parquet as layout
from wcd_schema.ids import instance_id
from wcd_schema.postgres import Base

from synthetic_bundle import PAGE_A, PAGE_B, render, write_metadata  # noqa: E402

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
def staged(db, work: Path) -> Path:
    # Stage 0 for the synthetic bundle's pages (page 101 and revisions 1001-1003 overlap the
    # mwdump fixture, page 202 is new): the metadata *directory* now holds both files, and the
    # domain's revisions Parquet is rebuilt from all of it, each revision once.
    meta_dir = work / "meta"
    meta_dir.mkdir()
    shutil.copy(STAGE0_META, meta_dir / "fixture.parquet")
    write_metadata(str(meta_dir / "synthetic.parquet"))
    assert stage0.main(["--domain", DOMAIN, "--metadata", str(meta_dir), "--parquet-dir", str(work / "parquet")]) == 0
    assert scalar(db, "SELECT count(*) FROM revisions") == len(FIXTURE_REVISIONS) + 2   # 2201, 2202
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
    assert scalar(db, "SELECT count(*) FROM web_resources WHERE numeric_page_id IS NOT NULL") == len(FIXTURE_PAGES) + 1  # + page 202
    assert scalar(db, "SELECT count(*) FROM normalized_citation_web_resources") > 0
    assert scalar(db, "SELECT count(*) FROM template_data") > 0
    # Stage-0 rows untouched
    assert scalar(db, "SELECT count(*) FROM revisions") == len(FIXTURE_REVISIONS) + 2
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


# ---------------------------------------------------------------------------
# Stage 4 (parts) and Stage 5 (ref_name_links)
# ---------------------------------------------------------------------------

def test_stage4_parts(db, staged: Path):
    dsn = os.environ["WCD_TEST_DATABASE_URL"]
    counts = stage4.run(dsn, DOMAIN)
    assert counts["citations"] == scalar(db, "SELECT count(*) FROM normalized_citations")
    rows = q(db, "SELECT nc.citation_type, p.part_index, p.part_type, p.norm_subtype, p.template_name, p.author_key, "
                 "p.year, p.extracted_isbn, p.extracted_url, p.content, p.part_sha1 "
                 "FROM normalized_citation_parts p JOIN normalized_citations nc ON nc.id = p.normalized_id")
    by_content = {r[9]: r for r in rows}
    # bullet block of two {{cite journal}} inside one <ref> → two template parts
    journal = sorted(r for r in rows if r[4] == "cite journal")
    assert [(r[1], r[5], r[6]) for r in journal] == [(0, "lee", "2001"), (1, "lee", "2002")]
    # {{cite book}} with ISBN: template part, normalized identifier
    books = [r for r in rows if r[4] == "cite book" and r[5] == "diamond"]
    assert len(books) == 3 and all(r[7] == "9780143036555" for r in books)
    # prose ref → ref_tag part through the ref-tag normalizer
    fischer = next(r for r in rows if r[5] == "fischer")
    assert fischer[:4] == ("ref_tag", 0, "ref_tag", "prose_citation") and fischer[6] == "1995"
    # standalone {{sfn}} → shorthand_template subtype with anchor author/year
    sfn = next(r for r in rows if r[0] == "shorthand")
    assert (sfn[3], sfn[5], sfn[6]) == ("shorthand_template", "routledge", "1919")
    # bare-url <ref> and bracketed endnote carry extracted_url; wrapper stripped from content
    assert by_content["[https://example.org/visits Visits]"][8] == "https://example.org/visits"
    assert by_content["[https://example.org/island Official site]"][0] == "endnote"
    assert not any(c.startswith("<ref") or c.startswith("* ") for c in by_content)
    # part_sha1 = sha1(content) everywhere, and the gate agrees
    from wcd_schema.ids import part_sha1
    assert all(part_sha1(r[9]) == r[10] for r in rows)
    from sqlalchemy.orm import Session
    with db.connect() as con_pg:
        assert gates.stage4(Session(con_pg), DOMAIN) == []
    # idempotent
    before = sorted(rows)
    stage4.run(dsn, DOMAIN)
    assert sorted(q(db, "SELECT nc.citation_type, p.part_index, p.part_type, p.norm_subtype, p.template_name, p.author_key, "
                       "p.year, p.extracted_isbn, p.extracted_url, p.content, p.part_sha1 "
                       "FROM normalized_citation_parts p JOIN normalized_citations nc ON nc.id = p.normalized_id")) == before
    # --resume touches nothing when every citation has parts
    assert stage4.run(dsn, DOMAIN, resume=True)["citations"] == 0


def test_stage4_gate_detects_missing_parts(db, staged: Path):
    from sqlalchemy.orm import Session
    with db.connect() as con_pg:
        con_pg.execute(text("DELETE FROM normalized_citation_parts WHERE normalized_id = (SELECT min(id) FROM normalized_citations)"))
        failures = gates.stage4(Session(con_pg), DOMAIN)
        assert any("have no part" in f for f in failures), failures
        con_pg.rollback()


def test_stage4_denorm_export(db, staged: Path, work: Path):
    dsn = os.environ["WCD_TEST_DATABASE_URL"]
    counts = materialize.export_citation_instances_denorm(dsn, DOMAIN, str(work / "parquet"))
    n_join = scalar(db, "SELECT count(*) FROM citation_instances ci JOIN normalized_citation_parts p "
                        "ON p.normalized_id = ci.normalized_id")
    assert counts["rows"] == n_join == 12   # 11 instances, one of which (the bullet block) has 2 parts
    con = duckdb.connect()
    glob_ = layout.dataset_glob(work / "parquet", "citation_instances_denorm")
    cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{glob_}', hive_partitioning=true)").fetchall()]
    assert cols == [f.name for f in layout.SCHEMAS["citation_instances_denorm"]] + ["domain", "page_bucket"]
    rows = con.execute(f"SELECT citation_instance_id, page_id, part_index, citation_type, part_type, global_source_id, page_bucket "
                       f"FROM read_parquet('{glob_}', hive_partitioning=true) ORDER BY page_id, citation_instance_id, part_index").fetchall()
    assert all(r[5] is None and r[6] == r[1] % 512 for r in rows)
    assert sum(1 for r in rows if r[2] == 1) == 1
    assert layout.list_buckets(work / "parquet", "citation_instances_denorm", DOMAIN) == [PAGE_A % 512, PAGE_B % 512]
    assert not (layout.dataset_dir(work / "parquet", "citation_instances_denorm") / f"_scratch_{DOMAIN}").exists()
    # a second export replaces, not appends
    materialize.export_citation_instances_denorm(dsn, DOMAIN, str(work / "parquet"))
    assert con.execute(f"SELECT count(*) FROM read_parquet('{glob_}')").fetchone()[0] == 12


def test_stage5_ref_name_links(db, staged: Path, work: Path):
    dsn = os.environ["WCD_TEST_DATABASE_URL"]
    counts = stage5.run(dsn, DOMAIN, str(work / "parquet"))
    assert counts["links"] == 3
    rows = q(db, "SELECT r.page_id, r.ref_name, r.first_revision_id, r.last_revision_id, "
                 "nc.reference_normalized LIKE '%edition=2nd%' FROM ref_name_links r "
                 "JOIN citation_instances ci ON ci.id = r.defining_instance_id "
                 "JOIN normalized_citations nc ON nc.id = ci.normalized_id ORDER BY 1, 2, 3")
    # "diamond": first edition in 1001–1002, redefined (2nd edition) in 1003; page 101 has
    # later revisions (1004–1006, from the Stage-0 fixture) without it, so the range closes.
    # "web1": defined in 2201 and still present in the page's latest revision 2202 → open range.
    assert rows == [(PAGE_A, "diamond", 1001, 1002, False), (PAGE_A, "diamond", 1003, 1003, True),
                    (PAGE_B, "web1", 2201, None, False)]
    # the Parquet copy matches and the gate passes; a re-run is idempotent
    con = duckdb.connect()
    pq_rows = con.execute(f"SELECT page_id, ref_name, first_revision_id, last_revision_id FROM read_parquet("
                          f"'{layout.partition_glob(work / 'parquet', 'ref_name_links', DOMAIN)}', hive_partitioning=true) "
                          "ORDER BY 1, 2, 3").fetchall()
    assert pq_rows == [(r[0], r[1], r[2], r[3]) for r in rows]
    from sqlalchemy.orm import Session
    with db.connect() as con_pg:
        assert gates.stage5(Session(con_pg), DOMAIN, str(work / "parquet")) == []
    assert stage5.run(dsn, DOMAIN, str(work / "parquet"))["links"] == 3
    assert scalar(db, "SELECT count(*) FROM ref_name_links") == 3


def test_stage5_gate_detects_overlap(db, staged: Path):
    from sqlalchemy.orm import Session
    with db.connect() as con_pg:
        con_pg.execute(text("UPDATE ref_name_links SET last_revision_id = NULL WHERE first_revision_id = 1001"))
        failures = gates.stage5(Session(con_pg), DOMAIN)
        assert any("overlapping" in f for f in failures), failures
        con_pg.rollback()


# ---------------------------------------------------------------------------
# Stage 6 (matching) on the Stage-4.6 export
# ---------------------------------------------------------------------------

def _read(con, work: Path, dataset: str, order: str):
    pattern = layout.partition_glob(work / "parquet", "matching_intermediate/" + dataset, DOMAIN)
    return con.execute(f"SELECT * FROM read_parquet('{pattern}', hive_partitioning=true) ORDER BY {order}").df()


def test_stage6_matching(db, staged: Path, work: Path):
    parquet = str(work / "parquet")
    # (denorm export ran in test_stage4_denorm_export; ref_name_links in test_stage5)
    assert stage6.main(["--domain", DOMAIN, "--parquet-dir", parquet, "--jobs", "1"]) == 0
    con = duckdb.connect()
    sources = _read(con, work, "sources", "page_id, source_id")
    tsources = _read(con, work, "template_sources", "page_id, source_id")
    merged = _read(con, work, "merged_sources", "page_id, merged_source_id")
    mmap = _read(con, work, "merged_citation_source_map", "page_id, part_sha1")
    gmap = _read(con, work, "global_source_map", "merged_source_id")
    gstage = _read(con, work, "global_sources_stage", "global_source_id")

    # 6b: the two editions of the Diamond book on page 101 share an ISBN → one template source of 2
    diamond_101 = tsources[(tsources.page_id == PAGE_A) & (tsources.extracted_isbn == "9780143036555")]
    assert len(diamond_101) == 1 and diamond_101.iloc[0].n_citations == 2 and diamond_101.iloc[0].match_type == "isbn"
    # 6a: the ref-tag parts of page 101 are all singletons (prose, sfn, bare urls)
    assert set(sources[sources.page_id == PAGE_A].match_type) == {"singleton"}
    # 6c: {{sfn|Routledge|1919}} merges with the Routledge {{cite book}} endnote on author+year
    both = merged[(merged.page_id == PAGE_A) & (merged.source_type == "both")]
    assert len(both) == 1 and both.iloc[0].author_key == "routledge" and both.iloc[0].year == "1919"
    assert both.iloc[0].match_type == "author_year" and both.iloc[0].n_total_citations == 2
    # every part of the bucket is mapped to exactly one merged source, with the per-page match values
    n_parts = con.execute(f"SELECT count(DISTINCT (page_id, part_sha1)) FROM read_parquet("
                          f"'{layout.partition_glob(work / 'parquet', 'citation_instances_denorm', DOMAIN)}', hive_partitioning=true)").fetchone()[0]
    assert len(mmap) == n_parts and mmap.duplicated(["page_id", "part_sha1"]).sum() == 0
    assert set(mmap.source_type) == {"ref_tag", "template"}
    assert mmap[mmap.merged_source_id == both.iloc[0].merged_source_id].shape[0] == 2
    # 6d: the Diamond book cited on both pages collapses to one global source (isbn), n_pages 2
    g_isbn = gstage[gstage.extracted_isbn == "9780143036555"]
    assert len(g_isbn) == 1 and g_isbn.iloc[0].n_pages == 2 and g_isbn.iloc[0].match_type == "isbn"
    assert g_isbn.iloc[0].source_type == "template" and g_isbn.iloc[0].n_total_citations == 3
    assert set(gmap[gmap.global_source_id == g_isbn.iloc[0].global_source_id].page_id) == {PAGE_A, PAGE_B}
    # every merged source has a map row; singletons carry no confidence
    assert set(gmap.merged_source_id) == set(merged.merged_source_id)
    assert gstage[gstage.match_type == "singleton"].confidence.isna().all()
    assert stage6.gate(parquet, DOMAIN) == []
    # re-run: markers skip done buckets; --force rebuilds identically
    before = merged.copy()
    assert stage6.main(["--domain", DOMAIN, "--parquet-dir", parquet, "--jobs", "1", "--force"]) == 0
    after = _read(con, work, "merged_sources", "page_id, merged_source_id")
    assert after.equals(before)
