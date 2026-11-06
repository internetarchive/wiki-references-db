"""``stage0.load_pages``: titles survive COPY escaping, the article
``web_resources`` rows carry ``url_hash = md5(url)`` exactly as
``WebResource.compute_url_hash`` computes it, and loading again (or a page
listed twice) changes nothing. Needs ``WCD_TEST_DATABASE_URL`` (its ``public``
schema is dropped)."""
from __future__ import annotations

import os

import duckdb
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from wcd_pipeline import init_db, stage0
from wcd_pipeline.config import database_url
from wcd_schema.postgres import WebResource

pytestmark = pytest.mark.skipif(not os.environ.get("WCD_TEST_DATABASE_URL"),
                                reason="set WCD_TEST_DATABASE_URL to a scratch Postgres database")

DOMAIN = "xx.wikipedia.org"
PAGES = [  # (page_id, ns, title)
    (1, 0, "Plain"),
    (2, 0, r"Back\slash \N and \\ two"),
    (3, 4, "Wikipedia:Tab\there, newline\nthere, cr\rhere"),
    (4, 0, "Ünïcødé — 日本語"),
    (5, 0, None),
]


@pytest.fixture(scope="module")
def engine():
    os.environ["DATABASE_URL"] = os.environ["WCD_TEST_DATABASE_URL"]
    eng = create_engine(database_url())
    with eng.begin() as con:
        con.execute(text("DROP SCHEMA public CASCADE"))
        con.execute(text("CREATE SCHEMA public"))
    init_db.main([])
    yield eng
    eng.dispose()


def _metadata(path, pages):
    con = duckdb.connect()
    con.execute("CREATE TABLE m (type VARCHAR, page_id UBIGINT, ns BIGINT, title VARCHAR)")
    con.executemany("INSERT INTO m VALUES ('page', ?, ?, ?)", pages)
    con.execute(f"COPY m TO '{path}' (FORMAT PARQUET)")


def _load(engine, path):
    session = sessionmaker(bind=engine)()
    try:
        domain_id = stage0.ensure_domain(session, DOMAIN)
        return domain_id, stage0.load_pages(session, str(path), DOMAIN, domain_id)
    finally:
        session.close()


def _rows(engine):
    with engine.connect() as con:
        pm = con.execute(text("SELECT page_id, page_namespace, page_title FROM page_metadata ORDER BY page_id")).fetchall()
        wr = con.execute(text("SELECT numeric_page_id, numeric_namespace_id, url, url_hash, domain_id "
                              "FROM web_resources ORDER BY numeric_page_id")).fetchall()
    return pm, wr


def test_titles_hashes_and_idempotence(engine, tmp_path):
    meta = tmp_path / "pages.parquet"
    _metadata(meta, PAGES)
    domain_id, n = _load(engine, meta)
    assert n == len(PAGES)
    pm, wr = _rows(engine)
    assert pm == [(pid, ns, title) for pid, ns, title in PAGES]
    expected_urls = [f"https://{DOMAIN}/w/index.php?curid={pid}" for pid, _, _ in PAGES]
    assert [(r[0], r[1], r[2], r[4]) for r in wr] == [
        (pid, ns, url, domain_id) for (pid, ns, _), url in zip(PAGES, expected_urls)]
    assert [r[3] for r in wr] == [WebResource.compute_url_hash(u) for u in expected_urls]

    # again, plus a page listed twice in one file: same rows, nothing duplicated
    twice = tmp_path / "twice.parquet"
    _metadata(twice, PAGES + [PAGES[1]])
    _load(engine, twice)
    assert _rows(engine) == (pm, wr)


def test_revisions_load_in_parallel_units_and_rerun_is_a_no_op(engine, tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(stage0, "REVISION_GROUP", 2048)
    con = duckdb.connect()
    con.execute("""CREATE TABLE m (type VARCHAR, page_id UBIGINT, ns BIGINT, title VARCHAR, rev_id UBIGINT,
                   parent_rev_id UBIGINT, file_path VARCHAR, offset_begin UBIGINT, length UBIGINT, timestamp VARCHAR)""")
    rows = [(r % 5 + 1, r, (r - 1) if r % 7 else None, f"x{r % 2}.mwrev.zst", r * 100, r % 50,
             f"2020-01-{r % 28 + 1:02d}T01:02:03Z") for r in range(1, 10_001)]
    con.executemany("INSERT INTO m VALUES ('revision', ?, 0, NULL, ?, ?, ?, ?, ?, ?)", rows)
    meta = tmp_path / "revs.parquet"
    con.execute(f"COPY m TO '{meta}' (FORMAT PARQUET)")

    session = sessionmaker(bind=engine)()
    try:
        domain_id = stage0.ensure_domain(session, DOMAIN)
        bundles = stage0.load_bundles(session, str(meta))
        for attempt in range(2):  # the second run must change nothing
            n = stage0.load_revisions(session, str(meta), domain_id, bundles, jobs=4, work_dir=str(tmp_path))
            assert n == len(rows)
            # the primary key is rebuilt after the load into the empty table only
            assert ("rebuilt revisions_pkey" in capsys.readouterr().out) == (attempt == 0)
            with engine.connect() as c:
                got = c.execute(text(
                    "SELECT r.revision_id, r.page_id, r.parent_revision_id, b.file_path, r.offset_begin, r.length, "
                    "to_char(r.revision_timestamp, 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') FROM revisions r "
                    "JOIN revision_bundles b ON b.id = r.found_in_bundle WHERE r.domain_id = :d ORDER BY 1"),
                    {"d": domain_id}).fetchall()
            assert [(r[1], r[0], r[2], r[3], r[4], r[5], r[6]) for r in got] == rows
        assert not list(tmp_path.glob("stage0-revisions-*"))  # the work directory is cleaned up
    finally:
        session.close()


def test_gate_reports_revisions_whose_page_is_missing(engine):
    from wcd_pipeline import gates
    other = "yy.wikipedia.org"
    session = sessionmaker(bind=engine)()
    try:
        d = stage0.ensure_domain(session, other)
        session.execute(text("INSERT INTO page_metadata (domain_id, page_id, page_title, page_namespace) "
                             "VALUES (:d, 7, 'Seven', 0)"), {"d": d})
        session.execute(text("INSERT INTO revisions (revision_id, domain_id, page_id, revision_timestamp) VALUES "
                             "(9000001, :d, 7, '2020-01-01'), (9000002, :d, 7, '2020-01-02')"), {"d": d})
        session.commit()
        assert gates.stage0(session, other, None) == []
        # two revisions of one page that has no page_metadata row: one page reported
        session.execute(text("INSERT INTO revisions (revision_id, domain_id, page_id, revision_timestamp) VALUES "
                             "(9000003, :d, 8, '2020-01-03'), (9000004, :d, 8, '2020-01-04')"), {"d": d})
        session.commit()
        assert gates.stage0(session, other, None) == ["1 page(s) in revisions have no page_metadata row"]
    finally:
        session.close()
