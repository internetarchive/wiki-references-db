"""``stage0``: metadata written before RevisionChest recorded log events (no
``rcid`` or ``log_*`` columns) and after it (with ``log`` rows) are read
together, and ``load_log_events`` loads the log rows into ``log_events`` once.
Needs ``WCD_TEST_DATABASE_URL`` (its ``public`` schema is dropped)."""
from __future__ import annotations

import os

import duckdb
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from wcd_pipeline import init_db, stage0
from wcd_pipeline.config import database_url

pytestmark = pytest.mark.skipif(not os.environ.get("WCD_TEST_DATABASE_URL"),
                                reason="set WCD_TEST_DATABASE_URL to a scratch Postgres database")

DOMAIN = "xx.wikipedia.org"


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


def _metadata(dirpath):
    con = duckdb.connect()
    # A file in the layout before log events: ten columns.
    con.execute("""COPY (SELECT 'page' AS type, 7::UBIGINT AS page_id, 0::BIGINT AS ns, 'P7' AS title,
                          NULL::UBIGINT AS rev_id, NULL::UBIGINT AS parent_rev_id, NULL AS file_path,
                          NULL::UBIGINT AS offset_begin, NULL::UBIGINT AS length, NULL AS timestamp)
                   TO '{}' (FORMAT PARQUET)""".format(dirpath / "old.parquet"))
    # A file from a sync that saw a revision being hidden and a page deleted.
    con.execute("""COPY (SELECT * FROM (VALUES
                     ('log', 7::UBIGINT, 0::BIGINT, 'P7', NULL::UBIGINT, NULL::UBIGINT, NULL, NULL::UBIGINT,
                      NULL::UBIGINT, '2026-10-19T00:00:20Z', 2::UBIGINT, 1002::UBIGINT, 'delete', 'revision',
                      '{"ids":[101],"type":"revision"}'),
                     ('log', NULL, 1, 'Talk:P8', NULL, NULL, NULL, NULL, NULL, '2026-10-19T00:00:30Z',
                      3, NULL, 'delete', 'delete', NULL))
                   AS t(type, page_id, ns, title, rev_id, parent_rev_id, file_path, offset_begin, length,
                        timestamp, rcid, log_id, log_type, log_action, log_params))
                   TO '{}' (FORMAT PARQUET)""".format(dirpath / "new.parquet"))


def test_old_and_new_metadata_read_together_and_log_rows_load_once(engine, tmp_path):
    _metadata(tmp_path)
    assert stage0.metadata_counts(str(tmp_path)) == {"page": 1, "log": 2}
    session = sessionmaker(bind=engine)()
    try:
        domain_id = stage0.ensure_domain(session, DOMAIN)
        assert stage0.load_log_events(session, str(tmp_path), domain_id) == 2
        stage0.load_log_events(session, str(tmp_path), domain_id)  # again: nothing new
    finally:
        session.close()
    with engine.connect() as con:
        rows = con.execute(text(
            "SELECT rcid, log_id, log_type, log_action, page_id, page_namespace, page_title, "
            "log_timestamp::text, log_params->'ids'->>0 FROM log_events ORDER BY rcid")).fetchall()
    assert rows == [
        (2, 1002, "delete", "revision", 7, 0, "P7", "2026-10-19 00:00:20", "101"),
        (3, None, "delete", "delete", None, 1, "Talk:P8", "2026-10-19 00:00:30", None),
    ]


def test_metadata_without_log_rows_loads_none(engine, tmp_path):
    con = duckdb.connect()
    con.execute("COPY (SELECT 'page' AS type, 1::UBIGINT AS page_id) TO '{}' (FORMAT PARQUET)"
                .format(tmp_path / "m.parquet"))
    session = sessionmaker(bind=engine)()
    try:
        assert stage0.load_log_events(session, str(tmp_path), stage0.ensure_domain(session, DOMAIN)) == 0
    finally:
        session.close()
