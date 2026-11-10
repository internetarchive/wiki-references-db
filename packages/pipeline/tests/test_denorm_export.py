"""The denorm export (COPY → CSV → DuckDB → Parquet) keeps every value exact:
NULL vs empty string, quotes, commas, newlines, backslashes, booleans.

Needs a scratch database; skipped unless ``WCD_TEST_DATABASE_URL`` is set
(the schema ``public`` of that database is dropped and recreated).
"""
from __future__ import annotations

import os

import duckdb
import pytest
from sqlalchemy import create_engine, text

from wcd_pipeline import init_db, materialize
from wcd_pipeline.config import database_url, psycopg_dsn

pytestmark = pytest.mark.skipif(not os.environ.get("WCD_TEST_DATABASE_URL"),
                                reason="set WCD_TEST_DATABASE_URL to a scratch Postgres database")

# the last but one makes a CSV row over DuckDB's default 2 MB line limit (enwiki has a 2.8 MB row)
AWKWARD = ['plain', '', 'quote " inside', 'comma, and\nnewline', 'back\\slash', 'ünïcödé — ✓', 'NULL', '\\N',
           'http://en.wikipedia.org/wiki/x' * 50_000, None]


def test_export_round_trips_values(tmp_path):
    os.environ["DATABASE_URL"] = os.environ["WCD_TEST_DATABASE_URL"]
    engine = create_engine(database_url())
    with engine.begin() as con:
        con.execute(text("DROP SCHEMA public CASCADE"))
        con.execute(text("CREATE SCHEMA public"))
    init_db.main([])
    with engine.begin() as con:
        con.execute(text("INSERT INTO domains (id, value) VALUES (1, 'en.wikipedia.org')"))
        for i, v in enumerate(AWKWARD, 1):
            con.execute(text(
                "INSERT INTO normalized_citations (id, normalized_sha1, reference_normalized, appears_on_domain_id, "
                "appears_on_page_id, citation_type) VALUES (:i, lpad(CAST(:i AS text), 40, '0'), 'r', 1, 7, 'ref_tag')"), {"i": i})
            con.execute(text(
                "INSERT INTO citation_instances (id, domain_id, page_id, normalized_id, raw_sha1, reference_type, reference_name) "
                "VALUES (:i, 1, 7, :i, lpad(CAST(:i AS text), 40, '1'), 1, :v)"), {"i": i, "v": v})
            con.execute(text(
                "INSERT INTO normalized_citation_parts (normalized_id, part_index, part_sha1, content, part_type, "
                "norm_content, title, year_reliable, is_archive_url) "
                "VALUES (:i, 0, lpad(CAST(:i AS text), 40, '2'), :c, 'ref_tag', :v, :v, :b, :nb)"),
                {"i": i, "v": v, "c": v if v is not None else "x", "b": i % 2 == 0, "nb": None if i % 3 else True})
    base = tmp_path / "pq"
    counts = materialize.export_citation_instances_denorm(psycopg_dsn(), "en.wikipedia.org", str(base))
    assert counts["rows"] == len(AWKWARD)
    rows = duckdb.sql(f"SELECT citation_instance_id, reference_name, norm_content, title, content, year_reliable, "
                      f"is_archive_url, global_source_id FROM read_parquet('{base}/**/*.parquet', hive_partitioning = true) "
                      f"ORDER BY citation_instance_id").fetchall()
    for (i, name, norm, title, content, yr, arch, gsid), v in zip(rows, AWKWARD):
        assert (name, norm, title) == (v, v, v), i
        assert content == (v if v is not None else "x")
        assert yr is (i % 2 == 0) and arch is (None if i % 3 else True) and gsid is None
    engine.dispose()
