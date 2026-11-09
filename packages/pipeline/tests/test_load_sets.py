"""The set-based (COPY + INSERT … SELECT) loaders of Stage 3 keep the rules of
the row-by-row loaders they replaced: lookups by natural key, the same skips,
upserts onto Stage-0 rows, re-runs that change nothing, and the
citation_instance_id check.

Needs a scratch database; skipped unless ``WCD_TEST_DATABASE_URL`` is set
(the schema ``public`` of that database is dropped and recreated).
"""
from __future__ import annotations

import hashlib
import os

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from sqlalchemy import create_engine, text

from wcd_pipeline import init_db, load
from wcd_pipeline.config import database_url
from wcd_schema.ids import instance_id

pytestmark = pytest.mark.skipif(not os.environ.get("WCD_TEST_DATABASE_URL"),
                                reason="set WCD_TEST_DATABASE_URL to a scratch Postgres database")

EN = "en.wikipedia.org"
SHA_A, SHA_B, SHA_C, SHA_D = ("a" * 40, "b" * 40, "c" * 40, "d" * 40)
RAW_1, RAW_2, RAW_3 = ("1" * 40, "2" * 40, "3" * 40)
ARTICLE_URL = "https://en.wikipedia.org/wiki?curid=7"
TABLES = ["domains", "web_resources", "normalized_citations", "citation_instances", "ncwr"]


@pytest.fixture()
def engine():
    os.environ["DATABASE_URL"] = os.environ["WCD_TEST_DATABASE_URL"]
    engine = create_engine(database_url())
    with engine.begin() as con:
        con.execute(text("DROP SCHEMA public CASCADE"))
        con.execute(text("CREATE SCHEMA public"))
    init_db.main([])
    yield engine
    engine.dispose()


def _write(staging, row_group_size, name, columns):
    deduped = staging / "deduped"
    deduped.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table(columns), deduped / f"{name}.parquet", row_group_size=row_group_size)


def _stage(staging, citation_instance_ids=None, row_group_size=None):
    rg = row_group_size
    _write(staging, rg, "domains", {"value": [EN, "example.com"], "for_container_label": pa.array([None, None], pa.string())})
    _write(staging, rg, "web_resources", {
        "url": ["https://example.com/a", "https://unknown.example/b", ARTICLE_URL, "https://ex.com/ü?q=\"x\",y"],
        "domain_label": ["example.com", "not-a-domain.example", EN, None]})
    _write(staging, rg, "normalized_citations", {
        "normalized_sha1": [SHA_A, SHA_B, SHA_C, SHA_D],
        "reference_normalized": ["ref a", "ref b, with \"quotes\"\nand a newline", "ref c", "ref d"],
        "appears_on_page_id": pa.array([7, 7, None, 7], pa.int64()),
        "appears_on_domain": [EN, EN, EN, "not-a-domain.example"],
        "citation_type": ["template", "ref_tag", "template", "template"]})
    keys = [(EN, 7, RAW_1), (EN, 7, RAW_2), (EN, 7, RAW_3)]
    ids = citation_instance_ids or [instance_id(*k) for k in keys]
    _write(staging, rg, "citation_instances", {
        "citation_instance_id": pa.array(ids, pa.int64()),
        "domain": [k[0] for k in keys], "page_id": pa.array([k[1] for k in keys], pa.int64()),
        "raw_sha1": [k[2] for k in keys],
        # SHA_C was skipped (no page id), so the third instance has no citation to point at
        "normalized_sha1": [SHA_A, SHA_B, SHA_C],
        "reference_type": pa.array([1, None, 1], pa.int8()),
        "reference_name": pa.array(["n1", None, ""], pa.string())})
    _write(staging, rg, "ncwr", {
        "normalized_sha1": [SHA_A, SHA_B, SHA_A, SHA_C],
        "url": ["https://example.com/a", "https://ex.com/ü?q=\"x\",y", "https://never-staged.example/", "https://example.com/a"]})


def _article_row(engine):
    """A Stage-0 article row that a staged cited URL collides with."""
    with engine.begin() as con:
        con.execute(text("INSERT INTO domains (value) VALUES (:d)"), {"d": EN})
        con.execute(text(
            "INSERT INTO web_resources (url, url_hash, domain_id, numeric_page_id, numeric_namespace_id) "
            "SELECT :u, :h, id, 7, 0 FROM domains WHERE value = :d"),
            {"u": ARTICLE_URL, "h": hashlib.md5(ARTICLE_URL.encode()).hexdigest(), "d": EN})


def _snapshot(engine):
    with engine.connect() as con:
        # sorted in Python: the database collation may order URLs differently
        q = lambda sql: sorted(tuple(r) for r in con.execute(text(sql)))  # noqa: E731
        return {
            "web_resources": q("SELECT w.url, w.url_hash, d.value, w.numeric_page_id, w.numeric_namespace_id "
                               "FROM web_resources w LEFT JOIN domains d ON d.id = w.domain_id ORDER BY w.url"),
            "normalized_citations": q("SELECT n.normalized_sha1, n.reference_normalized, d.value, n.appears_on_page_id, "
                                      "n.citation_type FROM normalized_citations n "
                                      "JOIN domains d ON d.id = n.appears_on_domain_id ORDER BY 1"),
            "citation_instances": q("SELECT c.id, d.value, c.page_id, c.raw_sha1, n.normalized_sha1, c.reference_type, "
                                    "c.reference_name FROM citation_instances c JOIN domains d ON d.id = c.domain_id "
                                    "JOIN normalized_citations n ON n.id = c.normalized_id ORDER BY c.raw_sha1"),
            "ncwr": q("SELECT n.normalized_sha1, w.url FROM normalized_citation_web_resources x "
                      "JOIN normalized_citations n ON n.id = x.normalized_id "
                      "JOIN web_resources w ON w.id = x.web_resource_id ORDER BY 1, 2"),
        }


@pytest.mark.parametrize("row_group_size,jobs", [(None, 1), (1, 1), (1, 4)])
def test_set_based_loaders(engine, tmp_path, row_group_size, jobs):
    """One row group, or one row per row group (one unit each) on 1 or 4 connections."""
    _article_row(engine)
    _stage(tmp_path, row_group_size=row_group_size)
    args = ["-d", str(tmp_path), "--jobs", str(jobs), "--tables", *TABLES]
    load.main(args)
    got = _snapshot(engine)

    def md5(u):
        return hashlib.md5(u.encode()).hexdigest()

    assert got["web_resources"] == [
        ("https://en.wikipedia.org/wiki?curid=7", md5(ARTICLE_URL), EN, 7, 0),   # Stage-0 columns kept
        ("https://ex.com/ü?q=\"x\",y", md5("https://ex.com/ü?q=\"x\",y"), None, None, None),
        ("https://example.com/a", md5("https://example.com/a"), "example.com", None, None),
        ("https://unknown.example/b", md5("https://unknown.example/b"), None, None, None),  # unknown domain: no id
    ]
    # SHA_C has no page id, SHA_D an unknown domain: both skipped
    assert got["normalized_citations"] == [
        (SHA_A, "ref a", EN, 7, "template"),
        (SHA_B, "ref b, with \"quotes\"\nand a newline", EN, 7, "ref_tag"),
    ]
    assert got["citation_instances"] == sorted([
        (instance_id(EN, 7, RAW_1), EN, 7, RAW_1, SHA_A, 1, "n1"),
        (instance_id(EN, 7, RAW_2), EN, 7, RAW_2, SHA_B, 0, None),   # NULL reference_type → 0
    ])
    assert got["ncwr"] == [(SHA_A, "https://example.com/a"), (SHA_B, "https://ex.com/ü?q=\"x\",y")]

    # a re-run changes nothing
    load.main(args)
    assert _snapshot(engine) == got


def test_citation_instance_id_mismatch_fails(engine, tmp_path):
    _stage(tmp_path, citation_instance_ids=[instance_id(EN, 7, RAW_1), 12345, None])
    load.main(["-d", str(tmp_path), "--tables", "domains", "normalized_citations"])
    with pytest.raises(Exception, match="citation_instance_id mismatch"):
        load.main(["-d", str(tmp_path), "--tables", "citation_instances"])
    with engine.connect() as con:
        assert con.execute(text("SELECT count(*) FROM citation_instances")).scalar() == 0


def test_sorted_copy_orders_row_groups(tmp_path, monkeypatch):
    """Units are the sorted copy's row groups, so they must come out in key order
    (``_duck`` turns preserve_insertion_order off, which ORDER BY must override)."""
    n = 20000
    keys = [hashlib.sha1(str(i).encode()).hexdigest() for i in range(n)]
    src = tmp_path / "t.parquet"
    pq.write_table(pa.table({"k": keys, "v": list(range(n))}), src, row_group_size=997)
    # DuckDB rounds row groups to whole vectors (2,048 rows)
    monkeypatch.setattr(load, "SORTED_GROUP", 4096)
    out = load._sorted_copy(str(src), "k", str(tmp_path / "work"))
    f = pq.ParquetFile(out)
    assert f.metadata.num_row_groups >= 4
    got = [k for i in range(f.metadata.num_row_groups) for k in f.read_row_group(i).column("k").to_pylist()]
    assert got == sorted(keys)


def test_loaders_leave_no_sorted_copies(engine, tmp_path):
    _stage(tmp_path, row_group_size=1)
    load.main(["-d", str(tmp_path), "--jobs", "2", "--tables", *TABLES])
    assert not any((tmp_path / ".load-sort").rglob("*"))
