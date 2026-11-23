"""`dedup --previous` + `load --changes` give what a dedup and load of everything gives.

Staging set A is loaded in full; set B is then deduped against A's deduped set and only its
change set loaded. B changes merged values of rows A already loaded (a smaller template
parameter value and a citation_type that changes the parts of a citation of A, a smaller reference_name for an
instance of A), adds an instance of an old citation on a new page, and adds new citations.
Every Postgres table load writes, the parts (`parts --resume --redo-from` the change set) and
both template datasets must equal the result of one dedup, load and parts of A and B together.

Needs a scratch database; skipped unless ``WCD_TEST_DATABASE_URL`` is set (its schema
``public`` is dropped and recreated).
"""
from __future__ import annotations

import os
import shutil

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from sqlalchemy import create_engine, text

from sqlalchemy.orm import Session

from wcd_pipeline import dedup, gates, init_db, load, materialize, stage4
from wcd_pipeline.config import database_url
from wcd_schema import parquet as layout
from wcd_schema.ids import instance_id
from wcd_schema.staging import SCHEMAS

pytestmark = pytest.mark.skipif(not os.environ.get("WCD_TEST_DATABASE_URL"),
                                reason="set WCD_TEST_DATABASE_URL to a scratch Postgres database")

EN = "en.wikipedia.org"
S1, S2, S3 = "1" * 40, "2" * 40, "3" * 40
R1, R2, R3, R4 = "a" * 40, "b" * 40, "c" * 40, "d" * 40


def _stage(d, stem, nc, ci, td):
    """One shard's staging files: nc (sha1, page, type), ci (page, raw, sha1, name),
    td (sha1, template, key, value)."""
    d.mkdir(parents=True, exist_ok=True)
    tables = {
        "containers": [{"label": EN}],
        "domains": [{"value": EN, "for_container_label": EN}],
        "wiki_templates": [{"domain_label": EN, "name": n} for n in sorted({t[1] for t in td})],
        "normalized_citations": [{"normalized_sha1": s, "reference_normalized": f"<ref>{s[0]}</ref>",
                                  "appears_on_page_id": p, "appears_on_domain": EN, "citation_type": ty}
                                 for s, p, ty in nc],
        "citation_instances": [{"citation_instance_id": instance_id(EN, p, r), "domain": EN, "page_id": p,
                                "raw_sha1": r, "normalized_sha1": s, "reference_type": 1, "reference_name": n}
                               for p, r, s, n in ci],
        "template_data": [{"domain_label": EN, "template_name": t, "normalized_sha1": s, "offset_start": 0,
                           "parameter_key": k, "parameter_value": v} for s, t, k, v in td],
    }
    for name, rows in tables.items():
        pq.write_table(pa.Table.from_pylist(rows, schema=SCHEMAS[name]), d / f"{stem}-{name}-0000.parquet")


def stage_a(d):
    _stage(d, "a", nc=[(S1, 10, "template"), (S2, 20, "template")],
           ci=[(10, R1, S1, "n2"), (20, R2, S2, None)],
           td=[(S1, "Cite web", "title", "Zebra"), (S1, "Cite web", "url", "http://x"),
               (S2, "Cite book", "title", "Book")])


def stage_b(d):
    # S1 again from page 30: another raw form whose parameter value is smaller, and typed
    # endnote (which changes its parts); page 20's instance of S2 named "n1" (< NULL by MIN); S2 cited from page 40;
    # S3 new
    _stage(d, "b", nc=[(S1, 30, "endnote"), (S2, 40, "template"), (S3, 30, "template")],
           ci=[(30, R3, S1, None), (20, R2, S2, "n1"), (40, R4, S2, None), (30, R1, S3, None)],
           td=[(S1, "Cite web", "title", "Apple"), (S1, "Cite web", "url", "http://x"),
               (S2, "Cite book", "title", "Book"), (S3, "Cite web", "title", "Apple")])


@pytest.fixture()
def engine(monkeypatch):
    os.environ["DATABASE_URL"] = os.environ["WCD_TEST_DATABASE_URL"]
    monkeypatch.setattr(load, "TEMPLATE_COUNT_MIN", 1)   # every value is "heavy"
    engine = create_engine(database_url())
    yield engine
    engine.dispose()


def _fresh(engine):
    with engine.begin() as con:
        con.execute(text("DROP SCHEMA public CASCADE"))
        con.execute(text("CREATE SCHEMA public"))
    init_db.main([])


DENORM_COLS = "citation_instance_id, page_id, normalized_sha1, citation_type, reference_name, part_index, norm_subtype"


def _snapshot(engine, parquet):
    with engine.connect() as con:
        q = lambda sql: sorted(tuple(r) for r in con.execute(text(sql)))  # noqa: E731
        out = {
            "normalized_citations": q("SELECT normalized_sha1, appears_on_page_id, citation_type FROM normalized_citations"),
            "citation_instances": q("SELECT c.id, c.page_id, n.normalized_sha1, c.reference_type, c.reference_name "
                                    "FROM citation_instances c JOIN normalized_citations n ON n.id = c.normalized_id"),
            "wiki_templates": q("SELECT name FROM wiki_templates"),
            "normalized_citation_parts": q("SELECT n.normalized_sha1, p.part_index, p.part_sha1, p.content, p.part_type "
                                           "FROM normalized_citation_parts p "
                                           "JOIN normalized_citations n ON n.id = p.normalized_id"),
            "template_value_counts": q("SELECT w.name, t.parameter_key, t.parameter_value, t.n_citations "
                                       "FROM template_value_counts t JOIN wiki_templates w ON w.id = t.wiki_template_id"),
        }
    con = duckdb.connect()
    for ds, cols in (("template_params", "page_id, normalized_sha1, template_name, parameter_key, parameter_value"),
                     ("template_param_index", "normalized_sha1, template_name, parameter_key, parameter_value")):
        out[ds] = sorted(con.execute(
            f"SELECT {cols} FROM read_parquet('{layout.dataset_glob(parquet, ds)}', hive_partitioning = true)"
        ).fetchall(), key=repr)
    out["citation_instances_denorm"] = sorted(con.execute(
        f"SELECT {DENORM_COLS} FROM read_parquet('{layout.dataset_glob(parquet, 'citation_instances_denorm')}', "
        "hive_partitioning = true)").fetchall(), key=repr)
    return out


def _dedup_load(staging, parquet, *dedup_args, changes=False):
    dedup.main(["-d", str(staging), *dedup_args])
    load.main(["-d", str(staging), "--parquet-dir", str(parquet), *(["--changes"] if changes else [])])
    redo = ["--resume", "--redo-from", str(staging / "deduped" / "changes" / "normalized_citations.parquet")]
    assert stage4.main(["--jobs", "1", "--no-gate", *(redo if changes else [])]) == 0
    # denorm-export: in full, or for the staged pages plus those citing a retyped citation
    _denorm(staging, parquet, changes)


def _denorm(staging, parquet, changes):
    """denorm-export: in full, or for the staged pages (the touched ones) plus those citing
    a retyped citation."""
    pages = []
    if changes:
        touched = staging / "touched.parquet"
        staged = pa.concat_tables(pq.read_table(f) for f in sorted(staging.glob("*-citation_instances-*.parquet")))
        pq.write_table(pa.table({"page_id": pa.array(sorted(set(staged.column("page_id").to_pylist())),
                                                     pa.int64())}), touched)
        pages = ["--pages-from", str(touched),
                 "--pages-from", str(staging / "deduped" / "changes" / "retyped_pages.parquet")]
    assert materialize.main(["--domain", EN, "--parquet-dir", str(parquet), *pages]) == 0


def test_changes_load_equals_full_load(engine, tmp_path):
    # refresh: A in full, then B's changes
    _fresh(engine)
    stage_a(tmp_path / "a")
    stage_b(tmp_path / "b")
    _dedup_load(tmp_path / "a", tmp_path / "pq-refresh")
    _dedup_load(tmp_path / "b", tmp_path / "pq-refresh", "--previous", str(tmp_path / "a" / "deduped"), changes=True)
    refreshed = _snapshot(engine, tmp_path / "pq-refresh")

    # full: A and B staged together
    _fresh(engine)
    both = tmp_path / "both"
    shutil.copytree(tmp_path / "a", both, ignore=shutil.ignore_patterns("deduped"))
    for f in (tmp_path / "b").glob("b-*.parquet"):
        shutil.copy(f, both)
    _dedup_load(both, tmp_path / "pq-full")
    full = _snapshot(engine, tmp_path / "pq-full")

    # the case this test is for: merged values of rows A loaded did change
    assert (S1, 10, "endnote") in full["normalized_citations"]
    assert (10, S1, "Cite web", "title", "Apple") in full["template_params"]
    assert ("Cite web", "title", "Apple", 2) in full["template_value_counts"]
    # page 10 is not in B, but cites S1, which B retyped
    assert pq.read_table(tmp_path / "b" / "deduped" / "changes" / "retyped_pages.parquet").column(
        "page_id").to_pylist() == [10, 30]
    assert any(r[1] == 10 and r[3] == "endnote" for r in full["citation_instances_denorm"])
    for name in full:
        assert refreshed[name] == full[name], name

    # loading the same changes again changes nothing
    _fresh(engine)
    _dedup_load(tmp_path / "a", tmp_path / "pq-again")
    for _ in range(2):
        load.main(["-d", str(tmp_path / "b"), "--parquet-dir", str(tmp_path / "pq-again"), "--changes"])
        assert stage4.main(["--jobs", "1", "--no-gate", "--resume", "--redo-from",
                            str(tmp_path / "b" / "deduped" / "changes" / "normalized_citations.parquet")]) == 0
        _denorm(tmp_path / "b", tmp_path / "pq-again", changes=True)
    assert _snapshot(engine, tmp_path / "pq-again") == full


    # gate 3 over the change set: passes, and catches a changed row that did not land, or
    # landed without its merged value
    def gate():
        with Session(engine) as session:
            return gates.stage3_changes(session, EN, str(tmp_path / "pq-again"), str(tmp_path / "b"))
    assert gate() == []
    with engine.begin() as con:
        con.execute(text("UPDATE normalized_citations SET citation_type = 'template' WHERE normalized_sha1 = :s"),
                    {"s": S1})
        con.execute(text("UPDATE citation_instances SET reference_name = NULL WHERE reference_name = 'n1'"))
        con.execute(text("DELETE FROM normalized_citation_parts WHERE normalized_id = "
                         "(SELECT id FROM normalized_citations WHERE normalized_sha1 = :s)"), {"s": S3})
        con.execute(text("DELETE FROM citation_instances WHERE normalized_id = "
                         "(SELECT id FROM normalized_citations WHERE normalized_sha1 = :s)"), {"s": S3})
    failures = gate()
    assert any("1 of 2 changed normalized_citations rows differ" in f for f in failures), failures
    assert any("1 of 4 changed citation_instances rows are not in Postgres" in f for f in failures), failures
    assert any("1 of 4 changed citation_instances rows differ" in f for f in failures), failures
