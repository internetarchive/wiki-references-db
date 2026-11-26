"""Stage 7a, 7e and 7f write only the global sources whose values change: a
re-run over the same inputs leaves every row where it was (same ``xmin``), and
a changed source is still rewritten, recounted and rescored.

Needs a scratch database; skipped unless ``WCD_TEST_DATABASE_URL`` is set
(the schema ``public`` of that database is dropped and recreated).
"""
from __future__ import annotations

import os

import duckdb
import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from sqlalchemy import create_engine, text

from wcd_pipeline import init_db, stage7
from wcd_pipeline.config import database_url, psycopg_dsn
from wcd_schema import parquet as layout

pytestmark = pytest.mark.skipif(not os.environ.get("WCD_TEST_DATABASE_URL"),
                                reason="set WCD_TEST_DATABASE_URL to a scratch Postgres database")

D = "en.wikipedia.org"


def _stage(base, rows):
    d = layout.partition_dir(base, stage7.MI + "global_sources_stage", D)
    d.mkdir(parents=True, exist_ok=True)
    cols = {c: [] for c in stage7._GS_COLS}
    for gid, mt, title in rows:
        r = dict.fromkeys(stage7._GS_COLS)
        r.update(global_source_id=gid, match_type=mt, confidence=0.5, source_type="ref_tag", title=title)
        for c in cols:
            cols[c].append(r[c])
    types = {"confidence": pa.float64()}
    pq.write_table(pa.table({c: pa.array(v, types.get(c, pa.string())) for c, v in cols.items()}),
                   d / "part-0.parquet")


def _state(pg):
    with pg.cursor() as cur:
        cur.execute("SELECT global_source_id, xmin::text::bigint, match_type, n_pages, n_total_citations, "
                    "reliability_score, verdict FROM global_sources ORDER BY 1")
        return {r[0]: r[1:] for r in cur.fetchall()}


def test_rerun_writes_only_changed_sources(tmp_path):
    os.environ["DATABASE_URL"] = os.environ["WCD_TEST_DATABASE_URL"]
    engine = create_engine(database_url())
    with engine.begin() as con:
        con.execute(text("DROP SCHEMA public CASCADE"))
        con.execute(text("CREATE SCHEMA public"))
    init_db.main([])
    pg = psycopg2.connect(psycopg_dsn())
    con = duckdb.connect()
    with pg.cursor() as cur:
        cur.execute("INSERT INTO domains (id, value) VALUES (1, %s)", (D,))
    pg.commit()
    ids = [f"g{i}" for i in range(6)]

    def build(match_types, citations):
        _stage(tmp_path, [(g, mt, f"title {g}") for g, mt in zip(ids, match_types)])
        stage7.step_a(pg, con, D, str(tmp_path))
        with pg.cursor() as cur:
            cur.execute("DELETE FROM page_source_summary")
            cur.executemany("INSERT INTO page_source_summary (domain_id, page_id, global_source_id, n_citations) "
                            "VALUES (1, %s, %s, %s)",
                            [(p, g, n) for g, n in zip(ids, citations) for p in (1, 2)])
        pg.commit()
        return stage7.step_e(pg), stage7.step_f(pg, psycopg_dsn())

    assert build(["doi"] * 6, [1] * 6) == (6, 6)
    first = _state(pg)
    assert all(v[2] == 2 and v[4] is not None for v in first.values())

    # the same inputs again: nothing is written
    assert build(["doi"] * 6, [1] * 6) == (0, 0)
    assert _state(pg) == first

    # g1's match type changes (a new score), g2's counts change; the rest stay put
    assert build(["doi", "url", "doi", "doi", "doi", "doi"], [1, 1, 3, 1, 1, 1]) == (2, 1)
    after = _state(pg)
    assert {g for g in ids if after[g][0] != first[g][0]} == {"g1", "g2"}
    assert after["g1"][1] == "url" and after["g1"][4] != first["g1"][4]
    assert after["g2"][2:] == (2, 6) + first["g2"][4:]
    pg.close()
    engine.dispose()
