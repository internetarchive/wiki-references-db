"""Stage 7f's set-based UPDATE scores every global source exactly as the
per-row compute_score path did, over every combination of the inputs the
score reads (NULL and '' included), with the canonical part chosen as the
first by (normalized_id, part_index) among parts sharing its sha1.

Needs a scratch database; skipped unless ``WCD_TEST_DATABASE_URL`` is set
(the schema ``public`` of that database is dropped and recreated).
"""
from __future__ import annotations

import itertools
import os

import psycopg2
import psycopg2.extras
import pytest
from sqlalchemy import create_engine, text

from wcd_pipeline import init_db, stage7
from wcd_pipeline.config import database_url, psycopg_dsn

pytestmark = pytest.mark.skipif(not os.environ.get("WCD_TEST_DATABASE_URL"),
                                reason="set WCD_TEST_DATABASE_URL to a scratch Postgres database")

SOURCE_TYPES = [None, "", "ref_tag", "template", "both"]
MATCH_TYPES = [None, "", "doi", "isbn", "pmid", "pmc", "arxiv", "wikidata_qid", "url", "author_year", "title_fuzzy",
               "strong_author_fuzzy", "author_title_fuzzy", "singleton", "author_year_fuzzy"]
AUTHORS = [None, "", " ", "smith"]
YEARS = [None, "", "2001"]
PARTS = [None] + list(itertools.product([None, 0, 2], [None, False, True], [None, "noise", "prose_citation"]))


def test_set_based_score_matches_compute_score():
    os.environ["DATABASE_URL"] = os.environ["WCD_TEST_DATABASE_URL"]
    engine = create_engine(database_url())
    with engine.begin() as con:
        con.execute(text("DROP SCHEMA public CASCADE"))
        con.execute(text("CREATE SCHEMA public"))
    init_db.main([])
    pg = psycopg2.connect(psycopg_dsn())
    with pg.cursor() as cur:
        cur.execute("INSERT INTO domains (id, value) VALUES (1, 'en.wikipedia.org')")
        cur.execute("INSERT INTO normalized_citations (id, normalized_sha1, reference_normalized, appears_on_domain_id, "
                    "appears_on_page_id, citation_type) VALUES (1, repeat('a', 40), 'r', 1, 1, 'ref_tag'), "
                    "(2, repeat('b', 40), 'r', 1, 1, 'ref_tag')")
        for k, part in enumerate(PARTS[1:]):
            sha = f"{k:040x}"
            tier, reliable, subtype = part
            # the canonical part (normalized_id 1) and a later one with the same sha1 and other fields
            cur.execute("INSERT INTO normalized_citation_parts (normalized_id, part_index, part_sha1, content, part_type, "
                        "id_tier, year_reliable, norm_subtype) VALUES (1, %s, %s, 'c', 'ref_tag', %s, %s, %s), "
                        "(2, %s, %s, 'c', 'ref_tag', 1, true, 'noise')",
                        (k, sha, tier, reliable, subtype, k, sha))
        rows = []
        for i, (st, mt, au, yr, p) in enumerate(itertools.product(SOURCE_TYPES, MATCH_TYPES, AUTHORS, YEARS,
                                                                    range(len(PARTS)))):
            rows.append((f"{i:016x}", st, mt, au, yr, None if p == 0 else f"{p - 1:040x}"))
        psycopg2.extras.execute_values(cur, "INSERT INTO global_sources (global_source_id, source_type, match_type, "
                                            "author_key, year, canonical_part_sha1) VALUES %s", rows)
    pg.commit()

    def scores():
        with pg.cursor() as cur:
            cur.execute("SELECT global_source_id, reliability_score, verdict FROM global_sources ORDER BY 1")
            out = cur.fetchall()
            cur.execute("UPDATE global_sources SET reliability_score = NULL, verdict = NULL")
        pg.commit()
        return out

    assert stage7.step_f(pg, psycopg_dsn()) == len(rows)
    set_based = scores()
    stage7._step_f_rowwise(pg, psycopg_dsn())
    assert set_based == scores()
    assert {v for _, _, v in set_based} == {"high_confidence", "medium_confidence", "low_confidence", "questionable",
                                             "likely_not_citation"}
    pg.close()
    engine.dispose()
