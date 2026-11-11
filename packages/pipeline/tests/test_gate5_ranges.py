"""Gate 5's overlap check orders ref_name_links ranges by revision time, as
Stage 5 builds them, not by revision id (enwiki has imported histories whose
ids run backwards)."""
import duckdb

from wcd_pipeline.gates import _RANGE_OVERLAP_SQL


def _count(links, revisions):
    con = duckdb.connect()
    con.execute("CREATE TABLE revisions (revision_id BIGINT, revision_timestamp TIMESTAMP)")
    con.executemany("INSERT INTO revisions VALUES (?, ?)", revisions)
    con.execute("CREATE TABLE ref_name_links (page_id INTEGER, ref_name VARCHAR, first_revision_id BIGINT, "
                "last_revision_id BIGINT)")
    con.executemany("INSERT INTO ref_name_links VALUES (?, ?, ?, ?)", links)
    return con.execute(_RANGE_OVERLAP_SQL.format(links="ref_name_links", revisions="revisions", where="")).fetchone()[0]


# ids 358 (Feb) → 354 (Apr 6, 16:45) → 355 (Apr 6, 21:41): one page's history, ids out of time order
REVS = [(358, "2010-02-10 19:13:47"), (354, "2010-04-06 16:45:31"), (355, "2010-04-06 21:41:36"),
        (360, "2010-05-01 00:00:00")]


def test_ranges_consecutive_in_time_do_not_overlap_whatever_their_ids():
    # [358 .. 354] then [355 .. open): an id comparison calls this an overlap (354 >= 355 is false, but
    # 358 < 355 is false too, so the old check paired them the other way round)
    assert _count([(7, "pop118", 358, 354), (7, "pop118", 355, None)], REVS) == 0


def test_real_overlaps_are_counted():
    # second range starts (Apr 6 16:45) before the first ends (Apr 6 21:41)
    assert _count([(7, "a", 358, 355), (7, "a", 354, 360)], REVS) == 1
    # an open range followed by another one
    assert _count([(7, "b", 358, None), (7, "b", 360, None)], REVS) == 1
    # other names and pages are independent
    assert _count([(7, "c", 358, 354), (8, "c", 354, None), (7, "d", 355, None)], REVS) == 0
