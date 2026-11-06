"""The revision load's work units: the rev_id-sorted intermediate file and the
half-open ranges cut from its row groups must cover every revision exactly
once (a range taken from the wrong column once left all but one unit empty)."""
from __future__ import annotations

import random

import duckdb

from wcd_pipeline import stage0


def _metadata(path, n=20_000, seed=3):
    rng = random.Random(seed)
    rev_ids = rng.sample(range(1, 10 * n), n)
    con = duckdb.connect()
    con.execute("""CREATE TABLE m (type VARCHAR, page_id UBIGINT, ns BIGINT, title VARCHAR, rev_id UBIGINT,
                   parent_rev_id UBIGINT, file_path VARCHAR, offset_begin UBIGINT, length UBIGINT, timestamp VARCHAR)""")
    con.executemany("INSERT INTO m VALUES ('revision', ?, 0, NULL, ?, NULL, ?, ?, 10, '2020-01-01T00:00:00Z')",
                    [(r % 97, r, f"b{r % 3}.mwrev.zst", r * 10) for r in rev_ids])
    con.execute("INSERT INTO m (type, page_id, ns, title) VALUES ('page', 1, 0, 'P')")
    con.execute(f"COPY m TO '{path}' (FORMAT PARQUET)")
    return sorted(rev_ids)


def test_units_cover_every_revision_once(tmp_path, monkeypatch):
    monkeypatch.setattr(stage0, "REVISION_GROUP", 4096)
    meta = tmp_path / "meta.parquet"
    rev_ids = _metadata(meta)
    bundles = {"b0.mwrev.zst": 10, "b1.mwrev.zst": 11, "b2.mwrev.zst": 12}
    f = stage0._sorted_revisions(str(meta), bundles, str(tmp_path))
    ranges = stage0._revision_ranges(f)
    assert len(ranges) > 3
    # contiguous, half-open, increasing; the last is open-ended
    assert all(hi == ranges[i + 1][0] for i, (_, hi) in enumerate(ranges[:-1]))
    assert all(lo < hi for lo, hi in ranges[:-1]) and ranges[-1][1] is None
    assert ranges[0][0] == rev_ids[0]
    con = duckdb.connect()
    seen = []
    for lo, hi in ranges:
        where = f"rev_id >= {lo}" + (f" AND rev_id < {hi}" if hi is not None else "")
        part = [r[0] for r in con.execute(f"SELECT rev_id FROM '{f}' WHERE {where} ORDER BY 1").fetchall()]
        assert part, (lo, hi)
        seen += part
    assert seen == rev_ids
    # bundle ids resolved in the file
    got = dict(con.execute(f"SELECT rev_id, bundle_id FROM '{f}'").fetchall())
    assert all(got[r] == bundles[f"b{r % 3}.mwrev.zst"] for r in rev_ids)


def test_memory_limit_values():
    for ok in ("24GB", "512MiB", " 1.5 gb ", "100MB"):
        assert stage0.check_memory_limit(ok) == ok
    assert stage0.check_memory_limit(None) is None
    for bad in ("lots", "24", "1GB'; DROP TABLE x; --", "GB"):
        try:
            stage0.check_memory_limit(bad)
        except ValueError:
            continue
        raise AssertionError(f"accepted {bad!r}")


def test_connections_get_the_memory_limit_and_spill_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(stage0, "MEMORY_LIMIT", "1GB")
    con = stage0._duckdb(temp_dir=str(tmp_path / "spill"))
    limit, temp = con.execute("SELECT current_setting('memory_limit'), current_setting('temp_directory')").fetchone()
    assert limit in ("953.6 MiB", "1.0 GB", "1GB"), limit
    assert temp == str(tmp_path / "spill")
    monkeypatch.setattr(stage0, "MEMORY_LIMIT", None)
    assert stage0._duckdb().execute("SELECT current_setting('memory_limit')").fetchone()[0] != limit


def test_cli_rejects_a_bad_memory_limit(capsys):
    try:
        stage0.main(["--domain", "x.org", "--metadata", "m.parquet", "--no-postgres", "--parquet-dir", "p",
                     "--memory-limit", "plenty"])
    except SystemExit as e:
        assert e.code == 2
    else:
        raise AssertionError("accepted --memory-limit plenty")
    assert "not a memory size" in capsys.readouterr().err


def test_parquet_step_spills_under_the_work_dir_and_cleans_up(tmp_path, monkeypatch):
    meta = tmp_path / "meta.parquet"
    rev_ids = _metadata(meta, n=3000)
    work = tmp_path / "work"
    monkeypatch.setattr(stage0, "MEMORY_LIMIT", "512MB")
    n = stage0.write_revisions_parquet(str(meta), "x.wikipedia.org", str(tmp_path / "pq"), work_dir=str(work))
    assert n == len(rev_ids)
    assert work.is_dir() and not list(work.iterdir())
