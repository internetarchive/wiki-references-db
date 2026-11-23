"""``dedup --previous``'s change set: written in key partitions it is the one-pass EXCEPT,
and a run that failed while writing it resumes without deduplicating again."""
from __future__ import annotations

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from wcd_pipeline import dedup


def _nc(path, rows):
    pq.write_table(pa.table({
        "normalized_sha1": [r[0] for r in rows],
        "reference_normalized": [r[1] for r in rows],
        "appears_on_page_id": pa.array([1] * len(rows), pa.int64()),
        "appears_on_domain": ["en.wikipedia.org"] * len(rows),
        "citation_type": [r[2] for r in rows],
    }), path)


def _rows(path):
    return sorted(duckdb.connect().execute(f"SELECT * FROM '{path}'").fetchall(), key=repr)


@pytest.fixture
def sets(tmp_path):
    new, prev = tmp_path / "new", tmp_path / "prev"
    new.mkdir(), prev.mkdir()
    same = [(f"s{i}", f"ref {i}", "template") for i in range(20)]
    _nc(prev / "normalized_citations.parquet",
        same + [("t1", "old", "ref_tag"), ("gone", "x", "ref_tag"), (None, "null key", "ref_tag")])
    _nc(new / "normalized_citations.parquet",
        same + [("t1", "old", "template"), ("n1", "new", "bare_url"), ("n2", "new", "endnote"),
                (None, "null key", "ref_tag")])
    return new, prev


@pytest.mark.parametrize("rows_per_partition", [3, 1000])
def test_partitioned_changes_equal_one_except(sets, monkeypatch, rows_per_partition):
    new, prev = sets
    monkeypatch.setattr(dedup, "ROWS_PER_PARTITION", rows_per_partition)
    dedup.write_changes(duckdb.connect(), str(new), str(prev), "normalized_citations")
    got = _rows(new / "changes" / "normalized_citations.parquet")
    want = sorted(duckdb.connect().execute(
        f"SELECT * FROM '{new}/normalized_citations.parquet' EXCEPT "
        f"SELECT * FROM '{prev}/normalized_citations.parquet'").fetchall(), key=repr)
    assert got == want
    assert [r[0] for r in got] == ["n1", "n2", "t1"]
    assert not list(new.glob(".changes-*")) and not list((new / "changes").glob("*.partial"))


def test_no_changes_gives_an_empty_file_with_the_columns(sets, monkeypatch):
    new, prev = sets
    monkeypatch.setattr(dedup, "ROWS_PER_PARTITION", 3)
    dedup.write_changes(duckdb.connect(), str(prev), str(prev), "normalized_citations")
    t = pq.read_table(prev / "changes" / "normalized_citations.parquet")
    assert t.num_rows == 0 and t.column_names == pq.read_schema(prev / "normalized_citations.parquet").names


def test_a_failed_change_set_resumes_without_deduplicating(tmp_path, monkeypatch):
    staging, previous = tmp_path / "staging", tmp_path / "prev"
    (staging / "s1").mkdir(parents=True), previous.mkdir()
    _nc(staging / "s1" / "s1-normalized_citations-0000.parquet", [("a", "r", "template")])
    _nc(previous / "normalized_citations.parquet", [("b", "r", "template")])
    real = dedup.write_changes

    def fail(*a, **k):
        raise MemoryError("out of memory")
    monkeypatch.setattr(dedup, "write_changes", fail)
    with pytest.raises(MemoryError):
        dedup.main(["-d", str(staging), "--previous", str(previous), "--tables", "normalized_citations"])

    calls = []
    monkeypatch.setattr(dedup, "write_changes", real)
    monkeypatch.setattr(dedup, "ALL_TABLES", [(n, (lambda *a: calls.append(n)) if n == "normalized_citations" else f)
                                              for n, f in dedup.ALL_TABLES])
    dedup.main(["-d", str(staging), "--previous", str(previous), "--tables", "normalized_citations"])
    assert calls == []
    assert [r[0] for r in _rows(staging / "deduped" / "changes" / "normalized_citations.parquet")] == ["a"]
    assert (staging / "deduped" / ".done-normalized_citations").exists()
