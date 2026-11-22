"""Page sets (wcd_pipeline.pages): reading them, and rewriting a bucket for some pages,
including after a rewrite that stopped part way."""
from __future__ import annotations

import json
import os

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from wcd_pipeline import pages
from wcd_schema import parquet as layout

DS = "page_revision_stats"
DOMAIN = "en.wikipedia.org"


def _rows(page_ids, base=0):
    schema = layout.SCHEMAS[DS]
    cols = {f.name: [None] * len(page_ids) for f in schema}
    cols["page_id"] = page_ids
    cols["rev_id"] = [base + i for i in range(len(page_ids))]
    return pa.Table.from_pydict(cols, schema=schema)


def _bucket(tmp_path):
    return layout.partition_dir(tmp_path, DS, DOMAIN, 1)


def _read(tmp_path):
    d = _bucket(tmp_path)
    t = pa.concat_tables([pq.read_table(f) for f in sorted(d.glob(layout.FILE_GLOB))])
    return sorted(zip(t.column("page_id").to_pylist(), t.column("rev_id").to_pylist()))


def _seed(tmp_path):
    d = _bucket(tmp_path)
    d.mkdir(parents=True)
    pq.write_table(_rows([1, 1, 513]), d / "a.parquet")
    pq.write_table(_rows([1025], base=10), d / "b.parquet")


def test_read_page_set(tmp_path):
    pq.write_table(pa.table({"page_id": pa.array([5, 3, None, 5], pa.uint64()), "x": [1, 2, 3, 4]}),
                   tmp_path / "m.parquet")
    assert pages.read_page_set(tmp_path / "m.parquet") == [3, 5]
    assert pages.read_page_set(tmp_path) == [3, 5]
    assert pages.by_bucket([3, 515, 4]) == {3: [3, 515], 4: [4]}


def test_replace_pages_keeps_other_pages(tmp_path):
    _seed(tmp_path)
    n = pages.replace_pages(tmp_path, DS, DOMAIN, 1, [1, 1537], _rows([1, 1537], base=100))
    assert n == 4
    assert _read(tmp_path) == [(1, 100), (513, 2), (1025, 10), (1537, 101)]
    assert len(list(_bucket(tmp_path).glob(layout.FILE_GLOB))) == 1
    # the same page set again gives the same rows
    pages.replace_pages(tmp_path, DS, DOMAIN, 1, [1, 1537], _rows([1, 1537], base=100))
    assert _read(tmp_path) == [(1, 100), (513, 2), (1025, 10), (1537, 101)]


@pytest.mark.parametrize("moved_in", [False, True])
def test_interrupted_replace_is_repaired(tmp_path, monkeypatch, moved_in):
    """A rewrite that stopped before (or after) moving its file in: the next one sees the
    old rows once, never twice and never none."""
    _seed(tmp_path)
    real_replace = os.replace

    class Stop(Exception):
        pass

    def stop_replace(a, b):
        if moved_in:
            real_replace(a, b)
        raise Stop

    monkeypatch.setattr(pages.os, "replace", stop_replace)
    with pytest.raises(Stop):
        pages.replace_pages(tmp_path, DS, DOMAIN, 1, [1], _rows([1], base=100))
    monkeypatch.setattr(pages.os, "replace", real_replace)
    assert (_bucket(tmp_path) / pages.JOURNAL).exists()
    j = json.loads((_bucket(tmp_path) / pages.JOURNAL).read_text())
    assert sorted(j["old"]) == ["a.parquet", "b.parquet"]

    pages.replace_pages(tmp_path, DS, DOMAIN, 1, [1], _rows([1], base=200))
    assert _read(tmp_path) == [(1, 200), (513, 2), (1025, 10)]
    assert not (_bucket(tmp_path) / pages.JOURNAL).exists()
    assert not list(_bucket(tmp_path).glob(".pages-*"))
