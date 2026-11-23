"""``stage0-load --append``: new metadata added to the revisions dataset gives what a rebuild
from all the metadata gives, including revisions the dataset already has, and a re-run
changes nothing."""
from __future__ import annotations

import datetime

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from wcd_pipeline import stage0
from wcd_schema import parquet as layout

DOMAIN = "en.wikipedia.org"


def _meta(path, revs):
    """RevisionChest metadata rows: (rev_id, page_id, parent, timestamp)."""
    pq.write_table(pa.table({
        "type": ["revision"] * len(revs) + ["page"],
        "rev_id": pa.array([r[0] for r in revs] + [None], pa.uint64()),
        "page_id": pa.array([r[1] for r in revs] + [1], pa.uint64()),
        "parent_rev_id": pa.array([r[2] for r in revs] + [None], pa.uint64()),
        "timestamp": [r[3] for r in revs] + [None],
        "file_path": [str(path.with_suffix(".mwrev"))] * len(revs) + [None],
    }), path)


def _rows(base):
    con = duckdb.connect()
    return sorted(con.execute(
        f"SELECT page_bucket, revision_id, page_id, parent_revision_id, revision_timestamp "
        f"FROM read_parquet('{layout.dataset_glob(base, 'revisions')}', hive_partitioning = true)").fetchall())


def test_append_equals_rebuild(tmp_path):
    old, new = tmp_path / "meta" / "old.parquet", tmp_path / "meta" / "new.parquet"
    old.parent.mkdir()
    _meta(old, [(10, 1, None, "2026-10-01T00:00:00Z"), (11, 1, 10, "2026-10-02T00:00:00Z"),
                (20, 513, None, "2026-10-03T00:00:00Z")])
    # rev 11 again (already in the dataset), a page's new revision, a new page, and a
    # revision twice in the same file
    _meta(new, [(11, 1, 10, "2026-10-02T00:00:00Z"), (30, 1, 11, "2026-11-02T00:00:00Z"),
                (40, 2, None, "2026-11-03T00:00:00Z"), (40, 2, None, "2026-11-03T00:00:00Z")])

    stage0.write_revisions_parquet(str(old.parent), DOMAIN, str(tmp_path / "rebuilt"), work_dir=str(tmp_path / "w"))
    stage0.write_revisions_parquet(str(old), DOMAIN, str(tmp_path / "appended"), work_dir=str(tmp_path / "w"))
    assert stage0.append_revisions_parquet(str(new), DOMAIN, str(tmp_path / "appended"),
                                           work_dir=str(tmp_path / "w")) == 2
    assert _rows(tmp_path / "appended") == _rows(tmp_path / "rebuilt")
    assert len(_rows(tmp_path / "rebuilt")) == 5
    assert (layout.partition_dir(tmp_path / "appended", "revisions", DOMAIN, 1) / "meta-new.parquet").exists()

    # a re-run replaces the file's own bucket files, and adds nothing
    assert stage0.append_revisions_parquet(str(new), DOMAIN, str(tmp_path / "appended"),
                                           work_dir=str(tmp_path / "w")) == 2
    assert _rows(tmp_path / "appended") == _rows(tmp_path / "rebuilt")
    assert not list((tmp_path / "w").iterdir())   # its scratch is gone


def test_append_to_an_empty_dataset(tmp_path):
    new = tmp_path / "new.parquet"
    _meta(new, [(40, 2, None, "2026-11-03T00:00:00Z")])
    assert stage0.append_revisions_parquet(str(new), DOMAIN, str(tmp_path / "pq"), work_dir=str(tmp_path / "w")) == 1
    assert _rows(tmp_path / "pq") == [(2, 40, 2, None, datetime.datetime(2026, 11, 3))]
