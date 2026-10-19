import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from wcd_schema import parquet as layout
from wcd_schema.duckdb import bind_bucket_views, register_views


def _write(base, name, domain, bucket, table):
    d = layout.partition_dir(base, name, domain, bucket)
    d.mkdir(parents=True)
    pq.write_table(table, d / "part-0.parquet")


def test_layout_paths(tmp_path):
    assert str(layout.partition_dir(tmp_path, "revisions", "en.wikipedia.org", 17)).endswith("revisions/domain=en.wikipedia.org/page_bucket=17")
    assert layout.partition_glob(tmp_path, "revisions", "en.wikipedia.org", 17).endswith("page_bucket=17/*.parquet*")
    assert layout.dataset_glob(tmp_path, "revisions").endswith("revisions/**/*.parquet*")


def test_register_and_bind_views(tmp_path):
    base = tmp_path / "parquet"
    rev = pa.table({"revision_id": pa.array([1, 2], pa.int64()), "page_id": pa.array([513, 513], pa.int32()),
                    "parent_revision_id": pa.array([None, 1], pa.int64()),
                    "revision_timestamp": pa.array([0, 60], pa.timestamp("s"))})
    _write(base, "revisions", "en.wikipedia.org", 1, rev)
    hist = pa.table({"citation_instance_id": pa.array([7], pa.int64()), "revision_id": pa.array([2], pa.int64()), "page_id": pa.array([513], pa.int32())})
    _write(base, "citation_history", "en.wikipedia.org", 1, hist)

    con = duckdb.connect()
    created = register_views(con, str(base))
    assert set(created) == {"revisions", "citation_history"}   # the others do not exist yet
    assert con.execute("SELECT domain, page_bucket, count(*) FROM revisions GROUP BY ALL").fetchall() == [("en.wikipedia.org", 1, 2)]
    assert layout.list_buckets(base, "revisions", "en.wikipedia.org") == [1]

    # bucket binding: present partition, and a missing one becomes an empty typed view
    bind_bucket_views(con, str(base), "en.wikipedia.org", 1, ["revisions", "citation_history", "named_ref_uses"])
    assert con.execute("SELECT count(*) FROM revisions").fetchone()[0] == 2
    assert con.execute("SELECT count(*) FROM named_ref_uses").fetchone()[0] == 0
    cols = [d[0] for d in con.execute("SELECT * FROM named_ref_uses").description]
    assert cols == ["domain", "page_bucket", "page_id", "revision_id", "ref_name", "offset_start"]
    # a join across the bucket works with the empty view in place
    n = con.execute("SELECT count(*) FROM revisions r LEFT JOIN named_ref_uses n USING (page_id, revision_id)").fetchone()[0]
    assert n == 2
