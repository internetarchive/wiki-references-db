"""The COPY path of Stage 3 keeps NULLs, empty strings and awkward text exact,
and chunking does not change the result."""
import os

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

from wcd_pipeline.config import _with_driver
from wcd_pipeline.load import copy_parquet_in_chunks

DB = os.environ.get("WCD_TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(not DB, reason="WCD_TEST_DATABASE_URL not set")

VALUES = [None, "", "plain", 'quote " inside', "comma, and\nnewline", "back\\slash", "tab\there", "ünïcödé — ✓", "NULL", "\\N"]


@pytest.mark.parametrize("chunk_rows", [3, 1000])
def test_copy_round_trips_values(tmp_path, chunk_rows):
    path = tmp_path / "t.parquet"
    pq.write_table(pa.table({"k": pa.array(range(len(VALUES)), pa.int32()), "v": pa.array(VALUES, pa.string())}), path)
    engine = create_engine(_with_driver(DB), poolclass=NullPool)
    session = sessionmaker(bind=engine)()
    try:
        session.execute(text("DROP TABLE IF EXISTS _copy_target"))
        session.execute(text("CREATE TABLE _copy_target (k integer PRIMARY KEY, v text)"))
        session.commit()
        staged, written = copy_parquet_in_chunks(
            session, f"SELECT k, v FROM read_parquet('{path}')", temp_table="_copy_stage", temp_ddl="k integer, v text",
            columns=["k", "v"], insert_sql="INSERT INTO _copy_target SELECT k, v FROM _copy_stage",
            chunk_rows=chunk_rows)
        assert (staged, written) == (len(VALUES), len(VALUES))
        got = [v for _, v in session.execute(text("SELECT k, v FROM _copy_target ORDER BY k")).all()]
        assert got == VALUES
    finally:
        session.rollback()
        session.execute(text("DROP TABLE IF EXISTS _copy_target"))
        session.commit()
        session.close()
