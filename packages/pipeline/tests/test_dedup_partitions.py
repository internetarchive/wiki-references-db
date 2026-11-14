"""wcd dedup in key partitions gives the same tables as one pass: the synthetic
bundle's staging is deduped with one partition per 3 staged rows (many
partitions, several empty) and with the default (one), and every deduped table
must match."""
from __future__ import annotations

import shutil

import duckdb
import pytest
import zstandard as zstd

from wcd_pipeline import dedup, extract

from synthetic_bundle import render  # noqa: E402
from test_stage1_golden import DOMAIN, STAGING_TABLES  # noqa: E402


@pytest.fixture(scope="module")
def staged(tmp_path_factory):
    base = tmp_path_factory.mktemp("dedup-parts")
    bundle = base / "synthetic.mwrev.zst"
    bundle.write_bytes(zstd.ZstdCompressor(level=3).compress(render()))
    extract.main([str(bundle), "-o", str(base / "one" / "synthetic"), "--domain", DOMAIN, "--parquet-dir", str(base / "pq")])
    shutil.copytree(base / "one", base / "many")
    dedup.main(["-d", str(base / "one")])
    dedup.main(["-d", str(base / "many"), "--rows-per-partition", "3", "--temp-dir", str(base / "tmp")])
    return base


@pytest.mark.parametrize("table", STAGING_TABLES)
def test_partitioned_dedup_matches_one_pass(staged, table):
    con = duckdb.connect()
    a = f"SELECT * FROM read_parquet('{staged / 'one' / 'deduped' / table}.parquet')"
    b = f"SELECT * FROM read_parquet('{staged / 'many' / 'deduped' / table}.parquet')"
    na, nb = (con.execute(f"SELECT count(*) FROM ({q})").fetchone()[0] for q in (a, b))
    assert na == nb and na > 0
    assert not con.execute(f"{a} EXCEPT {b}").fetchall() and not con.execute(f"{b} EXCEPT {a}").fetchall()


def test_partition_files_are_removed(staged):
    assert not list((staged / "tmp").glob(".partitions-*")) if (staged / "tmp").exists() else True
    assert not list((staged / "many" / "deduped").glob("*.partial"))
