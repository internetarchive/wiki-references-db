"""Per-page staging dedup (PageRefCache.first) writes fewer staging rows and
leaves `wcd dedup`'s output unchanged: the synthetic bundle is extracted with
it on and off (max_staged=0 disables it) and every deduped table must match."""
from __future__ import annotations

import functools

import duckdb
import pytest
import zstandard as zstd

from wcd_pipeline import dedup, extract

from synthetic_bundle import render  # noqa: E402
from test_stage1_golden import DOMAIN, STAGING_TABLES  # noqa: E402


def _extract(tmp, monkeypatch, max_staged=None):
    bundle = tmp / "synthetic.mwrev.zst"
    bundle.write_bytes(zstd.ZstdCompressor(level=3).compress(render()))
    if max_staged is not None:
        monkeypatch.setattr(extract, "PageRefCache", functools.partial(extract.PageRefCache, max_staged=max_staged))
    extract.main([str(bundle), "-o", str(tmp / "staging" / "synthetic"), "--domain", DOMAIN, "--parquet-dir", str(tmp / "pq")])
    monkeypatch.undo()
    con = duckdb.connect()
    staged = {t: con.execute(f"SELECT count(*) FROM read_parquet('{tmp}/staging/synthetic/*-{t}-*.parquet')").fetchone()[0]
              for t in STAGING_TABLES}
    dedup.main(["-d", str(tmp / "staging")])
    return staged, tmp / "staging" / "deduped"


@pytest.mark.parametrize("table", STAGING_TABLES)
def test_page_dedup_changes_nothing_after_dedup(tmp_path, monkeypatch, table):
    (tmp_path / "on").mkdir(); (tmp_path / "off").mkdir()
    staged_on, on = _extract(tmp_path / "on", monkeypatch)
    staged_off, off = _extract(tmp_path / "off", monkeypatch, max_staged=0)
    con = duckdb.connect()
    a, b = f"SELECT * FROM read_parquet('{on / table}.parquet')", f"SELECT * FROM read_parquet('{off / table}.parquet')"
    assert con.execute(f"SELECT count(*) FROM ({a})").fetchone() == con.execute(f"SELECT count(*) FROM ({b})").fetchone()
    assert not con.execute(f"{a} EXCEPT {b}").fetchall() and not con.execute(f"{b} EXCEPT {a}").fetchall()
    assert staged_on[table] <= staged_off[table]
    if table in ("citation_instances", "normalized_citations", "template_data"):
        assert staged_on[table] < staged_off[table], (table, staged_on[table], staged_off[table])
