"""Gate-R regression: Stage 1 (extract) + Stage 3 dedup on the synthetic bundle
must reproduce the committed golden Parquet exactly (set semantics, per table).

Regenerate the golden files after an intentional pipeline change with:

    UPDATE_GOLDEN=1 uv run pytest packages/pipeline/tests/test_stage1_golden.py

and review the diff of fixtures/golden/synthetic/ before committing.
"""
from __future__ import annotations

import glob
import os
import shutil
from pathlib import Path

import duckdb
import pytest

from wcd_pipeline import dedup, extract

from synthetic_bundle import render  # noqa: E402  (tests dir is on sys.path via rootdir conftest)

import zstandard as zstd

REPO_ROOT = Path(__file__).resolve().parents[3]
GOLDEN = REPO_ROOT / "fixtures" / "golden" / "synthetic"
TABLES = (
    "citation_histories", "citation_instances", "containers", "documents", "domains",
    "ncwr", "normalized_citations", "revisions", "template_data", "web_resources",
    "wiki_templates",
)


@pytest.fixture(scope="module")
def deduped_dir(tmp_path_factory) -> Path:
    base = tmp_path_factory.mktemp("stage1")
    bundle = base / "synthetic.mwrev.zst"
    bundle.write_bytes(zstd.ZstdCompressor(level=3).compress(render()))
    staging = base / "staging"
    extract.main([str(bundle), "-o", str(staging / "synthetic"), "--domain", "en.wikipedia.org"])
    dedup.main(["-d", str(staging)])
    out = staging / "deduped"
    if os.environ.get("UPDATE_GOLDEN"):
        GOLDEN.mkdir(parents=True, exist_ok=True)
        for t in TABLES:
            shutil.copy(out / f"{t}.parquet", GOLDEN / f"{t}.parquet")
    return out


@pytest.mark.parametrize("table", TABLES)
def test_table_matches_golden(deduped_dir: Path, table: str):
    golden = GOLDEN / f"{table}.parquet"
    assert golden.exists(), f"missing golden file {golden}; run with UPDATE_GOLDEN=1"
    con = duckdb.connect()
    a = f"read_parquet('{golden}')"
    b = f"read_parquet('{deduped_dir / (table + '.parquet')}')"
    n_a = con.execute(f"select count(*) from {a}").fetchone()[0]
    n_b = con.execute(f"select count(*) from {b}").fetchone()[0]
    only_a = con.execute(f"select * from {a} except select * from {b}").fetchall()
    only_b = con.execute(f"select * from {b} except select * from {a}").fetchall()
    assert n_a == n_b and not only_a and not only_b, (
        f"{table}: golden={n_a} rows, actual={n_b}; missing={only_a[:3]} extra={only_b[:3]}"
    )
