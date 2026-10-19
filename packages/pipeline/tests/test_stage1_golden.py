"""Gate-R regression: Stage 1 (extract) + Stage 3 dedup on the synthetic bundle
must reproduce the committed golden Parquet exactly (set semantics, per table):
the staging tables after dedup, and the three Parquet-tier datasets the
extractor writes directly (read back through their hive partitions).

Regenerate the golden files after an intentional pipeline change with:

    UPDATE_GOLDEN=1 uv run pytest packages/pipeline/tests/test_stage1_golden.py

and review the diff of fixtures/golden/synthetic/ before committing.
"""
from __future__ import annotations

import os
import shutil
from pathlib import Path

import duckdb
import pytest
import zstandard as zstd

from wcd_pipeline import dedup, extract
from wcd_schema import parquet as layout
from wcd_schema.ids import instance_id

from synthetic_bundle import render  # noqa: E402  (tests dir is on sys.path via rootdir conftest)

REPO_ROOT = Path(__file__).resolve().parents[3]
GOLDEN = REPO_ROOT / "fixtures" / "golden" / "synthetic"
DOMAIN = "en.wikipedia.org"
STAGING_TABLES = ("citation_instances", "containers", "domains", "ncwr", "normalized_citations",
                  "template_data", "web_resources", "wiki_templates")
TIER_DATASETS = ("citation_history", "named_ref_uses", "revision_signals")


@pytest.fixture(scope="module")
def run(tmp_path_factory) -> dict:
    base = tmp_path_factory.mktemp("stage1")
    bundle = base / "synthetic.mwrev.zst"
    bundle.write_bytes(zstd.ZstdCompressor(level=3).compress(render()))
    staging = base / "staging"
    parquet = base / "parquet"
    extract.main([str(bundle), "-o", str(staging / "synthetic"), "--domain", DOMAIN, "--parquet-dir", str(parquet)])
    dedup.main(["-d", str(staging)])
    out = {"deduped": staging / "deduped", "parquet": parquet, "staging": staging / "synthetic"}
    if os.environ.get("UPDATE_GOLDEN"):
        GOLDEN.mkdir(parents=True, exist_ok=True)
        for t in STAGING_TABLES:
            shutil.copy(out["deduped"] / f"{t}.parquet", GOLDEN / f"{t}.parquet")
        con = duckdb.connect()
        for ds in TIER_DATASETS:
            con.execute(f"COPY (SELECT * FROM read_parquet('{layout.dataset_glob(parquet, ds)}', hive_partitioning=true) "
                        f"ORDER BY ALL) TO '{GOLDEN / (ds + '.parquet')}' (FORMAT PARQUET)")
    return out


def _assert_same(con, a_sql: str, b_sql: str, what: str):
    n_a = con.execute(f"select count(*) from ({a_sql})").fetchone()[0]
    n_b = con.execute(f"select count(*) from ({b_sql})").fetchone()[0]
    only_a = con.execute(f"{a_sql} except {b_sql}").fetchall()
    only_b = con.execute(f"{b_sql} except {a_sql}").fetchall()
    assert n_a == n_b and not only_a and not only_b, (
        f"{what}: golden {n_a} rows, produced {n_b}; only in golden: {only_a[:5]}; only in output: {only_b[:5]}")


@pytest.mark.parametrize("table", STAGING_TABLES)
def test_staging_table_matches_golden(run: dict, table: str):
    golden = GOLDEN / f"{table}.parquet"
    assert golden.exists(), f"missing golden file {golden}; run with UPDATE_GOLDEN=1"
    con = duckdb.connect()
    _assert_same(con, f"select * from read_parquet('{golden}')",
                 f"select * from read_parquet('{run['deduped'] / (table + '.parquet')}')", table)


@pytest.mark.parametrize("dataset", TIER_DATASETS)
def test_tier_dataset_matches_golden(run: dict, dataset: str):
    golden = GOLDEN / f"{dataset}.parquet"
    assert golden.exists(), f"missing golden file {golden}; run with UPDATE_GOLDEN=1"
    con = duckdb.connect()
    _assert_same(con, f"select * from read_parquet('{golden}')",
                 f"select * from read_parquet('{layout.dataset_glob(run['parquet'], dataset)}', hive_partitioning=true)",
                 dataset)


def test_tier_layout_and_ids(run: dict):
    parquet = run["parquet"]
    # one file per shard in each touched bucket, named after the shard; no flat files left behind
    for ds in ("citation_history", "named_ref_uses"):
        files = sorted(p.name for p in layout.dataset_dir(parquet, ds).rglob("*.parquet"))
        assert files and all(f.startswith("synthetic-") for f in files), files
    assert layout.list_buckets(parquet, "citation_history", DOMAIN) == sorted({101 % 512, 202 % 512})
    assert (layout.partition_dir(parquet, "revision_signals", DOMAIN) / "synthetic.parquet").exists()
    assert not list(run["staging"].glob("*.flat.parquet"))

    con = duckdb.connect()
    # instance ids in history equal the Python function over the staged instances
    inst = con.execute(f"select citation_instance_id, domain, page_id, raw_sha1 from read_parquet('{run['deduped'] / 'citation_instances.parquet'}')").fetchall()
    assert inst and all(i == instance_id(d, p, r) for i, d, p, r in inst)
    hist_ids = {r[0] for r in con.execute(f"select distinct citation_instance_id from read_parquet('{layout.dataset_glob(parquet, 'citation_history')}')").fetchall()}
    assert hist_ids == {i for i, *_ in inst}
    # partition column = page_id % 512, every revision has a signals row, self-closing refs are reuses only
    assert con.execute(f"select count(*) from read_parquet('{layout.dataset_glob(parquet, 'citation_history')}', hive_partitioning=true) where page_bucket <> page_id % 512").fetchone()[0] == 0
    assert con.execute(f"select count(*) from read_parquet('{layout.dataset_glob(parquet, 'revision_signals')}')").fetchone()[0] == 5
    reuses = con.execute(f"select page_id, revision_id, ref_name from read_parquet('{layout.dataset_glob(parquet, 'named_ref_uses')}') order by all").fetchall()
    assert reuses == [(101, 1001, "diamond"), (101, 1002, "diamond"), (101, 1002, "diamond"),
                      (101, 1003, "diamond"), (101, 1003, "diamond"), (202, 2002, "web1")]
    assert con.execute(f"select count(*) from read_parquet('{run['deduped'] / 'citation_instances.parquet'}') "
                       "where reference_name is not null").fetchone()[0] == 3  # diamond ×2 definitions, web1
    types = dict(con.execute(f"select citation_type, count(*) from read_parquet('{run['deduped'] / 'normalized_citations.parquet'}') group by 1").fetchall())
    assert set(types) <= {"ref_tag", "template", "bare_url", "endnote", "shorthand", "external_link"}
    assert None not in types


def test_rerun_replaces_shard_files(run: dict, tmp_path):
    """Extracting the same bundle again leaves exactly one set of files."""
    parquet = run["parquet"]
    before = sorted(str(p) for p in layout.dataset_dir(parquet, "citation_history").rglob("*.parquet"))
    bundle = tmp_path / "synthetic.mwrev.zst"
    bundle.write_bytes(zstd.ZstdCompressor(level=3).compress(render()))
    extract.main([str(bundle), "-o", str(tmp_path / "staging"), "--domain", DOMAIN, "--parquet-dir", str(parquet)])
    after = sorted(str(p) for p in layout.dataset_dir(parquet, "citation_history").rglob("*.parquet"))
    assert after == before
    con = duckdb.connect()
    n = con.execute(f"select count(*) from read_parquet('{layout.dataset_glob(parquet, 'citation_history')}')").fetchone()[0]
    g = con.execute(f"select count(*) from read_parquet('{GOLDEN / 'citation_history.parquet'}')").fetchone()[0]
    assert n == g
