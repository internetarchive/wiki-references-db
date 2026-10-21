"""split_bundle.py: pages stay whole, nothing is lost, and Stage 1 on the pieces
equals Stage 1 on the whole."""
from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pytest
import zstandard as zstd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from split_bundle import split  # noqa: E402

from wcd_pipeline import dedup, extract  # noqa: E402
from wcd_pipeline.extract import get_revisions_from_mwrev_zst  # noqa: E402
from wcd_pipeline.synthetic import REVISIONS, render  # noqa: E402
from wcd_schema import parquet as layout  # noqa: E402


@pytest.fixture
def bundle(tmp_path) -> Path:
    p = tmp_path / "synthetic.mwrev.zst"
    p.write_bytes(zstd.ZstdCompressor(level=3).compress(render()))
    return p


def test_split_keeps_pages_whole(bundle, tmp_path):
    parts = split(str(bundle), 2, str(tmp_path / "out"))
    assert [(p.endswith("synthetic-0.mwrev.zst"), pages, revs) for p, pages, revs in parts] == [(True, 1, 3), (False, 1, 2)]
    seen = []
    for p, _, _ in parts:
        revs = list(get_revisions_from_mwrev_zst(p))
        assert len({r["page_id"] for r in revs}) == 1          # one page per piece here
        seen += [(r["page_id"], r["revision_id"], r["revision_text"]) for r in revs]
    original = [(r["page_id"], r["revision_id"], r["revision_text"]) for r in get_revisions_from_mwrev_zst(str(bundle))]
    assert sorted(seen) == sorted(original) and len(original) == len(REVISIONS)


def test_extracting_pieces_equals_extracting_whole(bundle, tmp_path):
    runs = {}
    for name, bundles in (("whole", [bundle]), ("split", [Path(p) for p, _, _ in split(str(bundle), 2, str(tmp_path / "pieces"))])):
        staging, parquet = tmp_path / name / "staging", tmp_path / name / "parquet"
        for b in bundles:
            extract.main([str(b), "-o", str(staging / b.name.replace(".mwrev.zst", "")), "--domain", "en.wikipedia.org",
                          "--parquet-dir", str(parquet), "--batch-bytes", "1"])   # byte cap: one revision per batch
        dedup.main(["-d", str(staging)])
        con = duckdb.connect()
        runs[name] = {
            "instances": sorted(con.execute(f"SELECT citation_instance_id, page_id, raw_sha1, normalized_sha1 FROM read_parquet('{staging / 'deduped' / 'citation_instances.parquet'}')").fetchall()),
            "history": sorted(con.execute(f"SELECT citation_instance_id, revision_id, page_id FROM read_parquet('{layout.dataset_glob(parquet, 'citation_history')}')").fetchall()),
            "reuses": sorted(con.execute(f"SELECT page_id, revision_id, ref_name, offset_start FROM read_parquet('{layout.dataset_glob(parquet, 'named_ref_uses')}')").fetchall()),
            "signals": con.execute(f"SELECT count(*) FROM read_parquet('{layout.dataset_glob(parquet, 'revision_signals')}')").fetchone()[0],
        }
    assert runs["whole"] == runs["split"]
    # two pieces → two files per touched bucket dataset, named after their shard
    files = sorted(p.name for p in layout.dataset_dir(tmp_path / "split" / "parquet", "citation_history").rglob("*.parquet"))
    assert files == ["synthetic-0-0.parquet", "synthetic-1-0.parquet"]
