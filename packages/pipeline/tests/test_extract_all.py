"""extract-all: a shard whose worker fails is not marked DONE and fails the run;
a re-run clears it and redoes it."""
from __future__ import annotations

import os
from pathlib import Path

import pytest
import zstandard as zstd

from wcd_pipeline import extract_all
from wcd_pipeline.synthetic import render


def test_failed_shard_is_not_done(tmp_path, capsys):
    bundles = tmp_path / "bundles"
    bundles.mkdir()
    (bundles / "good.mwrev.zst").write_bytes(zstd.ZstdCompressor(level=3).compress(render()))
    (bundles / "bad.mwrev.zst").write_bytes(b"this is not a zstd stream")
    staging, parquet = tmp_path / "staging", tmp_path / "parquet"
    extract_all.FAILED_SHARDS.clear()
    with pytest.raises(SystemExit) as exc:
        extract_all.main(["-d", str(bundles), "-o", str(staging), "--domain", "en.wikipedia.org",
                          "--parquet-dir", str(parquet), "--jobs", "2", "--metrics-interval", "0.2"])
    assert exc.value.code == 1
    assert (staging / "good" / "DONE.txt").exists()
    assert not (staging / "bad" / "DONE.txt").exists() and (staging / "bad" / "FAILED.txt").exists()
    assert "[FAILED]" in capsys.readouterr().out
    # a re-run skips the good shard and retries the bad one, clearing its marker first
    (bundles / "bad.mwrev.zst").write_bytes(zstd.ZstdCompressor(level=3).compress(render()))
    extract_all.FAILED_SHARDS.clear()
    extract_all.main(["-d", str(bundles), "-o", str(staging), "--domain", "en.wikipedia.org",
                      "--parquet-dir", str(parquet), "--jobs", "2", "--metrics-interval", "0.2"])
    out = capsys.readouterr().out
    assert "[skip]" in out and "good.mwrev.zst" in out.split("[skip]")[1].split("\n")[0]
    assert (staging / "bad" / "DONE.txt").exists() and not (staging / "bad" / "FAILED.txt").exists()
    assert os.path.exists(parquet / "revision_signals" / "domain=en.wikipedia.org" / "bad.parquet")
