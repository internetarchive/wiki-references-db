"""The Stage-1 bundle reader reads what RevisionChest writes: frames with a
128 MiB long-distance window, and bundles made of several frames (`sync`)."""
from __future__ import annotations

import random

import zstandard as zstd

from wcd_pipeline.extract import get_revisions_from_mwrev_zst
from wcd_pipeline.synthetic import render

# RevisionChest's bundle settings (mwrev::WINDOW_LOG): level 3, 2^27 window, LDM.
LONG = zstd.ZstdCompressionParameters.from_level(3, window_log=27, enable_ldm=True)


def _noise(n: int, seed: int) -> str:
    rng = random.Random(seed)
    lines = ("".join(rng.choices("abcdefghijklmnopqrstuvwxyz", k=79)) for _ in range(n // 80))
    return "\n".join(lines)


def _bundle(revisions: list[tuple[int, str]]) -> bytes:
    out = []
    for rev_id, text in revisions:
        parent = rev_id - 1 if rev_id > 1 else ""
        out.append(f"# page_id=1 ns=0 rev_id={rev_id} parent_rev_id={parent} "
                   f"timestamp=2020-01-0{rev_id}T00:00:00Z user_id=0\n")
        out.extend(f" {line}\n" for line in text.split("\n"))
    return "".join(out).encode("utf-8")


def test_reads_a_long_window_bundle(tmp_path):
    # Three copies of a 3 MiB text: matched across more than a 2 MiB window.
    text = _noise(3 << 20, 1)
    raw = _bundle([(1, text), (2, text), (3, text)])
    data = zstd.ZstdCompressor(compression_params=LONG).compress(raw)
    assert len(data) < len(text)  # the repeats really were matched at long range
    p = tmp_path / "long.mwrev.zst"
    p.write_bytes(data)
    revs = list(get_revisions_from_mwrev_zst(p))
    assert [r["revision_id"] for r in revs] == [1, 2, 3]
    assert all(r["revision_text"] == text for r in revs)


def test_reads_every_frame_of_a_multi_frame_bundle(tmp_path):
    raw = render()
    cut = raw.index(b"\n#", len(raw) // 2) + 1  # split on a revision boundary
    one = tmp_path / "one.mwrev.zst"
    one.write_bytes(zstd.ZstdCompressor(level=3).compress(raw))
    two = tmp_path / "two.mwrev.zst"
    two.write_bytes(zstd.ZstdCompressor(level=3).compress(raw[:cut])
                    + zstd.ZstdCompressor(compression_params=LONG).compress(raw[cut:]))
    expected = list(get_revisions_from_mwrev_zst(one))
    assert len(expected) > 1
    assert list(get_revisions_from_mwrev_zst(two)) == expected
