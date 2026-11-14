"""Split one ``.mwrev.zst`` bundle into N bundles on page boundaries.

Stage 1 parallelises across bundles (one process per bundle), so a single
large bundle — a RevisionChest build of one dump file, as small and mid-sized
wikis publish their whole history in, or a fixture built from one XML export —
runs on one core. This cuts it into N bundles, assigning whole pages
round-robin by page order, so N extract workers share it (``wcd setup`` does
this automatically when a dump has fewer files than ``--jobs``). Offsets
inside the bundle are not preserved (Stage 1 never reads them; they live in
the Stage-0 metadata, which still refers to the original bundle).

The pieces are written at plain zstd level 3, not with RevisionChest's
128 MiB long-distance window: they are disposable Stage-1 input, and N open
long-window compressors would cost N × ~150 MB. Any zstd reader reads both.
Lines are copied as bytes (no decoding), so the split runs at zstd speed.

    uv run python scripts/split_bundle.py fixture.xml.mwrev.zst -n 4 -o shards/
    # → shards/fixture.xml-0.mwrev.zst … fixture.xml-3.mwrev.zst
"""
from __future__ import annotations

import argparse
import io
import os
import sys

import zstandard as zstd


def piece_paths(path: str, n: int, out_dir: str) -> list[str]:
    stem = os.path.basename(path)
    stem = stem[:-len(".mwrev.zst")] if stem.endswith(".mwrev.zst") else stem
    return [os.path.join(out_dir, f"{stem}-{i}.mwrev.zst") for i in range(n)]


def split(path: str, n: int, out_dir: str, level: int = 3) -> list[tuple[str, int, int]]:
    """Write ``n`` pieces of ``path`` into ``out_dir``; returns (piece, pages, revisions) each."""
    os.makedirs(out_dir, exist_ok=True)
    outs = []
    for p in piece_paths(path, n, out_dir):
        fh = open(p, "wb")
        outs.append((p, fh, zstd.ZstdCompressor(level=level).stream_writer(fh), [0, 0]))  # pages, revisions
    current_page = None
    target = None
    k = -1
    with open(path, "rb") as fh:
        reader = zstd.ZstdDecompressor().stream_reader(fh, read_across_frames=True)
        for line in io.BufferedReader(reader, buffer_size=4 * 2**20):
            if line.startswith(b"#"):
                page_id = None
                for tok in line[1:].split():
                    if tok.startswith(b"page_id="):
                        page_id = tok[8:]
                        break
                if page_id != current_page:
                    current_page = page_id
                    k = (k + 1) % n
                    target = outs[k]
                    target[3][0] += 1
                target[3][1] += 1
            if target is not None:
                target[2].write(line)
    for p, fh, w, _ in outs:
        w.close()
        fh.close()
    return [(p, c[0], c[1]) for p, _, _, c in outs]


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("bundle")
    ap.add_argument("-n", type=int, default=2, help="number of output bundles (default 2)")
    ap.add_argument("-o", "--out-dir", default=".")
    args = ap.parse_args(argv)
    for p, pages, revs in split(args.bundle, args.n, args.out_dir):
        print(f"{p}: {pages} pages, {revs} revisions", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
