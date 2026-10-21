#!/usr/bin/env python3
"""Split one ``.mwrev.zst`` bundle into N bundles on page boundaries.

Stage 1 parallelises across bundles (one process per bundle), so a single
large bundle — a RevisionChest build of one dump file, or a fixture built from
one XML export — runs on one core. This cuts it into N bundles, assigning
whole pages round-robin by page order, so ``wcd extract-all`` can use N
workers. Offsets inside the bundle are not preserved (Stage 1 never reads
them; they live in the Stage-0 metadata, which still refers to the original
bundle).

    uv run python scripts/split_bundle.py fixture.xml.mwrev.zst -n 4 -o shards/
    # → shards/fixture.xml-0.mwrev.zst … fixture.xml-3.mwrev.zst
"""
from __future__ import annotations

import argparse
import io
import os
import sys

import zstandard as zstd


def split(path: str, n: int, out_dir: str, level: int = 3) -> list[tuple[str, int, int]]:
    stem = os.path.basename(path)
    stem = stem[:-len(".mwrev.zst")] if stem.endswith(".mwrev.zst") else stem
    os.makedirs(out_dir, exist_ok=True)
    outs = []
    for i in range(n):
        p = os.path.join(out_dir, f"{stem}-{i}.mwrev.zst")
        fh = open(p, "wb")
        outs.append((p, fh, zstd.ZstdCompressor(level=level).stream_writer(fh), [0, 0]))  # pages, revisions
    current_page = None
    target = None
    k = -1
    with open(path, "rb") as fh:
        reader = io.TextIOWrapper(zstd.ZstdDecompressor().stream_reader(fh), encoding="utf-8")
        for line in reader:
            if line.startswith("#"):
                page_id = None
                for tok in line[1:].split():
                    if tok.startswith("page_id="):
                        page_id = tok[8:]
                        break
                if page_id != current_page:
                    current_page = page_id
                    k = (k + 1) % n
                    target = outs[k]
                    target[3][0] += 1
                target[3][1] += 1
            if target is not None:
                target[2].write(line.encode("utf-8"))
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
