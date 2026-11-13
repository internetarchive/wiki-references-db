"""Copy whole page histories out of one .mwrev.zst bundle, by decompressed byte range.

A page's revisions are contiguous in its bundle; its range runs from the start of
its first header line (the line ending just before its first offset_begin) to the
end of its last revision's text (max offset_begin + length). Writes one raw
(uncompressed) part per output group: OUT_DIR/<group>/<bundle-stem>.part

Usage: cut_pages.py BUNDLE RANGES.tsv OUT_DIR
  RANGES.tsv: group <TAB> page_id <TAB> first_offset <TAB> end_offset  (this bundle's pages)
"""
import os, sys
import zstandard as zstd

bundle, ranges_path, out_dir = sys.argv[1:4]
ranges = []
for line in open(ranges_path):
    g, pid, first, end = line.rstrip("\n").split("\t")
    ranges.append((int(first), int(end), g, pid))
ranges.sort()
stem = os.path.basename(bundle).removesuffix(".mwrev.zst")
outs = {}
def out(g):
    if g not in outs:
        os.makedirs(os.path.join(out_dir, g), exist_ok=True)
        outs[g] = open(os.path.join(out_dir, g, stem + ".part"), "ab")
    return outs[g]

CHUNK = 16 << 20
TAIL = 1 << 20          # header lines are far shorter than this
pos = 0                 # stream offset of buf[0]
buf = b""
i = 0                   # next range
active = None           # (end, file) while copying a range
with open(bundle, "rb") as fh:
    reader = zstd.ZstdDecompressor().stream_reader(fh, read_across_frames=True)
    while i < len(ranges) or active:
        chunk = reader.read(CHUNK)
        if not chunk:
            break
        buf += chunk
        while True:
            if active:
                end, f = active
                take = min(end, pos + len(buf)) - pos
                # copy from the cursor
                f.write(buf[cur - pos: take]) if take > cur - pos else None
                cur = pos + take
                if cur >= end:
                    active = None
                    i += 1
                    continue
                break
            if i >= len(ranges):
                break
            first, end, g, pid = ranges[i]
            if first > pos + len(buf):
                break
            # header line: from the newline before (first - 1) to first
            nl = buf.rfind(b"\n", 0, first - 1 - pos)
            start = pos + nl + 1
            assert buf[start - pos:start - pos + 9] == b"# page_id", (pid, buf[start - pos:start - pos + 40])
            cur = start
            active = (end, out(g))
        # keep only what may still be needed
        keep_from = cur if active else max(pos, pos + len(buf) - TAIL)
        buf = buf[keep_from - pos:]
        pos = keep_from
for f in outs.values():
    f.close()
done = i
print(f"{stem}: {done}/{len(ranges)} pages cut", file=sys.stderr)
sys.exit(0 if done == len(ranges) else 1)
