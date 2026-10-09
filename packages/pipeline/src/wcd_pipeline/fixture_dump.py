"""Build a small MediaWiki XML dump containing only selected pages.

Streams one or more ``pages-meta-history`` XML dumps (``.xml`` or
``.xml.bz2``; for ``.7z`` pipe ``7z x -so FILE`` into ``-``) and writes a
single, valid dump holding the full revision history of every page whose id
or title is listed in the page-list file. The output is what RevisionChest
consumes to produce the fixture ``.mwrev.zst`` bundle and metadata.

The page list has one entry per line: a numeric page id, or a title
(underscores are treated as spaces). Blank lines and ``#`` comments are
ignored. Which pages go on that list is decided by the sampling strategy
(see fixtures/README.md); this tool only does the filtering.

Usage:
    wcd fixture-dump -p pages.txt -o fixture.xml.bz2 DUMP [DUMP ...]
    7z x -so enwiki-…-pages-meta-history1.xml-p1p857.7z | wcd fixture-dump -p pages.txt -o out.xml.bz2 -
"""
from __future__ import annotations

import argparse
import bz2
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import IO, Iterable

MW_NS_PREFIX = "http://www.mediawiki.org/xml/export-"


def read_page_list(path: str) -> tuple[set[int], set[str]]:
    ids: set[int] = set()
    titles: set[str] = set()
    for raw in Path(path).read_text(encoding="utf-8").splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        if line.isdigit():
            ids.add(int(line))
        else:
            titles.add(line.replace("_", " "))
    return ids, titles


def _open_dump(spec: str) -> IO[bytes]:
    if spec == "-":
        return sys.stdin.buffer
    if spec.endswith(".bz2"):
        return bz2.open(spec, "rb")
    if spec.endswith(".7z"):
        raise SystemExit(f"{spec}: 7z is not read directly; use `7z x -so {spec} | wcd fixture-dump … -`")
    return open(spec, "rb")


def _open_out(spec: str) -> IO[bytes]:
    if spec == "-":
        return sys.stdout.buffer
    if spec.endswith(".bz2"):
        return bz2.open(spec, "wb")
    return open(spec, "wb")


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _ns_of(tag: str) -> str:
    return tag[1:].split("}", 1)[0] if tag.startswith("{") else ""


_KNOWN_PREFIXES = {
    "http://www.w3.org/2001/XMLSchema-instance": "xsi",
    "http://www.w3.org/XML/1998/namespace": "xml",   # implicit; never declared
}


def _start_tag(elem: ET.Element, ns: str) -> bytes:
    """Re-create the root's start tag (ElementTree only exposes it as parsed attributes)."""
    decls = [f' xmlns="{ns}"'] if ns else []
    attrs = []
    for key, value in elem.attrib.items():
        if key.startswith("{"):
            uri, local = key[1:].split("}", 1)
            prefix = _KNOWN_PREFIXES.get(uri, f"ns{len(decls)}")
            if prefix != "xml" and f' xmlns:{prefix}=' not in "".join(decls):
                decls.append(f' xmlns:{prefix}="{uri}"')
            attrs.append(f' {prefix}:{local}="{value}"')
        else:
            attrs.append(f' {key}="{value}"')
    return f"<{_local(elem.tag)}{''.join(decls)}{''.join(attrs)}>\n".encode("utf-8")


def _serialize(elem: ET.Element, ns: str) -> bytes:
    """Serialize a subtree without repeating the root's default-namespace declaration."""
    data = ET.tostring(elem, encoding="utf-8")
    if ns:
        data = data.replace(f' xmlns="{ns}"'.encode("utf-8"), b"", 1)
    return data


def filter_dumps(dumps: Iterable[str], ids: set[int], titles: set[str], out: IO[bytes],
                 log: IO[str] = sys.stderr) -> dict:
    """Write the filtered dump to ``out``; return {found_ids, found_titles, n_pages, n_revisions}."""
    found_ids: set[int] = set()
    found_titles: set[str] = set()
    n_pages = n_revisions = 0
    header_written = False
    ns = ""

    for spec in dumps:
        log.write(f"[fixture-dump] reading {spec}\n")
        fh = _open_dump(spec)
        root = None
        for event, elem in ET.iterparse(fh, events=("start", "end")):
            if event == "start":
                if root is None:
                    root = elem
                    ns = _ns_of(elem.tag)
                    ET.register_namespace("", ns)
                    if not header_written:
                        out.write(b'<?xml version="1.0" encoding="utf-8"?>\n')
                        out.write(_start_tag(elem, ns))
                continue
            name = _local(elem.tag)
            if name == "siteinfo":
                if not header_written:
                    out.write(_serialize(elem, ns))
                    out.write(b"\n")
                    header_written = True
                root.remove(elem)
            elif name == "page":
                pid_el = elem.find(f"{{{ns}}}id") if ns else elem.find("id")
                title_el = elem.find(f"{{{ns}}}title") if ns else elem.find("title")
                pid = int(pid_el.text) if pid_el is not None and pid_el.text else None
                title = (title_el.text or "") if title_el is not None else ""
                if (pid in ids) or (title in titles):
                    out.write(_serialize(elem, ns))
                    out.write(b"\n")
                    n_pages += 1
                    n_revisions += sum(1 for c in elem if _local(c.tag) == "revision")
                    if pid is not None:
                        found_ids.add(pid)
                    found_titles.add(title)
                root.remove(elem)
        if fh is not sys.stdin.buffer:
            fh.close()

    out.write(b"</mediawiki>\n")
    return {"found_ids": found_ids, "found_titles": found_titles,
            "n_pages": n_pages, "n_revisions": n_revisions}


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description="Filter MediaWiki XML history dumps down to a page list")
    ap.add_argument("dumps", nargs="+", help=".xml / .xml.bz2 dump files, or - for stdin")
    ap.add_argument("-p", "--pages", required=True, help="page list: one page id or title per line")
    ap.add_argument("-o", "--output", required=True, help="output dump (.xml or .xml.bz2, or -)")
    return ap.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    ids, titles = read_page_list(args.pages)
    with _open_out(args.output) as out:
        stats = filter_dumps(args.dumps, ids, titles, out)
    missing_ids = sorted(ids - stats["found_ids"])
    missing_titles = sorted(titles - stats["found_titles"])
    print(f"[fixture-dump] wrote {stats['n_pages']} pages / {stats['n_revisions']} revisions to {args.output}",
          file=sys.stderr)
    if missing_ids or missing_titles:
        print(f"[fixture-dump] not found: {len(missing_ids)} ids, {len(missing_titles)} titles "
              f"({', '.join(map(str, missing_ids[:10]))}{', ' if missing_ids and missing_titles else ''}"
              f"{', '.join(missing_titles[:10])}{' …' if len(missing_ids) + len(missing_titles) > 20 else ''})",
              file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
