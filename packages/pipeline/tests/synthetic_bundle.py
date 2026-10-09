#!/usr/bin/env python3
"""Write a tiny synthetic .mwrev.zst bundle in RevisionChest's line format.

The bundle is deterministic and exercises every reference kind the extractor
knows about (<ref> with a citation template, prose <ref>, bare-URL <ref>,
named-ref definition and self-closing reuse, a bullet-block <ref>, a standalone
{{sfn}}, reference-section list items, and loose external links) across a
short revision history that adds, edits and removes references.

Used by the pipeline test-suite and as the Gate-R regression input; it is NOT
a substitute for the sampled test-article fixture (see fixtures/README.md).

Usage:
    python packages/pipeline/tests/synthetic_bundle.py OUT.mwrev.zst
"""
from __future__ import annotations

import sys

import zstandard as zstd

PAGE_A = 101
PAGE_B = 202

REV_A1 = """'''Example Island''' is an island.<ref name="diamond">{{cite book |last=Diamond |first=Jared |year=2005 |title=Collapse |publisher=Viking |isbn=978-0-14-303655-5}}</ref>
It was settled early.<ref>Fischer, Steven Roger (1995). "Preliminary Evidence". ''Journal of the Polynesian Society'' 104: 303–21.</ref>

== History ==
The island was visited in 1722.<ref name="diamond" /> Later visits followed.{{sfn|Routledge|1919|p=12}}

== References ==
{{reflist}}

== Bibliography ==
* {{cite book |last=Routledge |first=Katherine |year=1919 |title=The Mystery of Easter Island |location=London}}
* Métraux, Alfred (1940). ''Ethnology of Easter Island''. Honolulu.

== External links ==
* [https://example.org/island Official site]
"""

REV_A2 = REV_A1.replace(
    "Later visits followed.{{sfn|Routledge|1919|p=12}}",
    "Later visits followed.{{sfn|Routledge|1919|p=12}}<ref>[https://example.org/visits Visits]</ref>",
).replace(
    "== Bibliography ==",
    "== Trade ==\nTrade is documented.<ref name=\"diamond\" /><ref>\n* {{cite journal |last=Lee |first=A |year=2001 |title=Trade A |journal=J1}}\n* {{cite journal |last=Lee |first=B |year=2002 |title=Trade B |journal=J2}}\n</ref>\n\n== Bibliography ==",
)

# Third revision removes the prose ref and the bare-url ref.
REV_A3 = REV_A2.replace(
    '<ref>Fischer, Steven Roger (1995). "Preliminary Evidence". \'\'Journal of the Polynesian Society\'\' 104: 303–21.</ref>',
    "",
).replace("<ref>[https://example.org/visits Visits]</ref>", "")

REV_B1 = """'''Second Page''' cites the same book.<ref>{{cite book |last=Diamond |first=Jared |year=2005 |title=Collapse |publisher=Viking |isbn=978-0-14-303655-5}}</ref>
And a web page.<ref name="web1">{{cite web |url=https://example.org/page |title=A page |access-date=2020-01-01}}</ref>

== Notes ==
{{reflist}}
"""

REV_B2 = REV_B1.replace("And a web page.", "And a web page.<ref name=\"web1\" /> Again.")

REVISIONS = [
    # page_id, ns, rev_id, parent_rev_id, timestamp, text
    (PAGE_A, 0, 1001, None, "2004-01-01T00:00:00Z", REV_A1),
    (PAGE_A, 0, 1002, 1001, "2005-06-15T12:00:00Z", REV_A2),
    (PAGE_A, 0, 1003, 1002, "2010-03-03T03:03:03Z", REV_A3),
    (PAGE_B, 0, 2001, None, "2006-02-02T00:00:00Z", REV_B1),
    (PAGE_B, 0, 2002, 2001, "2007-02-02T00:00:00Z", REV_B2),
]


def render() -> bytes:
    out = []
    for page_id, ns, rev_id, parent, ts, text in REVISIONS:
        parent_s = "" if parent is None else str(parent)
        out.append(
            f"# page_id={page_id} ns={ns} rev_id={rev_id} parent_rev_id={parent_s} "
            f"timestamp={ts} user_id=1\n"
        )
        for line in text.split("\n"):
            out.append(" " + line + "\n")
    return "".join(out).encode("utf-8")


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(__doc__, file=sys.stderr)
        return 2
    data = zstd.ZstdCompressor(level=3).compress(render())
    with open(argv[1], "wb") as fh:
        fh.write(data)
    print(f"wrote {argv[1]} ({len(data)} bytes, {len(REVISIONS)} revisions)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
