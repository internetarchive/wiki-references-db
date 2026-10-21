"""A tiny synthetic .mwrev.zst bundle in RevisionChest's line format, and the
whole pipeline run on it (``build_database``) — the fixture the API tests
and the pipeline's own integration test share.

The bundle is deterministic and exercises every reference kind the extractor
knows about (<ref> with a citation template, prose <ref>, bare-URL <ref>,
named-ref definition and self-closing reuse, a bullet-block <ref>, a standalone
{{sfn}}, reference-section list items, and loose external links) across a
short revision history that adds, edits, removes and redefines references.

Used by the pipeline test-suite and as the Gate-R regression input; it is NOT
a substitute for the sampled test-article fixture (see fixtures/README.md).

Usage:
    python -m wcd_pipeline.synthetic OUT.mwrev.zst
"""
from __future__ import annotations

import sys
from pathlib import Path

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

# Third revision removes the prose ref and the bare-url ref, and redefines
# the "diamond" named ref (a new edition) so its ref_name_links range splits.
REV_A3 = REV_A2.replace(
    '<ref>Fischer, Steven Roger (1995). "Preliminary Evidence". \'\'Journal of the Polynesian Society\'\' 104: 303–21.</ref>',
    "",
).replace("<ref>[https://example.org/visits Visits]</ref>", "").replace(
    "|title=Collapse |publisher=Viking |isbn=978-0-14-303655-5}}</ref>\nIt was",
    "|title=Collapse |edition=2nd |publisher=Viking |isbn=978-0-14-303655-5}}</ref>\nIt was",
)

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
    # (revision ids 2201/2202 stay clear of the mwdump fixture's 2001)
    (PAGE_B, 0, 2201, None, "2006-02-02T00:00:00Z", REV_B1),
    (PAGE_B, 0, 2202, 2201, "2007-02-02T00:00:00Z", REV_B2),
]


PAGE_TITLES = {PAGE_A: ("Example Island", 0), PAGE_B: ("Second Page", 0)}


def metadata_rows(bundle_file: str = "synthetic.mwrev.zst") -> list[dict]:
    """What RevisionChest's ``--parquet`` metadata would say about this bundle
    (page rows + revision rows; offsets are not meaningful here)."""
    rows = [{"type": "page", "page_id": pid, "ns": ns, "title": title, "rev_id": None, "parent_rev_id": None,
             "file_path": None, "offset_begin": None, "length": None, "timestamp": None}
            for pid, (title, ns) in PAGE_TITLES.items()]
    for page_id, ns, rev_id, parent, ts, _text in REVISIONS:
        rows.append({"type": "revision", "page_id": page_id, "ns": None, "title": None, "rev_id": rev_id,
                     "parent_rev_id": parent, "file_path": bundle_file, "offset_begin": 0, "length": 0, "timestamp": ts})
    return rows


def write_metadata(path: str) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq
    schema = pa.schema([("type", pa.string()), ("page_id", pa.int64()), ("ns", pa.int64()), ("title", pa.string()),
                        ("rev_id", pa.int64()), ("parent_rev_id", pa.int64()), ("file_path", pa.string()),
                        ("offset_begin", pa.int64()), ("length", pa.int64()), ("timestamp", pa.string())])
    pq.write_table(pa.Table.from_pylist(metadata_rows(), schema=schema), path)


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


def build_database(dsn: str, work: str | Path, domain: str = "en.wikipedia.org", *, fresh_schema: bool = True) -> dict:
    """Run Stages 0–8 on the synthetic bundle against ``dsn`` (its ``public``
    schema is dropped first when ``fresh_schema``), with the Parquet tier under
    ``<work>/parquet``. Returns the paths and a few counts. Everything runs
    in-process; takes a few seconds."""
    import os

    from sqlalchemy import create_engine, text

    from . import dedup, extract, init_db, load, materialize, stage0, stage4, stage5, stage6, stage7, stage8
    from .config import database_url

    work = Path(work)
    work.mkdir(parents=True, exist_ok=True)
    os.environ["DATABASE_URL"] = dsn
    if fresh_schema:
        engine = create_engine(database_url())
        with engine.begin() as con:
            con.execute(text("DROP SCHEMA public CASCADE"))
            con.execute(text("CREATE SCHEMA public"))
        engine.dispose()
    init_db.main([])
    parquet = work / "parquet"
    meta = work / "meta"
    meta.mkdir(exist_ok=True)
    write_metadata(str(meta / "synthetic.parquet"))
    assert stage0.main(["--domain", domain, "--metadata", str(meta), "--parquet-dir", str(parquet)]) == 0
    bundle = work / "synthetic.mwrev.zst"
    bundle.write_bytes(zstd.ZstdCompressor(level=3).compress(render()))
    staging = work / "staging"
    extract.main([str(bundle), "-o", str(staging / "synthetic"), "--domain", domain, "--parquet-dir", str(parquet)])
    dedup.main(["-d", str(staging)])
    load.main(["-d", str(staging)])
    pg_dsn = "postgresql://" + dsn.split("://", 1)[1]
    stage4.run(pg_dsn, domain)
    materialize.export_citation_instances_denorm(pg_dsn, domain, str(parquet))
    stage5.run(pg_dsn, domain, str(parquet))
    assert stage6.main(["--domain", domain, "--parquet-dir", str(parquet), "--jobs", "1"]) == 0
    assert stage7.main(["--domain", domain, "--parquet-dir", str(parquet)]) == 0
    assert stage8.main(["--domain", domain, "--parquet-dir", str(parquet), "--jobs", "1"]) == 0
    return {"parquet_dir": str(parquet), "staging_dir": str(staging), "domain": domain,
            "pages": sorted(PAGE_TITLES), "revisions": [r[2] for r in REVISIONS]}


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
