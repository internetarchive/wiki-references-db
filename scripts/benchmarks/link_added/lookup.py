#!/usr/bin/env python3
"""When was each link of an article added? The WCD lookup, and its timing benchmark.

    lookup.py SAMPLE.csv OUT.json [--domain-id 1] [--drop-cache-cmd CMD]

Needs ``DATABASE_URL`` and ``PARQUET_DIR`` (as for the pipeline). SAMPLE.csv has a
``page_id`` column (``pick_sample.sql`` writes one); other columns are copied to
the output.

For one page: the URLs of all its citation instances ever (Postgres:
``citation_instances`` -> ``normalized_citation_web_resources`` ->
``web_resources``), each instance's presence per revision (Parquet
``citation_history``, joined to the page's ``revisions`` in (timestamp, id)
order), and per URL present in the latest revision:

* ``first_rev`` / ``first_ts``: the first revision with the URL;
* ``current_rev`` / ``current_ts``: the start of its current unbroken run (the
  revision that last put it back, when it was removed in between);
* ``n_runs``: how many separate runs it has had.

Each page is timed three ways: ``pruned`` reads only the ``citation_history``
file whose dump page range holds the page, cold (after ``--drop-cache-cmd``,
e.g. a script that writes 3 to /proc/sys/vm/drop_caches) and warm; ``bucket``
reads the whole bucket, as the API's ``bind_bucket_views`` does today. The
three must give the same links; the output records whether they did.

Prototype for SCA-388 (an API endpoint); results of the enwiki run are in the
implementation plan, §25.
"""
import argparse
import csv
import glob
import json
import os
import re
import subprocess
import time

import duckdb
import psycopg2
import pyarrow as pa

DOMAIN = "en.wikipedia.org"
# citation_history files are named after their dump file, which names its page range
RANGE = re.compile(r"-p(\d+)p(\d+)-\d+\.parquet$")

URL_SQL = """
SELECT ci.id, w.url FROM citation_instances ci
JOIN normalized_citation_web_resources l ON l.normalized_id = ci.normalized_id
JOIN web_resources w ON w.id = l.web_resource_id
WHERE ci.domain_id = %s AND ci.page_id = %s
"""

# Per instance, its runs of consecutive revisions; mapped to URLs and merged per URL
# (overlapping or adjacent intervals join). Same rows as a per-(url, revision) DISTINCT
# over the history join, 2-6x faster on enwiki's largest pages.
LINK_SQL = """
WITH revs AS (
    SELECT revision_id, revision_timestamp,
           row_number() OVER (ORDER BY revision_timestamp, revision_id) AS seq
    FROM rv WHERE page_id = $page
),
iseq AS (
    SELECT h.citation_instance_id AS iid, r.seq
    FROM ch h JOIN revs r ON r.revision_id = h.revision_id
    WHERE h.page_id = $page
),
iv AS (
    SELECT iid, min(seq) AS s, max(seq) AS e
    FROM (SELECT iid, seq, seq - row_number() OVER (PARTITION BY iid ORDER BY seq) AS g FROM iseq)
    GROUP BY iid, g
),
uv AS (
    SELECT DISTINCT m.url, iv.s, iv.e FROM iv JOIN m ON m.instance_id = iv.iid
),
marked AS (
    SELECT url, s, e,
           CASE WHEN s > coalesce(max(e) OVER (PARTITION BY url ORDER BY s, e
                                               ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), -1) + 1
                THEN 1 ELSE 0 END AS new_run
    FROM uv
),
runs AS (
    SELECT url, min(s) AS s, max(e) AS e
    FROM (SELECT *, sum(new_run) OVER (PARTITION BY url ORDER BY s, e ROWS UNBOUNDED PRECEDING) AS run FROM marked)
    GROUP BY url, run
),
per_url AS (
    SELECT url, count(*) AS n_runs, min(s) AS first_s, max(s) AS cur_s, max(e) AS last_seq FROM runs GROUP BY url
)
SELECT p.url, p.n_runs, f.revision_id AS first_rev, f.revision_timestamp AS first_ts,
       c.revision_id AS current_rev, c.revision_timestamp AS current_ts,
       p.last_seq, (SELECT max(seq) FROM revs) AS latest_seq, (SELECT count(*) FROM revs) AS n_revs
FROM per_url p JOIN revs f ON f.seq = p.first_s JOIN revs c ON c.seq = p.cur_s
ORDER BY first_ts, url
"""


def parquet_dir() -> str:
    return os.environ["PARQUET_DIR"]


def pg_connect():
    return psycopg2.connect(os.environ["DATABASE_URL"].replace("postgresql+psycopg2://", "postgresql://"))


def ch_files(page_id: int, pruned: bool, domain: str = DOMAIN) -> list[str]:
    d = f"{parquet_dir()}/citation_history/domain={domain}/page_bucket={page_id % 512}"
    files = sorted(glob.glob(f"{d}/*.parquet"))
    if not pruned:
        return files
    out = []
    for f in files:
        m = RANGE.search(f)
        if m is None or int(m.group(1)) <= page_id <= int(m.group(2)):
            out.append(f)
    return out


def bind(con, page_id: int, mapping, pruned: bool, domain: str = DOMAIN, key=lambda u: u) -> None:
    """Views ch / rv over the page's partitions, and the instance -> URL map as m."""
    con.execute(f"CREATE VIEW ch AS SELECT * FROM read_parquet({ch_files(page_id, pruned, domain)!r})")
    con.execute(f"CREATE VIEW rv AS SELECT * FROM read_parquet("
                f"'{parquet_dir()}/revisions/domain={domain}/page_bucket={page_id % 512}/*.parquet')")
    # an Arrow table: executemany into DuckDB took ~4 s for India's 12,745 rows
    con.register("m", pa.table({"instance_id": pa.array([r[0] for r in mapping], pa.int64()),
                                "url": pa.array([key(r[1]) for r in mapping], pa.string())}))


def lookup(pg, page_id: int, pruned: bool, domain_id: int = 1, threads: int = 8) -> dict:
    t0 = time.perf_counter()
    with pg.cursor() as cur:
        cur.execute(URL_SQL, (domain_id, page_id))
        mapping = cur.fetchall()
    t_pg = time.perf_counter() - t0
    con = duckdb.connect(config={"threads": threads})
    try:
        bind(con, page_id, mapping, pruned)
        n_files = len(ch_files(page_id, pruned))
        rows = con.execute(LINK_SQL.replace("$page", str(int(page_id)))).fetchall()
    finally:
        con.close()
    t_total = time.perf_counter() - t0
    links = [{"url": url, "first_rev": fr, "first_ts": str(fts), "current_rev": cr, "current_ts": str(cts),
              "n_runs": n_runs}
             for url, n_runs, fr, fts, cr, cts, last_seq, latest_seq, _ in rows if last_seq == latest_seq]
    return {"links": links, "n_urls_ever": len(rows), "n_files": n_files, "t_pg": round(t_pg, 4),
            "t_total": round(t_total, 4)}


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("sample")
    ap.add_argument("out")
    ap.add_argument("--domain-id", type=int, default=1)
    ap.add_argument("--drop-cache-cmd", help="command run before each page's cold lookup (none: cold = first run)")
    ap.add_argument("--warmup-page", type=int, default=736, help="page looked up once first, not recorded")
    args = ap.parse_args()
    pg = pg_connect()
    pg.autocommit = True
    sample = list(csv.DictReader(open(args.sample)))
    with pg.cursor() as cur:
        cur.execute("SELECT page_id, page_title FROM page_metadata WHERE domain_id = %s AND page_id = ANY(%s)",
                    (args.domain_id, [int(r["page_id"]) for r in sample]))
        titles = dict(cur.fetchall())
    lookup(pg, args.warmup_page, pruned=True, domain_id=args.domain_id)   # imports, DuckDB start-up
    results = []
    for row in sample:
        p = int(row["page_id"])
        if args.drop_cache_cmd:
            subprocess.run(args.drop_cache_cmd, shell=True, check=True)
        cold = lookup(pg, p, pruned=True, domain_id=args.domain_id)
        warm = lookup(pg, p, pruned=True, domain_id=args.domain_id)
        bucket = lookup(pg, p, pruned=False, domain_id=args.domain_id)
        same = cold["links"] == warm["links"] == bucket["links"]
        rec = {**{k: int(v) for k, v in row.items()}, "title": titles.get(p), "links": cold["links"],
               "n_links": len(cold["links"]), "n_urls_ever": cold["n_urls_ever"],
               "t_cold": cold["t_total"], "t_cold_pg": cold["t_pg"], "t_warm": warm["t_total"],
               "t_warm_pg": warm["t_pg"], "t_bucket": bucket["t_total"], "files_pruned": cold["n_files"],
               "files_bucket": bucket["n_files"], "pruned_equals_bucket": same}
        results.append(rec)
        print(f"{p:>9} {str(titles.get(p))[:40]:40} links {rec['n_links']:>4} cold {rec['t_cold']:.3f}s "
              f"warm {rec['t_warm']:.3f}s bucket {rec['t_bucket']:.3f}s same {same}", flush=True)
    json.dump(results, open(args.out, "w"), indent=1, default=str)


if __name__ == "__main__":
    main()
