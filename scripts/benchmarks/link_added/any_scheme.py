#!/usr/bin/env python3
"""WCD's added times with IABot's notion of "the same link": the scheme (http/https, any case) ignored.

    any_scheme.py LOOKUP.json OUT.json [--domain-id 1]

Same lookup as ``lookup.py``, with URLs grouped by their text after the scheme.
Writes {page_id: {url: [first_ts, current_ts, n_runs]}} for each current link of
LOOKUP.json (``lookup.py``'s output). Untimed.
"""
import argparse
import json
import os
import re
import sys

import duckdb

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lookup as la  # noqa: E402

SCHEME = re.compile(r"^https?://", re.I)


def key(url: str) -> str:
    return SCHEME.sub("", url)


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("lookup_json")
    ap.add_argument("out")
    ap.add_argument("--domain-id", type=int, default=1)
    args = ap.parse_args()
    data = json.load(open(args.lookup_json))
    pg = la.pg_connect()
    out = {}
    for a in data:
        if not a["n_links"]:
            continue
        p = a["page_id"]
        with pg.cursor() as cur:
            cur.execute(la.URL_SQL, (args.domain_id, p))
            mapping = cur.fetchall()
        con = duckdb.connect(config={"threads": 8})
        la.bind(con, p, mapping, pruned=True, key=key)
        rows = con.execute(la.LINK_SQL.replace("$page", str(p))).fetchall()
        con.close()
        by_key = {r[0]: (str(r[3]), str(r[5]), r[1]) for r in rows}
        out[p] = {link["url"]: by_key.get(key(link["url"])) for link in a["links"]}
    json.dump(out, open(args.out, "w"), indent=1)


if __name__ == "__main__":
    main()
