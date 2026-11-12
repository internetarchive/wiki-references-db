"""Stage 6d at full-wiki scale: ``wcd_semantic.match_global`` without the per-row Python.

The vendored ``match_global`` holds every merged source of a domain in a pandas
frame, keys a string union-find and a dict per row on them, and loops over every
component in Python: on enwiki ~131M merged sources, far past the build VM's RAM.
This module computes the same two outputs (``global_sources_stage`` rows without
the score, which 7f computes, and ``global_source_map``) as follows:

* DuckDB numbers the sources in the vendored order (``page_id, merged_source_id``)
  and returns, per hard identifier, each value's members in that order, values
  sorted as pandas' ``groupby`` sorts them (both compare strings by code point).
* The unions are replayed in the vendored order on an integer union-find with
  the same rank / path-halving / edge-info rules as ``wcd_semantic.unionfind``,
  so every component and its (match_type, confidence) come out the same.
* The author+year pass groups the ~14 % fuzzy candidates in Python by the same
  key, scores each group's pairs with rapidfuzz's ``cdist`` (the same C++ scorer
  ``fuzz.token_sort_ratio`` calls, float64) in row blocks, and unions the pairs
  over the threshold in the vendored ``i < j`` order.
* The unions (members' roots and each root's edge info) are saved, so a failure
  after them can re-run with ``--reuse-unions``.
* DuckDB builds the components: sha1 of the sorted members, the canonical member
  (identifier tier, citations, first in order), the sorted page ids as JSON, the
  citation total, and the canonical's ``source_type`` as ``run_global`` assigns
  it. A singleton (most sources) is its own row, with no aggregation; the
  multi-member components' member ids and pages are streamed through Python in
  sorted order, since DuckDB's ordered/list aggregates over every group ran out
  of memory on enwiki.

``test_global_match`` holds it to the vendored function on the fixture and on
random inputs.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import time

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from rapidfuzz import fuzz, process

from wcd_schema import parquet as layout
from wcd_semantic import match_global as vendored

from .config import duckdb_connect

HARD_SIGNALS = (("doi", "extracted_doi", 1.00), ("isbn", "extracted_isbn", 0.95), ("pmid", "extracted_pmid", 0.95),
                ("pmc", "extracted_pmc", 0.95), ("arxiv", "extracted_arxiv", 0.95),
                ("wikidata_qid", "extracted_wikidata_qid", 0.95))
# rows of a fuzzy group scored per cdist call (memory: rows × group size float64)
BLOCK_ROWS = int(os.getenv("MATCH_GLOBAL_BLOCK_ROWS", "2048"))


def log(msg: str) -> None:
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} [match 6d] {msg}", flush=True)


class IntUnionFind:
    """``wcd_semantic.unionfind.UnionFind`` on 0..n-1: the same rank rule, path
    halving and edge-info update, so the same sequence of unions gives the same
    roots and the same (match_type, confidence) per root."""

    def __init__(self, n: int):
        self.parent = np.arange(n, dtype=np.int64)
        self.rank = np.zeros(n, dtype=np.int16)
        self.edge_info: dict[int, tuple[str, float]] = {}
        self.touched: set[int] = set()

    def find(self, x: int) -> int:
        parent = self.parent
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = int(parent[x])
        return x

    def union(self, x: int, y: int, match_type: str, confidence: float) -> None:
        rx, ry = self.find(x), self.find(y)
        if rx == ry:
            return
        if self.rank[rx] < self.rank[ry]:
            rx, ry = ry, rx
        self.parent[ry] = rx
        if self.rank[rx] == self.rank[ry]:
            self.rank[rx] += 1
        current = self.edge_info.get(rx, (match_type, 0))
        if confidence > current[1]:
            self.edge_info[rx] = (match_type, confidence)
        self.touched.add(x)
        self.touched.add(y)


def _hard_pass(con, uf: IntUnionFind, columns: set[str]) -> dict[str, int]:
    counts = {}
    for sig, col, conf in HARD_SIGNALS:
        if col not in columns:
            continue
        n_merged = 0
        # groups in value order, members in source order (pandas groupby on the frame)
        for (members,) in con.execute(f"SELECT list(idx ORDER BY idx) FROM ms WHERE {col} IS NOT NULL "
                                      f"GROUP BY {col} HAVING count(*) > 1 ORDER BY {col}").fetchall():
            first = members[0]
            for m in members[1:]:
                uf.union(first, m, sig, conf)
                n_merged += 1
        counts[sig] = n_merged
    return counts


def _fuzzy_pass(con, uf: IntUnionFind, threshold: int, columns: set[str]) -> int:
    extra = " AND ".join(f"{c} IS NULL" for c in ("extracted_pmc", "extracted_arxiv", "extracted_wikidata_qid")
                         if c in columns)
    rows = con.execute(f"""SELECT idx, author_key, year, title FROM ms
        WHERE extracted_doi IS NULL AND extracted_isbn IS NULL AND extracted_pmid IS NULL
          {"AND " + extra if extra else ""}
          AND author_key IS NOT NULL AND year IS NOT NULL AND title IS NOT NULL
        ORDER BY idx""").fetchall()
    groups: dict[tuple, list] = {}
    for idx, author, year, title in rows:
        groups.setdefault((author.lower().strip(), year), []).append((idx, vendored._norm_title(title)))
    del rows
    n_merged = 0
    for key in sorted(groups):                       # pandas groupby sorts the keys
        members = groups[key]
        n = len(members)
        if n < 2:
            continue
        ids = [m[0] for m in members]
        titles = [m[1] or "" for m in members]
        has = np.array([bool(m[1]) for m in members])
        for lo in range(0, n - 1, BLOCK_ROWS):
            hi = min(lo + BLOCK_ROWS, n - 1)
            scores = process.cdist(titles[lo:hi], titles, scorer=fuzz.token_sort_ratio, dtype=np.float64,
                                   workers=-1)
            for r, i in enumerate(range(lo, hi)):
                if not has[i]:
                    continue
                row = scores[r]
                for j in np.nonzero(row[i + 1:] >= threshold)[0] + i + 1:
                    if has[j]:
                        score = float(row[j])
                        conf = round(0.60 + (score - threshold) / (100 - threshold) * 0.10, 4)
                        uf.union(ids[i], ids[j], "author_year_fuzzy", conf)
                        n_merged += 1
    return n_merged


def _unions_dir(parquet_base: str, domain: str):
    return layout.partition_dir(parquet_base, "matching_intermediate/_global_unions", domain)


def _save_unions(d, n: int, threshold: int, counts: dict, comp: pa.Table, edge: pa.Table) -> None:
    """Keep the union-find result (the hour-long part of 6d on enwiki) so a
    failure after it can be re-run with ``--reuse-unions``."""
    if d.exists():
        shutil.rmtree(d)
    d.mkdir(parents=True)
    pq.write_table(comp, d / "comp.parquet")
    pq.write_table(edge, d / "edge.parquet")
    (d / "meta.json").write_text(json.dumps({"merged_sources": n, "threshold": threshold, "merges": counts}))


def _load_unions(d, n: int, threshold: int):
    meta_path = d / "meta.json"
    if not meta_path.exists():
        log(f"no saved unions in {d}: computing them")
        return None
    meta = json.loads(meta_path.read_text())
    if meta["merged_sources"] != n or meta["threshold"] != threshold:
        log(f"saved unions are for {meta['merged_sources']:,} sources / threshold {meta['threshold']}: recomputing")
        return None
    return meta["merges"], pq.read_table(d / "comp.parquet"), pq.read_table(d / "edge.parquet")


def run_global_scaled(parquet_base: str, domain: str, threshold: int = vendored.AUTHOR_YEAR_THRESHOLD,
                      reuse_unions: bool = False) -> dict:
    """Write ``global_source_map`` and ``global_sources_stage`` for the domain
    (one Parquet file each), as ``stage6.run_global`` does with the vendored
    ``match_global``. Returns counts. The unions are saved under
    ``matching_intermediate/_global_unions``; ``reuse_unions`` takes them from
    there when they are for the same number of sources and threshold (for a
    re-run after a failure past the union-find, not after 6a-6c changed)."""
    t0 = time.time()
    pattern = layout.partition_glob(parquet_base, "matching_intermediate/merged_sources", domain)
    con = duckdb_connect()
    try:
        # the frame run_global hands to match_global: vendored order, NULL tier / count → 0,
        # year_reliable → bool, canonical_part_sha1 under its vendored name
        con.execute(f"""CREATE TEMP TABLE ms AS
            SELECT CAST(row_number() OVER (ORDER BY page_id, merged_source_id) - 1 AS BIGINT) AS idx,
                   * EXCLUDE (domain, page_bucket, canonical_part_sha1, id_tier, n_total_citations),
                   canonical_part_sha1 AS canonical_sha1,
                   COALESCE(id_tier, 0) AS id_tier, COALESCE(n_total_citations, 0) AS n_total_citations
            FROM read_parquet('{pattern}', hive_partitioning = true)""")
        n = con.execute("SELECT count(*) FROM ms").fetchone()[0]
        columns = {r[0] for r in con.execute("DESCRIBE ms").fetchall()}
        log(f"{n:,} merged sources numbered ({time.time() - t0:.0f}s)")
        udir = _unions_dir(parquet_base, domain)
        saved = _load_unions(udir, n, threshold) if reuse_unions else None
        if saved:
            counts, comp_in, edge_in = saved
            log(f"unions reused from {udir}: {counts}")
        else:
            uf = IntUnionFind(n)
            counts = _hard_pass(con, uf, columns)
            log(f"hard identifiers: {counts} ({time.time() - t0:.0f}s)")
            counts["author_year_fuzzy"] = _fuzzy_pass(con, uf, threshold, columns)
            log(f"author+year: {counts['author_year_fuzzy']} unions ({time.time() - t0:.0f}s)")
            touched = np.fromiter(sorted(uf.touched), dtype=np.int64, count=len(uf.touched))
            roots = np.fromiter((uf.find(int(x)) for x in touched), dtype=np.int64, count=len(touched))
            comp_in = pa.table({"idx": touched, "root": roots})
            edge = [(r, mt, float(c)) for r, (mt, c) in uf.edge_info.items()]
            edge_in = pa.table({"root": pa.array([e[0] for e in edge], pa.int64()),
                                "edge_match_type": pa.array([e[1] for e in edge], pa.string()),
                                "edge_confidence": pa.array([e[2] for e in edge], pa.float64())})
            del uf, edge, touched, roots
            _save_unions(udir, n, threshold, counts, comp_in, edge_in)
            log(f"{comp_in.num_rows:,} sources in {edge_in.num_rows:,} multi-member components; unions saved "
                f"({time.time() - t0:.0f}s)")
        con.register("comp_in", comp_in)
        con.register("edge_in", edge_in)
        _build(con, parquet_base, domain)
        n_gs = con.execute("SELECT count(*) FROM gs").fetchone()[0]
    finally:
        con.close()
    log(f"{n_gs:,} global sources ({time.time() - t0:.0f}s)")
    return {"merged_sources": int(n), "global_sources": int(n_gs), "merges": counts,
            "seconds": round(time.time() - t0, 1)}


def _multi_ids(con) -> pa.Table:
    """global_source_id (sha1 of the sorted member ids) and the sorted page ids
    of every multi-member component, streamed in (root, merged_source_id) order.
    DuckDB's ordered and list aggregates hold every group in memory, which the
    whole of enwiki's 131M sources did not fit."""
    roots, gids, n_pages, page_ids = [], [], [], []
    cur_root, members, pages = None, [], set()

    def flush():
        roots.append(cur_root)
        gids.append(hashlib.sha1(("global:" + "|".join(members)).encode()).hexdigest()[:16])
        ps = sorted(pages)
        n_pages.append(len(ps))
        page_ids.append("[" + ", ".join(map(str, ps)) + "]")

    reader = con.execute("""SELECT c.root, ms.merged_source_id, ms.page_id FROM comp_in c JOIN ms USING (idx)
                            ORDER BY c.root, ms.merged_source_id""").to_arrow_reader(1_000_000)
    for batch in reader:
        for root, msid, page in zip(*(batch.column(i).to_pylist() for i in range(3))):
            if root != cur_root:
                if cur_root is not None:
                    flush()
                cur_root, members, pages = root, [], set()
            members.append(msid)
            pages.add(page)
    if cur_root is not None:
        flush()
    return pa.table({"root": pa.array(roots, pa.int64()), "global_source_id": pa.array(gids, pa.string()),
                     "n_pages": pa.array(n_pages, pa.int32()), "page_ids": pa.array(page_ids, pa.string())})


def _build(con, parquet_base: str, domain: str) -> None:
    # a source in no multi-member component is its own global source (root = its idx)
    con.execute("""CREATE TEMP TABLE m AS
        SELECT ms.idx, COALESCE(c.root, ms.idx) AS root, ms.merged_source_id, ms.page_id
        FROM ms LEFT JOIN comp_in c USING (idx)""")
    con.register("multi_ids", _multi_ids(con))
    con.execute("""CREATE TEMP TABLE comp AS
        SELECT c.root, count(*) AS n_members, sum(ms.n_total_citations) AS n_total, min(c.idx) AS first_idx
        FROM comp_in c JOIN ms USING (idx) GROUP BY c.root""")
    # canonical member: vendored _id_priority (first non-NULL of doi > isbn > pmid > url,
    # then citations), max() keeps the first in member (= source) order
    con.execute("""CREATE TEMP TABLE canon AS
        SELECT root, canonical_sha1, author_key, year, title, extracted_doi, extracted_isbn, extracted_pmid,
               extracted_pmc, extracted_arxiv, extracted_wikidata_qid, extracted_url
        FROM (SELECT ms.*, c.root, row_number() OVER (PARTITION BY c.root ORDER BY
                      CASE WHEN extracted_doi IS NOT NULL THEN 4 WHEN extracted_isbn IS NOT NULL THEN 3
                           WHEN extracted_pmid IS NOT NULL THEN 2 WHEN extracted_url IS NOT NULL THEN 1 ELSE 0 END DESC,
                      n_total_citations DESC, idx) AS rk FROM comp_in c JOIN ms USING (idx))
        WHERE rk = 1""")
    # run_global: the global source's source_type is that of the first source (in source
    # order, over the whole domain) with the same canonical_sha1
    con.execute("""CREATE TEMP TABLE canon_type AS
        SELECT canonical_sha1, arg_min_null(source_type, idx) AS source_type FROM ms GROUP BY canonical_sha1""")
    con.execute("""CREATE TEMP TABLE gs AS
        SELECT i.global_source_id,
               CASE WHEN c.n_members = 1 THEN 'singleton' ELSE COALESCE(e.edge_match_type, 'singleton') END AS match_type,
               CASE WHEN c.n_members = 1 THEN NULL ELSE e.edge_confidence END AS confidence,
               i.n_pages, i.page_ids,
               k.canonical_sha1 AS canonical_part_sha1, t.source_type,
               k.author_key, k.year, k.title, k.extracted_doi, k.extracted_isbn, k.extracted_pmid, k.extracted_pmc,
               k.extracted_arxiv, k.extracted_wikidata_qid, k.extracted_url,
               CAST(c.n_total AS INTEGER) AS n_total_citations,
               c.root, c.first_idx
        FROM comp c JOIN canon k USING (root) JOIN multi_ids i USING (root)
        LEFT JOIN edge_in e USING (root)
        LEFT JOIN canon_type t ON t.canonical_sha1 IS NOT DISTINCT FROM k.canonical_sha1
        UNION ALL
        SELECT left(sha1('global:' || ms.merged_source_id), 16), 'singleton', CAST(NULL AS DOUBLE),
               CAST(1 AS INTEGER), '[' || CAST(ms.page_id AS VARCHAR) || ']',
               ms.canonical_sha1, t.source_type,
               ms.author_key, ms.year, ms.title, ms.extracted_doi, ms.extracted_isbn, ms.extracted_pmid,
               ms.extracted_pmc, ms.extracted_arxiv, ms.extracted_wikidata_qid, ms.extracted_url,
               CAST(ms.n_total_citations AS INTEGER), ms.idx, ms.idx
        FROM ms ANTI JOIN comp_in c USING (idx)
        LEFT JOIN canon_type t ON t.canonical_sha1 IS NOT DISTINCT FROM ms.canonical_sha1""")
    for ds, sql in (
        ("global_sources_stage",
         "SELECT * EXCLUDE (root, first_idx) FROM gs ORDER BY first_idx"),
        ("global_source_map",
         "SELECT m.merged_source_id, g.global_source_id, CAST(m.page_id AS INTEGER) AS page_id, g.match_type, g.confidence "
         "FROM m JOIN gs g USING (root) ORDER BY g.first_idx, m.idx"),
    ):
        d = layout.partition_dir(parquet_base, "matching_intermediate/" + ds, domain)
        if d.exists():
            shutil.rmtree(d)
        d.mkdir(parents=True)
        con.execute(f"COPY ({sql}) TO '{d / 'data.parquet'}' (FORMAT PARQUET, COMPRESSION ZSTD)")
