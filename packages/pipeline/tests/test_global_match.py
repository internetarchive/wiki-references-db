"""Stage 6d's scaled engine (wcd_pipeline.global_match) writes exactly what the
vendored match_global path (stage6.run_global) writes, on random inputs built to
hit its edge cases: identifiers shared across pages, author keys differing in
case and whitespace, near-duplicate / missing / short titles, ties in the
canonical member's priority, canonical sha1s shared by sources of different
(or no) source_type."""
from __future__ import annotations

import hashlib
import random

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from wcd_pipeline import global_match, stage6
from wcd_schema import parquet as layout
from wcd_schema.parquet import SCHEMAS

DOMAIN = "en.wikipedia.org"
MS = "matching_intermediate/merged_sources"

TITLES = ["The Spirit of Japanese Law", "the spirit of japanese law!", "Spirit of the Japanese Law",
          "Britain and Japan", "Britain and Japan: Biographical Portraits", "A History of Exeter",
          "History of Exeter, A", "abc", "", None, "Völkerwanderung und Reich", "Volkerwanderung und Reich"]


def _rows(seed: int, n: int) -> list[dict]:
    rng = random.Random(seed)
    pick = lambda pool, p: rng.choice(pool) if rng.random() < p else None  # noqa: E731
    out = []
    for i in range(n):
        out.append({
            "merged_source_id": hashlib.sha1(f"{seed}-{i}".encode()).hexdigest()[:16],
            "page_id": rng.randint(1, max(2, n // 8)),
            "source_type": pick(["ref_tag", "template", "both"], 0.9),
            "ref_source_id": None, "tmpl_source_id": None,
            "canonical_part_sha1": pick([f"{k:040x}" for k in range(n // 3)], 0.95),
            "author_key": pick(["Smith", "smith ", " SMITH", "Lee", "lee", "Öztürk", "öztürk"], 0.7),
            "year": pick(["2001", "2002", "1999"], 0.8),
            "title": rng.choice(TITLES) if rng.random() < 0.85 else None,
            "extracted_doi": pick([f"10.1/{k}" for k in range(12)], 0.15),
            "extracted_isbn": pick([f"97800000000{k:02d}" for k in range(10)], 0.1),
            "extracted_pmid": pick([str(k) for k in range(6)], 0.05),
            "extracted_pmc": pick(["PMC1", "PMC2"], 0.03),
            "extracted_arxiv": pick(["2101.00001", "hep-th/9901001"], 0.03),
            "extracted_wikidata_qid": pick(["Q1", "Q2"], 0.03),
            "extracted_url": pick([f"https://e.org/{k}" for k in range(20)], 0.4),
            "n_ref_citations": rng.randint(0, 3), "n_tmpl_citations": rng.randint(0, 3),
            "n_total_citations": rng.choice([None, 0, 1, 1, 2, 5]),
            "match_type": pick(["doi", "author_year", "url", "singleton"], 0.8),
            "confidence": pick([0.6, 0.95, 1.0], 0.6),
            "id_tier": rng.choice([None, 0, 1, 2]),
            "year_reliable": rng.choice([None, True, False]),
            "norm_subtype": pick(["prose_citation", "noise", "bare_url"], 0.7),
        })
    return out


def _write_input(base, rows):
    schema = SCHEMAS[MS]
    by_bucket: dict[int, list] = {}
    for r in rows:
        by_bucket.setdefault(r["page_id"] % layout.PAGE_BUCKETS, []).append(r)
    for b, rs in by_bucket.items():
        d = layout.partition_dir(base, MS, DOMAIN, b)
        d.mkdir(parents=True)
        pq.write_table(pa.Table.from_pylist(rs, schema=schema), d / "data.parquet")


def _read(base, ds):
    pattern = layout.partition_glob(base, "matching_intermediate/" + ds, DOMAIN)
    cols = ", ".join(f.name for f in SCHEMAS["matching_intermediate/" + ds])
    return sorted(duckdb.sql(f"SELECT {cols} FROM read_parquet('{pattern}', hive_partitioning = false)").fetchall(),
                  key=repr)


@pytest.mark.parametrize("seed,n", [(1, 400), (2, 1500), (3, 3000)])
def test_scaled_engine_matches_vendored(tmp_path, monkeypatch, seed, n):
    rows = _rows(seed, n)
    a, b = tmp_path / "a", tmp_path / "b"
    _write_input(a, rows)
    _write_input(b, rows)
    stage6.run_global(str(a), DOMAIN)
    monkeypatch.setattr(global_match, "BLOCK_ROWS", 7)    # several row blocks per fuzzy group
    monkeypatch.setattr(global_match, "CHUNK_ROWS", 97)   # several output files
    global_match.run_global_scaled(str(b), DOMAIN)
    assert len(list(layout.partition_dir(b, "matching_intermediate/global_source_map", DOMAIN).glob("*.parquet"))) > 1
    for ds in ("global_sources_stage", "global_source_map"):
        want, got = _read(a, ds), _read(b, ds)
        assert len(got) == len(want), ds
        assert got == want, ds
    # the inputs do merge (a component keeps the edge info of its final root, so which
    # signals show depends on the union order, as upstream)
    types = {r[1] for r in _read(b, "global_sources_stage")}
    assert "singleton" in types and "author_year_fuzzy" in types and len(types) >= 3


def test_scaled_engine_reuses_saved_unions(tmp_path, capsys):
    rows = _rows(2, 1500)
    _write_input(tmp_path, rows)
    first = global_match.run_global_scaled(str(tmp_path), DOMAIN)
    want = {ds: _read(tmp_path, ds) for ds in ("global_sources_stage", "global_source_map")}
    capsys.readouterr()
    again = global_match.run_global_scaled(str(tmp_path), DOMAIN, reuse_unions=True)
    assert "unions reused" in capsys.readouterr().out
    assert again["merges"] == first["merges"] and again["global_sources"] == first["global_sources"]
    for ds, rows_ in want.items():
        assert _read(tmp_path, ds) == rows_, ds
    # unions saved for another threshold are not reused
    global_match.run_global_scaled(str(tmp_path), DOMAIN, threshold=90, reuse_unions=True)
    assert "recomputing" in capsys.readouterr().out


def test_int_union_find_matches_vendored():
    from wcd_semantic.unionfind import UnionFind

    rng = random.Random(5)
    n = 300
    ops = [(rng.randrange(n), rng.randrange(n), rng.choice(["a", "b"]), rng.choice([0.6, 0.9, 1.0]))
           for _ in range(600)]
    ref, fast = UnionFind(list(range(n))), global_match.IntUnionFind(n)
    for x, y, mt, c in ops:
        ref.union(x, y, mt, c)
        fast.union(x, y, mt, c)
    assert [ref.find(i) for i in range(n)] == [fast.find(i) for i in range(n)]
    assert ref.edge_info == fast.edge_info
