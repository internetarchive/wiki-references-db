import duckdb
import pytest

from wcd_schema import ids


def test_instance_id_is_signed_int64_of_sha1_prefix():
    v = ids.instance_id("en.wikipedia.org", 101, "0" * 40)
    assert isinstance(v, int)
    assert -(2**63) <= v < 2**63
    assert v == ids.instance_id("en.wikipedia.org", 101, "0" * 40)
    assert v != ids.instance_id("de.wikipedia.org", 101, "0" * 40)
    assert v != ids.instance_id("en.wikipedia.org", 102, "0" * 40)


@pytest.mark.parametrize("domain,page_id,sha", [
    ("en.wikipedia.org", 101, "0" * 40),
    ("en.wikipedia.org", 2147483647, "f" * 40),
    ("rm.wikipedia.org", 7, "25752af3b0b9b1a3c1c6d4a0d2e5f6a7b8c9d0e1"),
    ("ar.wikipedia.org", 1, "deadbeef" * 5),
] + [("en.wikipedia.org", i, f"{i:040x}") for i in range(0, 4000, 97)])
def test_duckdb_expression_matches_python(domain, page_id, sha):
    con = duckdb.connect()
    sql = ids.instance_id_sql("'" + domain + "'", str(page_id), "'" + sha + "'")
    got = con.execute(f"SELECT {sql}").fetchone()[0]
    assert got == ids.instance_id(domain, page_id, sha)


def test_source_ids_match_uva_formulas():
    import hashlib
    assert ids.global_source_id(["b", "a"]) == hashlib.sha1(b"global:a|b").hexdigest()[:16]
    assert ids.ref_source_id(12, "x" * 40) == hashlib.sha1(f"12:{'x' * 40}".encode()).hexdigest()[:16]
    assert ids.template_source_id(12, "x" * 40) == hashlib.sha1(f"12:tmpl:{'x' * 40}".encode()).hexdigest()[:16]
    assert ids.part_sha1("abc") == hashlib.sha1(b"abc").hexdigest()


def test_page_bucket():
    assert ids.page_bucket(0) == 0
    assert ids.page_bucket(512) == 0
    assert ids.page_bucket(513) == 1
    assert ids.PAGE_BUCKETS == 512


@pytest.mark.parametrize("name,key,value", [
    ("Cite journal", "doi-access", "free"),
    ("Cite web", "title", "Ünïcødé — 日本語 \\ 'quoted'"),
    ("Cite book", "last", None),
    ("Cite book", "last", ""),
    ("Sfn", "1", "Smith"),
] + [("Cite web", "url", f"https://example.org/{i}") for i in range(0, 3000, 61)])
def test_template_search_bucket_duckdb_matches_python(name, key, value):
    con = duckdb.connect()
    got = con.execute(f"SELECT {ids.template_search_bucket_sql('?', '?', '?')}", [name, key, value]).fetchone()[0]
    want = ids.template_search_bucket(name, key, value)
    assert got == want and 0 <= want < ids.TEMPLATE_SEARCH_BUCKETS


def test_template_search_buckets_spread():
    buckets = {ids.template_search_bucket("Cite web", "url", f"https://example.org/{i}") for i in range(5000)}
    assert len(buckets) > 500   # nearly all 512 used by 5,000 values of one template/key
