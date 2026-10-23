"""Stage 1's per-page reference cache: results equal the uncached work, and the
cache never carries entries from one page into the next."""
from citation_normalizer import get_sha1, normalize_wikitext

from wcd_pipeline.extract import PageRefCache

REF = "<ref name=a>{{cite_web | url=https://example.com/a |title= A }}</ref>"


def test_normalize_matches_uncached_and_reuses_entry():
    cache = PageRefCache()
    cache.start_page(1)
    first = cache.normalize(REF)
    normalized = normalize_wikitext(REF)
    assert first == (normalized, get_sha1(normalized), get_sha1(REF))
    assert cache.normalize(REF) is first


def test_cache_clears_on_page_change_and_at_the_bound():
    cache = PageRefCache(max_entries=2)
    cache.start_page(1)
    cache.normalize(REF)
    cache.templates["x"] = []
    cache.start_page(1)          # same page, within the bound: kept
    assert cache.normalized and cache.templates
    cache.start_page(2)          # new page: cleared
    assert not cache.normalized and not cache.templates
    for i in range(3):
        cache.normalize(f"<ref>{i}</ref>")
    cache.start_page(2)          # same page, over the bound: cleared
    assert not cache.normalized
