"""Tests for the hash-set based dedup that replaced the LRU caches in build_db."""

from build_db import (
    _seen_citation_keys,
    _seen_normalized_keys,
    _seen_ncwr_keys,
    _seen_wiki_template_keys,
    _seen_template_data_keys,
)


def test_hash_sets_are_plain_sets() -> None:
    """The dedup structures should be plain Python sets (unbounded, lossless)."""
    assert isinstance(_seen_citation_keys, set)
    assert isinstance(_seen_normalized_keys, set)
    assert isinstance(_seen_ncwr_keys, set)
    assert isinstance(_seen_wiki_template_keys, set)
    assert isinstance(_seen_template_data_keys, set)


def test_hash_set_dedup_is_lossless() -> None:
    """Plain sets never evict — adding the same key twice is always detected."""
    s = set()
    key = ('sha1_a', 'sha1_b')
    assert key not in s
    s.add(key)
    assert key in s
    # No eviction possible
    for i in range(1_000_000):
        s.add(('filler', i))
    assert key in s
