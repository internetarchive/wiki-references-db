"""Deterministic identifiers.

Every id that crosses an engine boundary (Parquet written by the extractor,
Postgres loaded later, DuckDB joining the two) is a pure function of its
inputs, so no stage ever needs another stage's sequence values.
"""
from __future__ import annotations

import hashlib
import struct
from typing import Iterable

PAGE_BUCKETS = 512


def page_bucket(page_id: int) -> int:
    """The Parquet partition of a page: ``page_id % 512``."""
    return int(page_id) % PAGE_BUCKETS


def instance_id(domain: str, page_id: int, raw_sha1: str) -> int:
    """``citation_instances.id``: the first 8 bytes of
    ``SHA1("{domain}|{page_id}|{raw_sha1}")`` as a signed big-endian int64.

    >>> instance_id("en.wikipedia.org", 101, "0" * 40)
    -4489895456726657543
    """
    digest = hashlib.sha1(f"{domain}|{int(page_id)}|{raw_sha1}".encode("utf-8")).digest()
    return struct.unpack(">q", digest[:8])[0]


# The same computation in DuckDB SQL, for gates and exports. Arguments are SQL
# expressions. sha1() yields lowercase hex; the first 16 hex digits are the
# first 8 bytes; reinterpreting the unsigned value as a two's-complement
# BIGINT matches Python's signed unpack.
def instance_id_sql(domain_expr: str, page_id_expr: str, raw_sha1_expr: str) -> str:
    unsigned = f"('0x' || left(sha1({domain_expr} || '|' || CAST({page_id_expr} AS VARCHAR) || '|' || {raw_sha1_expr}), 16))::UBIGINT"
    return (
        f"CASE WHEN {unsigned} >= 9223372036854775808::UHUGEINT "
        f"THEN CAST({unsigned}::HUGEINT - 18446744073709551616::HUGEINT AS BIGINT) "
        f"ELSE CAST({unsigned} AS BIGINT) END"
    )


def global_source_id(member_ids: Iterable[str]) -> str:
    """``global_sources.global_source_id`` (UVA): ``sha1("global:" + "|".join(sorted(members)))[:16]``."""
    joined = "|".join(sorted(member_ids))
    return hashlib.sha1(f"global:{joined}".encode("utf-8")).hexdigest()[:16]


def ref_source_id(page_id: int, root_part_sha1: str) -> str:
    """Per-page ref-tag source id (UVA 6a): ``sha1("{page_id}:{root}")[:16]``."""
    return hashlib.sha1(f"{int(page_id)}:{root_part_sha1}".encode("utf-8")).hexdigest()[:16]


def template_source_id(page_id: int, root_part_sha1: str) -> str:
    """Per-page template source id (UVA 6b): ``sha1("{page_id}:tmpl:{root}")[:16]``."""
    return hashlib.sha1(f"{int(page_id)}:tmpl:{root_part_sha1}".encode("utf-8")).hexdigest()[:16]


def part_sha1(content: str) -> str:
    """``normalized_citation_parts.part_sha1`` = SHA1 of the part content (UVA ``content_sha1``)."""
    return hashlib.sha1(content.encode("utf-8")).hexdigest()
