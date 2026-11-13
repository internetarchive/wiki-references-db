"""Deterministic identifiers.

Every id that crosses an engine boundary (Parquet written by the extractor,
Postgres loaded later, DuckDB joining the two) is a pure function of its
inputs, so no stage ever needs another stage's sequence values.
"""
from __future__ import annotations

import hashlib
import re
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


TEMPLATE_SEARCH_BUCKETS = 512
_UNIT_SEPARATOR = "\x1f"


def template_search_bucket(template_name: str, parameter_key: str, parameter_value: str | None) -> int:
    """The ``template_param_index`` partition of a (template, key, value) lookup:
    the first 4 bytes of ``MD5(name + US + key + US + value)`` (US = U+001F,
    a NULL value as the empty string) as an unsigned big-endian int, mod 512.

    The template report asks for all three, so a lookup reads one partition,
    and partitions are even however skewed the templates and keys are.
    """
    joined = _UNIT_SEPARATOR.join((template_name, parameter_key, parameter_value or ""))
    return int(hashlib.md5(joined.encode("utf-8")).hexdigest()[:8], 16) % TEMPLATE_SEARCH_BUCKETS


def template_search_bucket_sql(name_expr: str, key_expr: str, value_expr: str) -> str:
    """``template_search_bucket`` in DuckDB SQL (arguments are SQL expressions)."""
    joined = f"{name_expr} || chr(31) || {key_expr} || chr(31) || coalesce({value_expr}, '')"
    return f"CAST(('0x' || left(md5({joined}), 8))::UBIGINT % {TEMPLATE_SEARCH_BUCKETS} AS INTEGER)"


#: a build run's identifier, as it prefixes global source ids: lowercase letters, digits,
#: ``-`` and ``_`` (no ``.``, which separates it from the hash), at most 40 characters
RUN_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]{0,39}$")
#: between the run id and the hash in a run-qualified global source id
GLOBAL_SOURCE_RUN_SEP = "."
#: room for ``<run id>.<16 hex>`` (the Postgres columns are varchar of this length)
GLOBAL_SOURCE_ID_MAX_LEN = 64


def check_run_id(run_id: str | None) -> str | None:
    """``run_id`` if valid (None and "" mean an unqualified run), else ValueError."""
    if not run_id:
        return None
    if not RUN_ID_PATTERN.match(run_id):
        raise ValueError(f"run id {run_id!r} must match {RUN_ID_PATTERN.pattern}")
    return run_id


def qualify_global_source_id(hash16: str, run_id: str | None) -> str:
    """``<run_id>.<hash16>``, or the bare hash when the run has no id.

    Global source ids are interim, per-build handles (a source's membership, and
    so its hash, can change between builds). Builds made with ``WCD_RUN_ID`` set
    qualify them with the run id; bare ids are the build that predates run ids
    (the 2026-11 enwiki build). Two runs that form the same component get the
    same hash, so the suffix still compares across runs."""
    run_id = check_run_id(run_id)
    return f"{run_id}{GLOBAL_SOURCE_RUN_SEP}{hash16}" if run_id else hash16


def qualify_global_source_id_sql(hash_expr: str, run_id: str | None) -> str:
    """``qualify_global_source_id`` in SQL (DuckDB or Postgres); ``hash_expr`` is an expression."""
    run_id = check_run_id(run_id)
    return f"('{run_id}{GLOBAL_SOURCE_RUN_SEP}' || {hash_expr})" if run_id else hash_expr


def split_global_source_id(gid: str) -> tuple[str | None, str]:
    """(run id or None, hash) of a global source id."""
    run_id, sep, hash16 = gid.rpartition(GLOBAL_SOURCE_RUN_SEP)
    return (run_id, hash16) if sep else (None, gid)


def global_source_hash(member_ids: Iterable[str]) -> str:
    """UVA's global source hash: ``sha1("global:" + "|".join(sorted(members)))[:16]``."""
    joined = "|".join(sorted(member_ids))
    return hashlib.sha1(f"global:{joined}".encode("utf-8")).hexdigest()[:16]


def global_source_id(member_ids: Iterable[str], run_id: str | None = None) -> str:
    """``global_sources.global_source_id``: the UVA hash, qualified by the run id when there is one."""
    return qualify_global_source_id(global_source_hash(member_ids), run_id)


def ref_source_id(page_id: int, root_part_sha1: str) -> str:
    """Per-page ref-tag source id (UVA 6a): ``sha1("{page_id}:{root}")[:16]``."""
    return hashlib.sha1(f"{int(page_id)}:{root_part_sha1}".encode("utf-8")).hexdigest()[:16]


def template_source_id(page_id: int, root_part_sha1: str) -> str:
    """Per-page template source id (UVA 6b): ``sha1("{page_id}:tmpl:{root}")[:16]``."""
    return hashlib.sha1(f"{int(page_id)}:tmpl:{root_part_sha1}".encode("utf-8")).hexdigest()[:16]


def part_sha1(content: str) -> str:
    """``normalized_citation_parts.part_sha1`` = SHA1 of the part content (UVA ``content_sha1``)."""
    return hashlib.sha1(content.encode("utf-8")).hexdigest()
