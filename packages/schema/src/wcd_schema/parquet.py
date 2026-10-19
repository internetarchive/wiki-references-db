"""The Parquet tier: dataset names, schemas and the on-disk layout.

Every page-partitioned dataset lives under ``<parquet_base>/<name>/`` with hive
partitions ``domain=<domain>/page_bucket=<n>/`` where ``page_bucket =
page_id % 512``; the bucket is the unit of batching, checkpointing and
re-run in Stages 6–8. The partition columns are not stored inside the files.
"""
from __future__ import annotations

import glob
import os
from pathlib import Path

import pyarrow as pa

from .ids import PAGE_BUCKETS, page_bucket

__all__ = ["PAGE_BUCKETS", "page_bucket", "SCHEMAS", "PAGE_PARTITIONED", "DOMAIN_PARTITIONED",
           "GLOBAL", "dataset_dir", "partition_dir", "dataset_glob", "partition_glob", "list_buckets"]

# ---------------------------------------------------------------------------
# Schemas (the stored columns; partition columns are added by the reader)
# ---------------------------------------------------------------------------

SCHEMAS: dict[str, pa.Schema] = {
    # Stage 1 (written directly by the extractor in final layout)
    "citation_history": pa.schema([
        ("citation_instance_id", pa.int64()),
        ("revision_id", pa.int64()),
        ("page_id", pa.int32()),
    ]),
    "named_ref_uses": pa.schema([
        ("page_id", pa.int32()),
        ("revision_id", pa.int64()),
        ("ref_name", pa.string()),
        ("offset_start", pa.int32()),
    ]),
    "revision_signals": pa.schema([
        ("revision_id", pa.int64()),
        ("page_id", pa.int32()),
        ("has_ref_tag", pa.bool_()),
        ("has_named_ref", pa.bool_()),
        ("has_self_closing_ref", pa.bool_()),
        ("has_references_tag", pa.bool_()),
        ("has_cite_template", pa.bool_()),
        ("has_citation_template", pa.bool_()),
        ("has_shorthand_template", pa.bool_()),
    ]),
    # Stage 3 (re-partitioned from RevisionChest metadata)
    "revisions": pa.schema([
        ("revision_id", pa.int64()),
        ("page_id", pa.int32()),
        ("parent_revision_id", pa.int64()),
        ("revision_timestamp", pa.timestamp("s")),
    ]),
    # Stage 5
    "ref_name_links": pa.schema([
        ("page_id", pa.int32()),
        ("ref_name", pa.string()),
        ("defining_instance_id", pa.int64()),
        ("first_revision_id", pa.int64()),
        ("last_revision_id", pa.int64()),
    ]),
    # Stage 4.6 / 7c
    "citation_instances_denorm": pa.schema([
        ("citation_instance_id", pa.int64()),
        ("page_id", pa.int32()),
        ("raw_sha1", pa.string()),
        ("normalized_id", pa.int32()),
        ("normalized_sha1", pa.string()),
        ("reference_type", pa.int8()),
        ("reference_name", pa.string()),
        ("citation_type", pa.string()),
        ("part_index", pa.int16()),
        ("part_sha1", pa.string()),
        ("content", pa.string()),
        ("part_type", pa.string()),
        ("norm_subtype", pa.string()),
        ("norm_content", pa.string()),
        ("template_name", pa.string()),
        ("template_subtype", pa.string()),
        ("anchor_key", pa.string()),
        ("page_ref", pa.string()),
        ("extracted_doi", pa.string()),
        ("extracted_isbn", pa.string()),
        ("extracted_pmid", pa.string()),
        ("extracted_pmc", pa.string()),
        ("extracted_arxiv", pa.string()),
        ("extracted_wikidata_qid", pa.string()),
        ("extracted_librarybase_id", pa.string()),
        ("extracted_url", pa.string()),
        ("is_archive_url", pa.bool_()),
        ("author_key", pa.string()),
        ("title", pa.string()),
        ("year", pa.string()),
        ("year_reliable", pa.bool_()),
        ("id_tier", pa.int8()),
        ("id_tier_label", pa.string()),
        ("global_source_id", pa.string()),
        ("source_type", pa.string()),
        ("match_type", pa.string()),
        ("confidence", pa.float64()),
    ]),
    # Stage 7g
    "global_source_scores": pa.schema([
        ("global_source_id", pa.string()),
        ("n_pages", pa.int32()),
        ("n_total_citations", pa.int32()),
        ("reliability_score", pa.float64()),
        ("verdict", pa.string()),
    ]),
    # Stage 8
    "page_revision_stats": pa.schema([
        ("page_id", pa.int32()),
        ("rev_id", pa.int64()),
        ("timestamp", pa.timestamp("s")),
        ("n_total_citations", pa.int32()),
        ("n_self_closing", pa.int32()),
        ("n_unique_sources", pa.int32()),
        ("n_unresolved", pa.int32()),
        ("n_high_confidence", pa.int32()),
        ("n_new_sources", pa.int32()),
        ("pct_resolved", pa.float64()),
        ("n_ref_tag", pa.int32()),
        ("n_template", pa.int32()),
        ("n_bare_url", pa.int32()),
        ("n_endnote", pa.int32()),
        ("n_shorthand", pa.int32()),
        ("n_external_link", pa.int32()),
    ]),
}

#: datasets partitioned by domain= and page_bucket=
PAGE_PARTITIONED = (
    "citation_history", "named_ref_uses", "revisions", "ref_name_links",
    "citation_instances_denorm", "page_revision_stats",
    "matching_intermediate/sources", "matching_intermediate/citation_source_map",
    "matching_intermediate/template_sources", "matching_intermediate/template_citation_source_map",
    "matching_intermediate/merged_sources", "matching_intermediate/merged_citation_source_map",
)
#: datasets partitioned by domain= only
DOMAIN_PARTITIONED = ("revision_signals",)
#: unpartitioned datasets
GLOBAL = ("global_source_scores", "matching_intermediate/global_source_map")

FILE_GLOB = "*.parquet*"   # .parquet or .parquet.zst


def dataset_dir(base: str | os.PathLike, name: str) -> Path:
    return Path(base) / name


def partition_dir(base: str | os.PathLike, name: str, domain: str, bucket: int | None = None) -> Path:
    """``<base>/<name>/domain=<domain>[/page_bucket=<bucket>]``."""
    d = dataset_dir(base, name) / f"domain={domain}"
    return d if bucket is None else d / f"page_bucket={int(bucket)}"


def dataset_glob(base: str | os.PathLike, name: str) -> str:
    """Glob for every file of a dataset (use with ``hive_partitioning=true``)."""
    return str(dataset_dir(base, name) / "**" / FILE_GLOB)


def partition_glob(base: str | os.PathLike, name: str, domain: str, bucket: int | None = None) -> str:
    """Glob for one domain (or one bucket) of a dataset."""
    return str(partition_dir(base, name, domain, bucket) / ("**" if bucket is None else "") / FILE_GLOB).replace("//", "/")


def list_buckets(base: str | os.PathLike, name: str, domain: str) -> list[int]:
    """The page buckets present for a domain, ascending."""
    d = partition_dir(base, name, domain)
    out = []
    for p in glob.glob(str(d / "page_bucket=*")):
        try:
            out.append(int(os.path.basename(p).split("=", 1)[1]))
        except ValueError:
            continue
    return sorted(out)
