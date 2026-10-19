"""Source-profile classification for the page summary (Stage 7h).

Vendored from UVAWikipediaCitationsDatabase (scatter-llc, commit 29f3ae319d54),
``src/analytics_service/page_summary.py``: the bucket orders, _classify_source_format,
_classify_source_kind, _format_profile_buckets and _build_source_profile_summary. The
DuckDB query half of that module (``_get_page_source_profile``, ``get_page_summary``)
is not carried over; wcd_pipeline supplies the per-citation rows from Parquet.
Logic is kept verbatim; see NOTICE for the provenance rules.

Entry point: ``build_source_profile(rows, total_citation_uses) -> dict`` (added
here: it applies the classifiers to an iterable of citation rows and returns the
JSON shape ``page_summary_cache.source_profile`` stores).
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Iterable

SOURCE_PROFILE_FORMAT_ORDER = (
    "Citation templates",
    "Plain-text refs",
    "Other / mixed",
)
SOURCE_PROFILE_KIND_ORDER = (
    "Websites",
    "Journals",
    "Books",
    "News",
    "Other / unknown",
)


@dataclass(frozen=True)
class SourceProfileBucket:
    label: str
    count: int
    percentage: float


def _format_profile_buckets(
    counts: dict[str, int],
    labels: Iterable[str],
    *,
    total: int,
) -> list[SourceProfileBucket]:
    return [
        SourceProfileBucket(
            label=label,
            count=counts.get(label, 0),
            percentage=(
                counts.get(label, 0) / total * 100.0
                if total > 0
                else 0.0
            ),
        )
        for label in labels
    ]


def _classify_source_format(
    *,
    citation_type: str | None,
    has_ref_tag: bool | None,
    has_cite_template: bool | None,
    has_citation_template: bool | None,
) -> str:
    if (
        citation_type == "template"
        or bool(has_cite_template)
        or bool(has_citation_template)
    ):
        return "Citation templates"
    if bool(has_ref_tag) or citation_type:
        return "Plain-text refs"
    return "Other / mixed"


def _classify_source_kind(
    *,
    template_name: str | None,
    template_subtype: str | None,
    norm_subtype: str | None,
    extracted_isbn: str | None,
    extracted_doi: str | None,
    extracted_pmid: str | None,
    extracted_pmc: str | None,
    extracted_url: str | None,
) -> str:
    subtype = (template_subtype or "").strip().lower()
    template = (template_name or "").strip().lower()
    ref_subtype = (norm_subtype or "").strip().lower()

    if subtype == "cite_book" or "cite book" in template:
        return "Books"
    if subtype == "cite_journal" or "cite journal" in template:
        return "Journals"
    if subtype == "cite_news" or "cite news" in template:
        return "News"
    if subtype == "cite_web" or "cite web" in template:
        return "Websites"
    if subtype:
        return "Other / unknown"

    if extracted_isbn and extracted_isbn.strip():
        return "Books"
    if (
        (extracted_doi and extracted_doi.strip())
        or (extracted_pmid and extracted_pmid.strip())
        or (extracted_pmc and extracted_pmc.strip())
        or ref_subtype in {"author_date", "prose_citation"}
    ):
        return "Journals"
    if (
        (extracted_url and extracted_url.strip())
        or ref_subtype in {"bare_url", "wikilink_ref", "cite_html"}
    ):
        return "Websites"
    return "Other / unknown"


def _build_source_profile_summary(
    format_breakdown: list[SourceProfileBucket],
    source_kind_breakdown: list[SourceProfileBucket],
) -> str:
    format_rank = {
        label: index
        for index, label in enumerate(SOURCE_PROFILE_FORMAT_ORDER)
    }
    kind_rank = {
        label: index
        for index, label in enumerate(SOURCE_PROFILE_KIND_ORDER)
    }
    top_formats = [bucket for bucket in format_breakdown if bucket.count > 0]
    top_kinds = [bucket for bucket in source_kind_breakdown if bucket.count > 0]

    if not top_formats or not top_kinds:
        return "This page does not yet have enough citation detail for a source profile."

    top_format = max(
        top_formats,
        key=lambda bucket: (bucket.count, -format_rank.get(bucket.label, 999)),
    )
    top_kind = max(
        top_kinds,
        key=lambda bucket: (bucket.count, -kind_rank.get(bucket.label, 999)),
    )
    secondary_kind = None
    if len(top_kinds) > 1:
        remaining_kinds = [bucket for bucket in top_kinds if bucket != top_kind]
        if remaining_kinds:
            secondary_kind = max(
                remaining_kinds,
                key=lambda bucket: (
                    bucket.count,
                    -kind_rank.get(bucket.label, 999),
                ),
            )

    format_phrase = top_format.label.lower()
    if secondary_kind and secondary_kind.count > 0:
        return (
            f"This page mostly uses {top_kind.label.lower()} and "
            f"{secondary_kind.label.lower()}, and most references appear as "
            f"{format_phrase}."
        )
    return (
        f"This page mostly uses {top_kind.label.lower()}, and most references "
        f"appear as {format_phrase}."
    )


# ADDED (wcd): the assembly upstream did inline in _get_page_source_profile.

def build_source_profile(rows: Iterable[dict], total_citation_uses: int) -> dict:
    """``rows``: one dict per citation at the latest revision with the keys the
    two classifiers take (``citation_type``, ``template_name``,
    ``template_subtype``, ``norm_subtype``, ``extracted_isbn`` … ``extracted_url``;
    the three ``has_*`` flags are optional). Returns the ``source_profile`` JSON."""
    format_counts = {label: 0 for label in SOURCE_PROFILE_FORMAT_ORDER}
    source_kind_counts = {label: 0 for label in SOURCE_PROFILE_KIND_ORDER}
    for row in rows:
        format_label = _classify_source_format(
            citation_type=row.get("citation_type"),
            has_ref_tag=row.get("has_ref_tag"),
            has_cite_template=row.get("has_cite_template"),
            has_citation_template=row.get("has_citation_template"),
        )
        source_kind_label = _classify_source_kind(
            template_name=row.get("template_name"),
            template_subtype=row.get("template_subtype"),
            norm_subtype=row.get("norm_subtype"),
            extracted_isbn=row.get("extracted_isbn"),
            extracted_doi=row.get("extracted_doi"),
            extracted_pmid=row.get("extracted_pmid"),
            extracted_pmc=row.get("extracted_pmc"),
            extracted_url=row.get("extracted_url"),
        )
        format_counts[format_label] = format_counts.get(format_label, 0) + 1
        source_kind_counts[source_kind_label] = source_kind_counts.get(source_kind_label, 0) + 1
    format_breakdown = _format_profile_buckets(format_counts, SOURCE_PROFILE_FORMAT_ORDER, total=total_citation_uses)
    source_kind_breakdown = _format_profile_buckets(source_kind_counts, SOURCE_PROFILE_KIND_ORDER, total=total_citation_uses)
    return {
        "total_citation_uses": total_citation_uses,
        "format_breakdown": [asdict(b) for b in format_breakdown],
        "source_kind_breakdown": [asdict(b) for b in source_kind_breakdown],
        "summary": _build_source_profile_summary(format_breakdown, source_kind_breakdown),
    }


classify_source_format = _classify_source_format
classify_source_kind = _classify_source_kind
