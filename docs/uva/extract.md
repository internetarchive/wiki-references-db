# `citation_extractor.py` - Workflow

## Purpose

Streams a Wikipedia RevisionChest shard (`.mwrev.zst`) and extracts one structured citation record per ref tag per revision, writing output as a zstd-compressed newline-delimited JSON file (`.jsonl.zst`). This is the first step in the pipeline, it converts the raw compressed revision history into a flat, queryable citation stream.

---

## Inputs and Outputs

**Input:** A `.mwrev.zst` shard file - a compressed stream of Wikipedia revision history where each revision is a header line followed by wikitext.

**Output:** A `.jsonl.zst` file - one JSON object per citation per revision, ready for ingestion into DuckDB.

---

## Step-by-Step Workflow

### Step 1: Streaming decompression

```
zstd subprocess
  → stdout pipe (streaming, never fully loaded into memory)
  → line-by-line processing
```

The shard is decompressed via a `zstd -dc` subprocess piped to stdout. The file is never fully loaded into memory, lines are processed one at a time. This keeps memory usage flat regardless of shard size.

---

### Step 2: Revision boundary detection

Each revision begins with a header line starting with `#`:

Example:
```
# page_id=307 ns=0 rev_id=12345 parent_rev_id=12344 timestamp=2006-04-24T03:34:23Z user_id=42
```

When a header line is detected, `RevisionProcessor.start_revision()` is called, which first flushes the previous revision then resets state for the new one.

---

### Step 3: Cheap signal detection (pre-filter)

Before buffering any text, each line is scanned for cheap substring signals using `_update_signals()`:

| Signal | Substring checked |
|---|---|
| `saw_ref` | `<ref` or `<REF` |
| `saw_references` | `<references` |
| `saw_cite` | `{{cite `, `{{Cite ` |
| `saw_citation` | `{{citation`, `{{Citation` |
| `saw_shorthand` | `{{sfn`, `{{harv`, `{{harvnb`, `{{rp`, `{{r\|` |

If none of these signals fire, the revision is marked `interesting=False` and its text is **not buffered at all**. Only revisions with at least one citation signal are buffered for further processing.

---

### Step 4: Selective buffering

Lines from interesting revisions are appended to a buffer up to a configurable size limit (`--max-text-mb`, default 25 MB). Revisions exceeding this limit are truncated. This guards against large revisions consuming unbounded memory. Can be changed. 

---

### Step 5: Flush at revision boundary

When a new revision header is encountered (or at EOF), `flush()` processes the buffered text of the previous revision:

**5a. Flag extraction**: runs regex patterns over the full buffered text to produce boolean structural flags:
- `has_ref_tag`, `has_named_ref`, `has_self_closing_ref`
- `has_references_tag`, `has_cite_template`, `has_citation_template`
- `has_shorthand_template`

These flags summarize the citation structure of the revision and are stored with every citation row for downstream filtering.

**5b. Ref tag extraction** : `_extract_refs()` walks the text character by character using a depth counter to track nested `<ref>...</ref>` boundaries. Self-closing refs (`<ref name="X"/>`) are skipped, at normalization. Returns a list of `(ref_name, content)` pairs.

**5c. Footnote filtering** : `_is_citation()` checks each ref tag's content against patterns for known non-citation editorial notes (bare wikilinks to articles, HTML entities, raw HTML tags). Content containing a structured identifier (URL, ISBN, DOI, PMID) or a `{{cite}}` template always passes. Everything else is kept by default.

---

### Step 6: Citation parsing

Each surviving `(ref_name, content)` pair is passed to `_parse_citation()`:

**If content contains `{{`:**
- `_parse_cached()` attempts to parse the content as a `{{cite ...}}` or
  `{{citation}}` template using `mwparserfromhell`
- If successful → `citation_type="template"` with structured `params` dict
  and the raw wikitext in `raw`
- If not a cite template, or parse fails → falls through to ref_tag

**If content has no `{{`, or is not a cite template:**
- Stored as `citation_type="ref_tag"` with content as a plain string

`_parse_cached()` is decorated with `@lru_cache(maxsize=16384)` : the same wikitext appearing in thousands of revisions is parsed only once. Cache hit rates are high per shard, removing the need to run mwparserfromhell many times.

---

### Step 7: Output serialization

Each citation row is assembled as:

```python
{
    # Revision metadata
    "page_id": ..., "ns": ..., "rev_id": ...,
    "parent_rev_id": ..., "timestamp": ..., "user_id": ...,
    # Structural flags
    "has_ref_tag": ..., "has_named_ref": ..., ...
    # Citation fields
    "citation_type": "template" | "ref_tag",
    "template_name": ..., "params": {...}, "raw": ...,
    "content": ..., "ref_name": ...,
}
```

Rows are serialized with `orjson` (faster than stdlib `json`) and written to the zstd-compressed output stream. The compressor wraps the output file as a streaming writer. Output is compressed incrementally without accumulating the full output in memory.

---

## Key Design Decisions

**Streaming throughout.** Neither the input nor the output is fully loaded into memory. The zstd subprocess streams decompressed input line by line; the zstd writer streams compressed output record by record. This keeps memory usage bounded regardless of shard size.

**Cheap pre-filter before buffering.** The substring signal check in `_update_signals()` runs on every line but is O(1) per line — just Python `in` checks on fixed strings. This avoids buffering the majority of revisions (those with no citations) before any regex or parsing work is done.

**`@lru_cache` on template parsing.** Wikipedia revision history is extremely repetitive. the same citation template string appears in thousands of consecutive revisions. Caching the parsed result cuts `mwparserfromhell` calls by ~95%+ on typical shards.

**`RevisionProcessor` class for mutable state.** The streaming loop accumulates state across lines and revisions, counters, current revision metadata, line buffer, and per-revision signals. Encapsulating this in a class with explicit methods (`start_revision`, `ingest_line`, `flush`) makes the state visible and each method independently testable.

**Conservative footnote filtering.** The filter removes only patterns that are definitively not citations (bare wikilinks, HTML entities, raw HTML). Anything ambiguous is kept. False negatives (keeping a non-citation) are cheaper than false positives (discarding a real citation) since the normalizer and matching pipeline handle noise better than gaps.