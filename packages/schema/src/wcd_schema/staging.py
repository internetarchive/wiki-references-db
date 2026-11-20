"""Parquet schemas for the Stage-1 staging tables written by ``wcd_pipeline.extract``.

One pyarrow schema per staging table. ``wcd_pipeline.dedup`` consolidates
files written with these schemas and ``wcd_pipeline.load`` puts the result in
Postgres. The per-revision datasets (``citation_history``, ``named_ref_uses``,
``revision_signals``) are not staging tables: the extractor writes them in the
final Parquet-tier layout (``wcd_schema.parquet``) and nothing loads them.

``WIKIBASE_SCHEMAS`` are the staging tables of Stage 1 for a Wikibase wiki
(Wikidata), written by the Rust ``wikidata-refs extract`` (``rust/wikidata-refs``),
one file of each per bundle under ``<staging>/<table>/<bundle>.parquet``. No
later stage reads them yet.
"""
import pyarrow as pa

ROW_GROUP_SIZE = 10_000
MAX_ROWS_PER_FILE = 1_000_000

SCHEMAS = {
    'containers': pa.schema([
        ('label', pa.string()),
    ]),
    'domains': pa.schema([
        ('value', pa.string()),
        ('for_container_label', pa.string()),
    ]),
    # cited URLs only; article web_resources are written at Stage 0
    'web_resources': pa.schema([
        ('url', pa.string()),
        ('domain_label', pa.string()),
    ]),
    'citation_instances': pa.schema([
        ('citation_instance_id', pa.int64()),   # wcd_schema.ids.instance_id(domain, page_id, raw_sha1)
        ('domain', pa.string()),
        ('page_id', pa.int32()),
        ('raw_sha1', pa.string()),
        ('normalized_sha1', pa.string()),
        ('reference_type', pa.int16()),
        ('reference_name', pa.string()),
    ]),
    'normalized_citations': pa.schema([
        ('normalized_sha1', pa.string()),
        ('reference_normalized', pa.string()),
        ('appears_on_page_id', pa.int32()),
        ('appears_on_domain', pa.string()),
        ('citation_type', pa.string()),
    ]),
    'ncwr': pa.schema([
        ('normalized_sha1', pa.string()),
        ('url', pa.string()),
    ]),
    'wiki_templates': pa.schema([
        ('domain_label', pa.string()),
        ('name', pa.string()),
    ]),
    'template_data': pa.schema([
        ('domain_label', pa.string()),
        ('template_name', pa.string()),
        ('normalized_sha1', pa.string()),
        ('offset_start', pa.int32()),
        ('parameter_key', pa.string()),
        ('parameter_value', pa.string()),
    ]),
}

WIKIBASE_SCHEMAS = {
    # One row per reference a revision added to a statement or removed from one.
    # A reference is (statement, content): statement_id is the statement's GUID with
    # its subject in canonical case, reference_hash is Wikibase's reference hash
    # recomputed from the snaks (the same content hashes the same in every revision
    # and on every entity). A hidden or unreadable revision changes nothing; a
    # redirect removes every reference the entity had.
    'wikidata_reference_events': pa.schema([
        ('page_id', pa.int32()),
        ('entity_id', pa.string()),              # as the revision names it: Q42
        ('revision_id', pa.int64()),
        ('revision_timestamp', pa.timestamp('s')),
        ('statement_id', pa.string()),
        ('statement_property', pa.string()),     # the statement's main property: P31
        ('reference_hash', pa.string()),         # 40 hex digits
        ('added', pa.bool_()),                   # False: removed
        # True on the additions of a page's first revision in a bundle when that revision
        # has a parent: the page's history began in another bundle (the exports split very
        # large pages by revision range), so these rows are the state this part starts from,
        # to be set against the state the earlier part ended in, not additions by the revision.
        ('baseline', pa.bool_()),
    ]),
    # The snaks of a reference, written the first time a page shows it (so a
    # reference used on many pages repeats once per page, within a file).
    'wikidata_reference_snaks': pa.schema([
        ('page_id', pa.int32()),
        ('reference_hash', pa.string()),
        ('snak_order', pa.int32()),              # position in the reference, from 0
        ('property', pa.string()),               # P854, P248, …
        ('snaktype', pa.string()),               # value | somevalue | novalue
        ('value_type', pa.string()),             # string, wikibase-entityid, time, …; null without a value
        ('value', pa.string()),                  # a string as it is, an entity as its ID, else the JSON of the value
    ]),
}
