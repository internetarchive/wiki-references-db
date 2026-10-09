"""Parquet schemas for the Stage-1 staging tables written by ``wcd_pipeline.extract``.

One pyarrow schema per staging table. ``wcd_pipeline.dedup`` and
``wcd_pipeline.load`` consume files written with these schemas.
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
    'documents': pa.schema([
        ('language_code', pa.string()),
        ('has_container_label', pa.string()),
        ('page_id', pa.int32()),
    ]),
    'web_resources': pa.schema([
        ('url', pa.string()),
        ('domain_label', pa.string()),
        ('numeric_page_id', pa.int32()),
        ('numeric_namespace_id', pa.int32()),
        ('page_id', pa.int32()),
    ]),
    'citation_instances': pa.schema([
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
    ]),
    'citation_histories': pa.schema([
        ('page_id', pa.int32()),
        ('raw_sha1', pa.string()),
        ('revision_id', pa.int64()),
    ]),
    'revisions': pa.schema([
        ('revision_id', pa.int64()),
        ('page_id', pa.int32()),
        ('parent_revision_id', pa.int64()),
        ('revision_timestamp', pa.string()),
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
