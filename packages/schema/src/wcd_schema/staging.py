"""Parquet schemas for the Stage-1 staging tables written by ``wcd_pipeline.extract``.

One pyarrow schema per staging table. ``wcd_pipeline.dedup`` consolidates
files written with these schemas and ``wcd_pipeline.load`` puts the result in
Postgres. The per-revision datasets (``citation_history``, ``named_ref_uses``,
``revision_signals``) are not staging tables: the extractor writes them in the
final Parquet-tier layout (``wcd_schema.parquet``) and nothing loads them.
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
