"""wcd_schema — the single source of truth for WCD data layouts.

- ``wcd_schema.postgres``: SQLAlchemy models for the Postgres entity/serving tier
- ``wcd_schema.staging``: pyarrow schemas for the Stage-1 staging Parquet files
- ``wcd_schema.parquet``: the Tier-2 datasets — schemas and the ``domain=/page_bucket=`` layout
- ``wcd_schema.duckdb``: DuckDB view registration over Parquet (+ Postgres via ATTACH)
- ``wcd_schema.ids``: the deterministic identifiers (instance id, source ids, page bucket)

See ``docs/design/proposed_data_model.md``.
"""
