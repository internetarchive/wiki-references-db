"""wcd_schema — the single source of truth for WCD data layouts.

- ``wcd_schema.postgres``: SQLAlchemy models for the Postgres entity/serving tier
- ``wcd_schema.staging``: pyarrow schemas for the Stage-1 staging Parquet files

Planned (see docs/design/proposed_data_model.md): ``parquet`` (Tier-2 dataset
schemas and the ``domain=/page_bucket=`` layout), ``duckdb`` (view
registration), ``ids`` (deterministic identifier functions).
"""
