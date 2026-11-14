"""DuckDB over the Parquet tier (and, ATTACHed, Postgres).

``register_views`` creates one view per Parquet dataset (``hive_partitioning``
on, so ``domain`` and ``page_bucket`` are columns) and, when a Postgres DSN is
given, the small serving tables as ``pg.*`` views. ``bind_bucket_views``
re-points a set of views at a single partition so a bucketed stage never
globs the whole dataset inside its loop.
"""
from __future__ import annotations

import os

from .parquet import DOMAIN_PARTITIONED, GLOBAL, PAGE_PARTITIONED, dataset_glob, partition_glob

#: the datasets the API and the cross-engine stages read
PARQUET_VIEWS = (
    "citation_history", "named_ref_uses", "revisions", "ref_name_links",
    "citation_instances_denorm", "global_source_scores", "page_revision_stats",
)
#: Postgres tables exposed through the ATTACH
POSTGRES_VIEWS = ("domains", "global_sources", "page_source_summary", "page_metadata")


def _exists(base, name) -> bool:
    return os.path.isdir(os.path.join(base, name))


def attach_postgres(con, dsn: str, alias: str = "pg", read_only: bool = True) -> None:
    """``ATTACH`` a Postgres database (needs DuckDB's postgres extension)."""
    ro = ", READ_ONLY" if read_only else ""
    con.execute(f"ATTACH IF NOT EXISTS '{dsn}' AS {alias} (TYPE POSTGRES{ro})")


def register_views(con, parquet_base: str, pg_dsn: str | None = None, *, missing_ok: bool = True) -> list[str]:
    """Create the standard views on ``con``. Returns the names created.

    Datasets whose directory does not exist are skipped when ``missing_ok``
    (a build that has not reached that stage yet)."""
    created = []
    for name in PARQUET_VIEWS:
        if not _exists(parquet_base, name):
            if missing_ok:
                continue
            raise FileNotFoundError(os.path.join(parquet_base, name))
        con.execute(
            f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_parquet('{dataset_glob(parquet_base, name)}', hive_partitioning=true)"
        )
        created.append(name)
    if pg_dsn:
        attach_postgres(con, pg_dsn)
        con.execute("CREATE OR REPLACE VIEW domains AS SELECT id AS domain_id, value AS domain FROM pg.domains")
        created.append("domains")
        for name in POSTGRES_VIEWS[1:]:
            con.execute(f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM pg.{name}")
            created.append(name)
    return created


def bind_bucket_views(con, parquet_base: str, domain: str, bucket: int, names) -> None:
    """Point each named view at one ``domain=/page_bucket=`` partition (or one
    ``domain=`` partition for domain-partitioned datasets). A missing partition
    becomes an empty view with the dataset's columns, so a bucket with no
    ``named_ref_uses`` still joins."""
    from .parquet import SCHEMAS  # local import: pyarrow is cheap but not needed otherwise

    for name in names:
        if name in PAGE_PARTITIONED:
            pattern = partition_glob(parquet_base, name, domain, bucket)
            part_cols = f"'{domain}' AS domain, {int(bucket)} AS page_bucket, "
        elif name in DOMAIN_PARTITIONED:
            pattern = partition_glob(parquet_base, name, domain)
            part_cols = f"'{domain}' AS domain, "
        elif name in GLOBAL:
            pattern = dataset_glob(parquet_base, name)
            part_cols = ""
        else:
            raise KeyError(name)
        has_files = bool(con.execute(f"SELECT count(*) FROM glob('{pattern}')").fetchone()[0])
        if has_files:
            con.execute(f"CREATE OR REPLACE VIEW {name.split('/')[-1]} AS SELECT * FROM read_parquet('{pattern}', hive_partitioning=true)")
        else:
            schema = SCHEMAS.get(name)
            if schema is None:
                raise FileNotFoundError(pattern)
            cols = ", ".join(f"CAST(NULL AS {_duck_type(f.type)}) AS {f.name}" for f in schema)
            con.execute(f"CREATE OR REPLACE VIEW {name.split('/')[-1]} AS SELECT {part_cols}{cols} WHERE false")


def _duck_type(t) -> str:
    import pyarrow as pa

    if pa.types.is_int8(t):
        return "TINYINT"
    if pa.types.is_int16(t):
        return "SMALLINT"
    if pa.types.is_int32(t):
        return "INTEGER"
    if pa.types.is_int64(t):
        return "BIGINT"
    if pa.types.is_boolean(t):
        return "BOOLEAN"
    if pa.types.is_floating(t):
        return "DOUBLE"
    if pa.types.is_timestamp(t):
        return "TIMESTAMP"
    return "VARCHAR"
