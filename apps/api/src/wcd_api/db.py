"""Database access: a SQLAlchemy engine for Postgres and DuckDB over the
Parquet tier, one bucket partition at a time.

Every Parquet read is pruned to ``domain=…/page_bucket=…`` (``page_bucket =
page_id % 512``) by binding the dataset's view to that partition
(``wcd_schema.duckdb.bind_bucket_views``), so no request globs a whole
dataset. DuckDB connections are cheap in-process objects; one is created per
request that needs Parquet.
"""
from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

import duckdb
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from wcd_schema import parquet as layout
from wcd_schema.duckdb import bind_bucket_views
from wcd_schema.ids import page_bucket

from .settings import Settings


class Database:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.engine: Engine = create_engine(settings.database_url, pool_size=settings.pool_size,
                                            pool_pre_ping=True, hide_parameters=True)
        self.parquet_dir = settings.parquet_dir

    @contextmanager
    def connect(self) -> Iterator[Connection]:
        with self.engine.connect() as con:
            yield con

    def domain_id(self, con: Connection, domain: str) -> int | None:
        return con.execute(text("SELECT id FROM domains WHERE value = :d"), {"d": domain}).scalar()

    def domain_value(self, con: Connection, domain_id: int) -> str | None:
        return con.execute(text("SELECT value FROM domains WHERE id = :i"), {"i": domain_id}).scalar()

    # -- Parquet ----------------------------------------------------------

    def duck(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect()

    def bind(self, con: duckdb.DuckDBPyConnection, domain: str, page_id: int, names) -> None:
        """Bind ``names`` (Parquet datasets) to the partition holding ``page_id``."""
        bind_bucket_views(con, self.parquet_dir, domain, page_bucket(page_id), names)

    def has_dataset(self, name: str, domain: str) -> bool:
        return layout.partition_dir(self.parquet_dir, name, domain).is_dir()

    def dispose(self) -> None:
        self.engine.dispose()
