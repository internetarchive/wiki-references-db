"""Environment-driven configuration shared by every stage.

Values are read from the process environment after loading a ``.env`` file
from the current directory (python-dotenv); see ``example.env`` at the
repository root.

Postgres: ``DATABASE_URL`` is preferred. The historical ``DB_HOST`` /
``DB_PORT`` / ``DB_NAME`` / ``DB_USER`` / ``DB_PASS`` variables are still
accepted and assembled into a URL when ``DATABASE_URL`` is unset.
"""
from __future__ import annotations

import os

from dotenv import load_dotenv

load_dotenv()

_DB_PARTS = ("DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASS")


def _with_driver(url: str) -> str:
    """Pin the psycopg2 driver: the loaders use its ``copy_expert`` and
    SQLAlchemy >= 2.1 resolves a bare ``postgresql://`` to psycopg 3."""
    for prefix in ("postgresql://", "postgres://"):
        if url.startswith(prefix):
            return "postgresql+psycopg2://" + url[len(prefix):]
    return url


def database_url() -> str:
    """The SQLAlchemy URL for Postgres (always ``postgresql+psycopg2://``)."""
    url = os.getenv("DATABASE_URL")
    if url:
        return _with_driver(url)
    missing = [v for v in _DB_PARTS if not os.getenv(v)]
    if missing:
        raise RuntimeError(
            "Set DATABASE_URL, or all of "
            f"{', '.join(_DB_PARTS)} (missing: {', '.join(missing)}). "
            "Check your .env file (see example.env)."
        )
    return (
        f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@"
        f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )


def run_id() -> str | None:
    """``WCD_RUN_ID``: this build run's identifier, which qualifies the global
    source ids Stage 6d writes (``<run id>.<hash>``). Unset: bare ids, as the
    builds before run ids. See ``wcd_schema.ids.qualify_global_source_id``."""
    from wcd_schema.ids import check_run_id

    return check_run_id(os.getenv("WCD_RUN_ID") or None)


def staging_dir(default: str = "./staging") -> str:
    return os.getenv("STAGING_DIR", default)


def duckdb_connect(*args, **kwargs):
    """``duckdb.connect`` with the limits from the environment: ``DUCKDB_MEMORY_LIMIT``
    (e.g. ``6GB``) and ``DUCKDB_THREADS``, both per connection, so per worker process
    in a ``--jobs`` pool, and ``DUCKDB_TEMP_DIRECTORY`` (where it spills). Unset,
    DuckDB's defaults apply: 80 % of RAM and every core per connection, which on a
    full-size build overruns a memory-capped run as soon as two connections are busy,
    or one is beside Postgres."""
    import duckdb

    con = duckdb.connect(*args, **kwargs)
    mem = os.getenv("DUCKDB_MEMORY_LIMIT")
    if mem:
        con.execute(f"SET memory_limit = '{mem}'")
    tmp = os.getenv("DUCKDB_TEMP_DIRECTORY")
    if tmp:
        # one directory per connection: DuckDB names its spill files the same in every
        # process (duckdb_temp_storage_DEFAULT-0.tmp), and Stage 6's workers sharing
        # one directory overwrote each other's (an IO error on enwiki, or worse, a
        # silently wrong read). DuckDB removes its files on close; the directory stays.
        import tempfile

        os.makedirs(tmp, exist_ok=True)
        con.execute(f"SET temp_directory = '{tempfile.mkdtemp(prefix=f'p{os.getpid()}-', dir=tmp)}'")
    # DUCKDB_THREADS: per connection, so per worker in a --jobs pool. On enwiki, Stage 6
    # materialising a bucket with all 28 threads under an 8 GB limit ran out of memory,
    # and DuckDB 1.5.6 segfaulted in .df() instead of raising; 1–4 threads did not.
    threads = os.getenv("DUCKDB_THREADS")
    if threads:
        con.execute(f"SET threads = {int(threads)}")
    return con


def psycopg_dsn() -> str:
    """The same database as ``database_url()``, as a libpq / psycopg2 DSN."""
    url = database_url()
    return "postgresql://" + url.split("://", 1)[1]
